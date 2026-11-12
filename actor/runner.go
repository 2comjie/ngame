package actor

import (
	"container/heap"
	"context"
	"sync"
	"time"
)

type runner struct {
	key          Key
	activationId string
	options      options
	mu           sync.Mutex
	state        int
	reason       StopReason
	ctx          context.Context
	cancel       context.CancelFunc
	mailbox      chan Task
	timers       timerHeap
	wake         chan struct{}
	done         chan struct{}
	starting     map[chan error]struct{}
}

func (r *runner) Key() Key {
	return r.key
}

func (r *runner) ActivationId() string {
	return r.activationId
}

func (r *runner) Stop() {
	r.requestStop(StopReasonSelf)
}

func (r *runner) requestStop(reason StopReason) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state < stateStopping {
		r.state = stateStopping
		r.reason = reason
		r.wakeLoop()
		for ready := range r.starting {
			ready <- ErrStopping
			delete(r.starting, ready)
		}
	}
	if reason != StopReasonSelf {
		r.cancel()
	}
}

func (r *runner) RunOnMainLoop(task Task) error {
	if task == nil {
		panic("actor: 必须提供 Task")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state >= stateStopping {
		return ErrStopping
	}
	select {
	case r.mailbox <- task:
		return nil
	default:
		return ErrMailboxFull
	}
}

func (r *runner) After(delay time.Duration, task Task) (Timer, error) {
	if delay < 0 {
		return nil, ErrInvalidTimer
	}
	return r.addTimer(delay, 0, task)
}

func (r *runner) Every(interval time.Duration, task Task) (Timer, error) {
	if interval <= 0 {
		return nil, ErrInvalidTimer
	}
	return r.addTimer(interval, interval, task)
}

func (r *runner) addTimer(delay, interval time.Duration, task Task) (Timer, error) {
	if task == nil {
		panic("actor: 必须提供定时任务")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state >= stateStopping {
		return nil, ErrStopping
	}
	timer := &actorTimer{
		owner:    r,
		next:     time.Now().Add(delay),
		interval: interval,
		task:     task,
		index:    -1,
	}
	heap.Push(&r.timers, timer)
	r.wakeLoop()
	return timer, nil
}

func (r *runner) wakeLoop() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

type actorTimer struct {
	owner    *runner
	next     time.Time
	interval time.Duration
	task     Task
	index    int
	canceled bool
}

var _ Handle = (*runner)(nil)
var _ Timer = (*actorTimer)(nil)

func (timer *actorTimer) Cancel() {
	r := timer.owner
	r.mu.Lock()
	defer r.mu.Unlock()
	if !timer.canceled {
		if timer.index >= 0 {
			heap.Remove(&r.timers, timer.index)
		}
		timer.canceled = true
		timer.task = nil
		r.wakeLoop()
	}
}

func (r *runner) loop(value Actor) {
	lastUpdate := time.Now()
	lastActive := lastUpdate
	nextUpdate := lastUpdate.Add(r.options.updateInterval)
	timer := time.NewTimer(r.options.updateInterval)
	defer timer.Stop()
	var batch []*actorTimer
	for {
		r.mu.Lock()
		if r.state != stateRunning {
			r.mu.Unlock()
			return
		}
		// 固定本轮时间边界，一次取出已到期任务；回调中新加的任务留到下一轮。
		now := time.Now()
		for len(r.timers) > 0 && !r.timers[0].next.After(now) {
			batch = append(batch, heap.Pop(&r.timers).(*actorTimer))
		}
		r.mu.Unlock()

		if !nextUpdate.IsZero() && !nextUpdate.After(now) {
			interval := value.OnUpdate(UpdateContext{
				Context: r.ctx,
				Self:    r,
				Delta:   now.Sub(lastUpdate),
				Idle:    now.Sub(lastActive),
			})
			lastUpdate = now
			if interval < 0 {
				nextUpdate = time.Time{}
			} else {
				if interval == 0 {
					interval = r.options.updateInterval
				}
				nextUpdate = time.Now().Add(interval)
			}
		}

		for _, scheduled := range batch {
			r.mu.Lock()
			// Cancel 或 Stop 可能发生在任务已取出、尚未开始执行时。
			if r.state != stateRunning || scheduled.canceled {
				scheduled.canceled = true
				scheduled.task = nil
				r.mu.Unlock()
				continue
			}
			task := scheduled.task
			if scheduled.interval == 0 {
				scheduled.canceled = true
				scheduled.task = nil
			}
			r.mu.Unlock()
			task(r.ctx)
			r.mu.Lock()
			if !scheduled.canceled && r.state == stateRunning {
				scheduled.next = time.Now().Add(scheduled.interval)
				heap.Push(&r.timers, scheduled)
			} else {
				scheduled.canceled = true
				scheduled.task = nil
			}
			r.mu.Unlock()
		}
		clear(batch)
		batch = batch[:0]

		r.mu.Lock()
		if r.state != stateRunning {
			r.mu.Unlock()
			return
		}
		next := nextUpdate
		if len(r.timers) > 0 && (next.IsZero() || r.timers[0].next.Before(next)) {
			next = r.timers[0].next
		}
		r.mu.Unlock()

		// 整批定时工作结束后先处理一个已排队任务，避免定时工作饿死邮箱。
		var task Task
		select {
		case task = <-r.mailbox:
		default:
		}
		if task == nil {
			var timerC <-chan time.Time
			if !next.IsZero() {
				timer.Reset(time.Until(next))
				timerC = timer.C
			} else {
				timer.Stop()
			}
			select {
			case <-r.ctx.Done():
				return
			case <-r.wake:
				continue
			case <-timerC:
				continue
			case task = <-r.mailbox:
			}
		}
		r.mu.Lock()
		running := r.state == stateRunning
		r.mu.Unlock()
		if !running {
			return
		}
		task(r.ctx)
		lastActive = time.Now()
	}
}
