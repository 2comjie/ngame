package actor

import (
	"container/heap"
	"context"
	"errors"
	"sync"
	"time"
)

type runner struct {
	key          Key
	activationId string
	lease        *Lease
	options      options
	mu           sync.Mutex
	state        int
	reason       StopReason
	ctx          context.Context
	cancel       context.CancelFunc
	mailbox      chan *mailboxEntry
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

// checkLease 在回调前确认租约有效
func (r *runner) checkLease() bool {
	if r.lease.Active() {
		return true
	}
	r.requestStop(StopReasonLeaseLost)
	return false
}

func (r *runner) requestStop(reason StopReason) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state < stateStopping {
		r.state = stateStopping
		r.reason = reason
		r.wakeLoop()
		stopErr := ErrStopping
		if reason == StopReasonLeaseLost {
			stopErr = ErrGuardLost
		}
		r.drainMailbox(stopErr)
		for ready := range r.starting {
			ready <- stopErr
			delete(r.starting, ready)
		}
	}
	if reason == StopReasonLeaseLost {
		r.reason = reason
	}
	if reason != StopReasonSelf {
		r.cancel()
	}
}

var _ Handle = (*runner)(nil)

func (r *runner) wakeLoop() {
	select {
	case r.wake <- struct{}{}:
	default:
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
		if !r.checkLease() {
			return
		}
		r.mu.Lock()
		if r.state != stateRunning {
			r.mu.Unlock()
			return
		}
		// 按本轮时间取出到期任务，新任务留到下一轮
		now := time.Now()
		for len(r.timers) > 0 && !r.timers[0].next.After(now) {
			batch = append(batch, heap.Pop(&r.timers).(*actorTimer))
		}
		r.mu.Unlock()

		if !nextUpdate.IsZero() && !nextUpdate.After(now) && r.checkLease() {
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
			r.checkLease()
			r.mu.Lock()
			// 取出后仍可能被 Cancel 或 Stop
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

		// 定时任务后优先处理邮箱，避免饥饿
		var entry *mailboxEntry
		select {
		case entry = <-r.mailbox:
		default:
		}
		if entry == nil {
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
			case entry = <-r.mailbox:
			}
		}
		r.mu.Lock()
		running := r.state == stateRunning
		r.mu.Unlock()
		if !running {
			if entry.reply != nil {
				entry.reply <- messageResult{err: ErrStopping}
			}
			return
		}
		if !r.checkLease() {
			if entry.reply != nil {
				entry.reply <- messageResult{err: ErrGuardLost}
			}
			return
		}
		if r.execute(value, entry) {
			lastActive = time.Now()
		}
	}
}

func (r *runner) run(definition actorType) error {
	lease := r.lease
	stopLeaseWatch := context.AfterFunc(lease.ctx, func() { r.requestStop(StopReasonLeaseLost) })
	defer stopLeaseWatch()
	if !r.checkLease() {
		return ErrGuardLost
	}
	r.mu.Lock()
	starting := r.state == stateStarting
	r.mu.Unlock()
	if !starting {
		return ErrStopping
	}

	startCtx, cancel := context.WithTimeout(r.ctx, r.options.lifecycleTimeout)
	defer cancel()
	value, err := definition.loader(startCtx, r.key)
	if errors.Is(err, ErrEntityNotFound) && definition.onNotFound != nil {
		if !r.checkLease() {
			return ErrGuardLost
		}
		if err = definition.onNotFound(startCtx, r.key); err != nil {
			return err
		}
		value, err = definition.loader(startCtx, r.key)
	}
	if err != nil {
		return err
	}
	if value == nil {
		panic("actor: Loader 返回了空 Actor")
	}
	if !r.checkLease() {
		return ErrGuardLost
	}
	if err = startCtx.Err(); err != nil {
		return err
	}
	err = value.OnStart(StartContext{
		Context: startCtx,
		Self:    r,
	})
	started := err == nil
	if started {
		err = startCtx.Err()
	}
	cancel()
	r.mu.Lock()
	if !lease.Active() {
		err = ErrGuardLost
	} else if err == nil && r.state != stateStarting {
		err = ErrStopping
	}
	if err == nil {
		r.state = stateRunning
		for ready := range r.starting {
			ready <- nil
			delete(r.starting, ready)
		}
	}
	r.mu.Unlock()
	if err == nil {
		r.loop(value)
	}
	r.mu.Lock()
	r.state = stateStopping
	if !lease.Active() {
		r.reason = StopReasonLeaseLost
	} else if r.reason == 0 {
		r.reason = StopReasonSelf
	}
	reason := r.reason
	for _, timer := range r.timers {
		timer.index = -1
		timer.canceled = true
		timer.task = nil
	}
	r.timers = nil
	// 清理排队闭包，避免旧 Handle 持有业务对象
	r.drainMailbox(ErrStopping)
	r.mu.Unlock()
	if started {
		// 保存完成前持有租约，租约失效时取消 OnStop
		stopCtx, stop := context.WithTimeout(lease.ctx, r.options.lifecycleTimeout)
		if !lease.Active() {
			stop()
		}
		value.OnStop(StopContext{
			Context: context.WithValue(stopCtx, executionContextKey{}, r),
			Self:    r,
			Reason:  reason,
		})
		stop()
	}
	return err
}
