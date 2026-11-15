package actor

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/2comjie/ntool/logx"
	"github.com/2comjie/ntool/logx/logdef"
	"golang.org/x/sync/semaphore"
)

type runner struct {
	key          Key
	activationId string
	lease        *Lease
	options      options
	mu           sync.Mutex
	state        int
	startSlot    *semaphore.Weighted
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
		stopErr := stopError(reason)
		r.drainMailbox(stopErr)
		for ready := range r.starting {
			ready <- stopErr
			delete(r.starting, ready)
		}
	}
	if reason == StopReasonPanic || reason == StopReasonLeaseLost && r.reason != StopReasonPanic {
		r.reason = reason
	}
	if reason != StopReasonSelf {
		r.cancel()
	}
}

func stopError(reason StopReason) error {
	switch reason {
	case StopReasonPanic:
		return ErrPanic
	case StopReasonLeaseLost:
		return ErrGuardLost
	default:
		return ErrStopping
	}
}

func (r *runner) panicError(callback string, value any) error {
	r.requestStop(StopReasonPanic)
	logx.WithFields(logdef.Fields{
		"type":          r.key.Type,
		"id":            r.key.Id,
		"activation_id": r.activationId,
		"callback":      callback,
		"panic":         fmt.Sprint(value),
		"stack":         string(debug.Stack()),
	}).Error("actor: 业务回调 panic，停止当前激活")
	return ErrPanic
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
	defer func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		// 到期任务已移出堆，异常退出时仍需释放闭包
		for _, scheduled := range batch {
			scheduled.canceled = true
			scheduled.task = nil
		}
	}()
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

func (r *runner) run(definition actorType) (err error) {
	lease := r.lease
	stopLeaseWatch := context.AfterFunc(lease.ctx, func() { r.requestStop(StopReasonLeaseLost) })
	defer stopLeaseWatch()
	var value Actor
	var started bool
	callback := ""
	defer func() {
		if recovered := recover(); recovered != nil {
			if callback == "" {
				panic(recovered)
			}
			err = r.panicError(callback, recovered)
		}
		r.mu.Lock()
		r.state = stateStopping
		if r.reason != StopReasonPanic {
			if !lease.Active() {
				r.reason = StopReasonLeaseLost
			} else if r.reason == 0 {
				r.reason = StopReasonSelf
			}
		}
		reason := r.reason
		if reason == StopReasonPanic && err == nil {
			err = ErrPanic
		}
		for _, timer := range r.timers {
			timer.index = -1
			timer.canceled = true
			timer.task = nil
		}
		r.timers = nil
		r.drainMailbox(stopError(reason))
		r.mu.Unlock()
		if !started {
			return
		}
		// 清理完成前持有租约，租约失效时取消 OnStop
		stopCtx, stop := context.WithTimeout(lease.ctx, r.options.lifecycleTimeout)
		defer stop()
		if !lease.Active() {
			stop()
		}
		defer func() {
			if recovered := recover(); recovered != nil {
				stopErr := r.panicError("OnStop", recovered)
				if err == nil {
					err = stopErr
				}
			}
		}()
		value.OnStop(StopContext{
			Context: context.WithValue(stopCtx, executionContextKey{}, r),
			Self:    r,
			Reason:  reason,
		})
	}()
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
	callback = "Loader"
	value, err = definition.loader(startCtx, r.key)
	callback = ""
	if errors.Is(err, ErrEntityNotFound) && definition.onNotFound != nil {
		if !r.checkLease() {
			return ErrGuardLost
		}
		callback = "OnNotFound"
		err = definition.onNotFound(startCtx, r.key)
		callback = ""
		if err != nil {
			return err
		}
		callback = "Loader"
		value, err = definition.loader(startCtx, r.key)
		callback = ""
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
	callback = "OnStart"
	err = value.OnStart(StartContext{
		Context: startCtx,
		Self:    r,
	})
	callback = ""
	started = err == nil
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
		r.startSlot.Release(1)
		r.startSlot = nil
		for ready := range r.starting {
			ready <- nil
			delete(r.starting, ready)
		}
	}
	r.mu.Unlock()
	if err == nil {
		callback = "MainLoop"
		r.loop(value)
	}
	return err
}
