package actor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func loopRunner(t *testing.T) *runner {
	t.Helper()
	_, client := loaderRedis(t)
	key := Key{Type: "room", Id: "1"}
	lease, _, _, err := NewGuard("node", client).TryAcquire(context.Background(), key, "a")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &runner{
		key:          key,
		activationId: "a",
		lease:        lease,
		options:      defaultOptions(),
		state:        stateRunning,
		ctx:          ctx,
		cancel:       cancel,
		mailbox:      make(chan *mailboxEntry, 16),
		wake:         make(chan struct{}, 1),
		done:         make(chan struct{}),
		starting:     make(map[chan error]struct{}),
	}
	r.options.updateInterval = time.Hour
	r.ctx = context.WithValue(ctx, executionContextKey{}, r)
	t.Cleanup(func() { cancel(); _ = lease.Release(context.Background()) })
	return r
}

func TestTimerBatchAndCancellation(t *testing.T) {
	r := loopRunner(t)
	r.options.updateInterval = time.Nanosecond
	var fired []string
	_, err := r.After(0, func(context.Context) { fired = append(fired, "first") })
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := r.After(0, func(context.Context) { t.Error("已取出但被取消的任务仍执行") })
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.After(0, func(context.Context) { fired = append(fired, "last"); r.Stop() })
	if err != nil {
		t.Fatal(err)
	}
	updates := 0
	r.loop(&messageActor{onUpdate: func(UpdateContext) time.Duration {
		updates++
		cancelled.Cancel()
		return -1
	}})
	if updates != 1 || len(fired) != 2 || fired[0] != "first" || fired[1] != "last" {
		t.Fatalf("updates=%d fired=%v", updates, fired)
	}
	if _, err := r.After(time.Second, func(context.Context) {}); !errors.Is(err, ErrStopping) {
		t.Fatal(err)
	}
	if _, err := r.After(-time.Second, func(context.Context) {}); !errors.Is(err, ErrInvalidTimer) {
		t.Fatal(err)
	}
	if _, err := r.Every(0, func(context.Context) {}); !errors.Is(err, ErrInvalidTimer) {
		t.Fatal(err)
	}
	assertPanic(t, func() { _, _ = r.After(0, nil) })
}

func TestPeriodicTimerCancelsItself(t *testing.T) {
	r := loopRunner(t)
	r.options.updateInterval = time.Nanosecond
	count := 0
	var timer Timer
	var err error
	timer, err = r.Every(time.Millisecond, func(context.Context) {
		count++
		if count == 2 {
			timer.Cancel()
			r.Stop()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	r.loop(&messageActor{})
	if count != 2 {
		t.Fatalf("定时任务执行次数=%d", count)
	}
	timer.Cancel()
}

// 观察主循环进入等待，便于精确安排出队和停止的竞争
type loopWaitContext struct {
	context.Context
	ready chan struct{}
	once  sync.Once
}

func (ctx *loopWaitContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.ready) })
	return ctx.Context.Done()
}

func TestLoopCancellationAndDequeuedEntryFences(t *testing.T) {
	for _, mode := range []string{"cancel", "stop", "lease"} {
		t.Run(mode, func(t *testing.T) {
			r := loopRunner(t)
			r.options.updateInterval = time.Nanosecond
			waiting := &loopWaitContext{Context: r.ctx, ready: make(chan struct{})}
			r.ctx = waiting
			go func() { r.loop(&messageActor{}); close(r.done) }()
			waitLoader(t, waiting.ready)
			if mode == "cancel" {
				r.cancel()
			} else {
				r.mu.Lock()
				reply := make(chan messageResult, 1)
				r.mailbox <- &mailboxEntry{ctx: context.Background(), reply: reply}
				want := ErrStopping
				if mode == "stop" {
					r.state = stateStopping
				} else {
					r.lease.expiry.Stop()
					r.lease.mu.Lock()
					r.lease.deadline = time.Now().Add(-time.Second)
					r.lease.mu.Unlock()
					want = ErrGuardLost
				}
				r.mu.Unlock()
				if result := <-reply; !errors.Is(result.err, want) {
					t.Fatalf("出队竞争错误: %v", result.err)
				}
			}
			waitLoader(t, r.done)
		})
	}
}

func TestRunnerRejectsExpiredLeaseBeforeLoading(t *testing.T) {
	r := loopRunner(t)
	r.state = stateStarting
	r.lease.expiry.Stop()
	r.lease.mu.Lock()
	r.lease.deadline = time.Now().Add(-time.Second)
	r.lease.mu.Unlock()
	err := r.run(actorType{loader: func(context.Context, Key) (Actor, error) {
		t.Error("租约在等待调度期间失效，仍执行了 Loader")
		return &loadedActor{}, nil
	}})
	if !errors.Is(err, ErrGuardLost) {
		t.Fatal(err)
	}
}

func TestRunnerRejectsNilActor(t *testing.T) {
	r := loopRunner(t)
	r.state = stateStarting
	assertPanic(t, func() { _ = r.run(actorType{loader: func(context.Context, Key) (Actor, error) { return nil, nil }}) })
}
