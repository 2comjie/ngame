package actor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func waitPanicCleanup(t *testing.T, r *runner) {
	t.Helper()
	waitLoader(t, r.done)
	r.mu.Lock()
	cleaned := r.state == stateStopped && r.reason == StopReasonPanic && r.startSlot == nil &&
		len(r.mailbox) == 0 && len(r.timers) == 0 && len(r.starting) == 0
	r.mu.Unlock()
	if !cleaned {
		t.Fatal("panic 后未清理实例、名额、邮箱或定时任务")
	}
	if _, found, err := r.lease.guard.Owner(context.Background(), r.key); err != nil || found {
		t.Fatalf("panic 后未释放归属: found=%v error=%v", found, err)
	}
	if err := r.RunOnMainLoop(func(context.Context) {}); !errors.Is(err, ErrStopping) {
		t.Fatalf("旧 Handle 仍接受任务: %v", err)
	}
}

func TestStartupPanicNotifiesSharedWaiters(t *testing.T) {
	for _, stage := range []string{"Loader", "OnNotFound", "OnStart"} {
		t.Run(stage, func(t *testing.T) {
			_, redisClient := loaderRedis(t)
			entered, resume := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			fail := func() {
				close(entered)
				<-resume
				panic(stage)
			}
			var loads atomic.Int32
			system := New(NewGuard("node", redisClient), WithMaxActors(1), WithMaxStarting(1))
			system.Register("player", func(context.Context, Key) (Actor, error) {
				if loads.Add(1) > 1 {
					return &loadedActor{}, nil
				}
				switch stage {
				case "Loader":
					fail()
				case "OnNotFound":
					return nil, ErrEntityNotFound
				case "OnStart":
					return &loadedActor{onStart: func(ctx StartContext) error {
						if _, err := ctx.Self.After(time.Hour, func(context.Context) {}); err != nil {
							t.Error(err)
						}
						fail()
						return nil
					}}, nil
				}
				return &loadedActor{}, nil
			}, WithOnNotFound(func(context.Context, Key) error {
				fail()
				return nil
			}))
			if err := system.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := system.Shutdown(ctx); err != nil {
					t.Error(err)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			key := Key{Type: "player", Id: "1"}
			const callers = 8
			results := make(chan error, callers)
			for range callers {
				go func() {
					_, err := system.Spawn(ctx, key)
					results <- err
				}()
			}
			waitLoader(t, entered)
			system.mutex.Lock()
			r := system.actors[key]
			system.mutex.Unlock()
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				r.mu.Lock()
				waiting := len(r.starting)
				r.mu.Unlock()
				if waiting == callers {
					break
				}
				select {
				case <-ticker.C:
				case <-ctx.Done():
					t.Fatal("并发 Spawn 未全部加入共享激活")
				}
			}
			unblock()
			for range callers {
				if err := <-results; !errors.Is(err, ErrPanic) {
					t.Fatalf("启动 panic 未通知等待者: %v", err)
				}
			}
			waitPanicCleanup(t, r)
			if _, err := system.Spawn(ctx, key); err != nil || loads.Load() != 2 {
				t.Fatalf("panic 后未恢复激活名额: loads=%d error=%v", loads.Load(), err)
			}
		})
	}
}

func TestMessagePanicDrainsConcurrentCalls(t *testing.T) {
	_, redisClient := loaderRedis(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	stopEntered, stopResume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	unblockStop := sync.OnceFunc(func() { close(stopResume) })
	defer unblock()
	defer unblockStop()
	var unexpected, loads atomic.Int32
	value := &unloadActor{onStop: func(ctx StopContext) {
		if ctx.Reason != StopReasonPanic || ctx.Err() != nil {
			t.Errorf("异常停止上下文: reason=%v error=%v", ctx.Reason, ctx.Err())
		}
		close(stopEntered)
		<-stopResume
	}}
	value.onMessage = func(ctx MessageContext, message Message) ([]byte, error) {
		if message.Method != "panic" {
			unexpected.Add(1)
			return nil, nil
		}
		close(entered)
		<-resume
		panic("OnMessage")
	}
	system := loaderSystem(t, NewGuard("node", redisClient), func(_ context.Context, key Key) (Actor, error) {
		if key.Id == "fault" && loads.Add(1) == 1 {
			return value, nil
		}
		return &loadedActor{}, nil
	}, WithMaxActors(2), WithMaxStarting(1))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	healthy := Key{Type: "player", Id: "healthy"}
	if _, err := system.Spawn(ctx, healthy); err != nil {
		t.Fatal(err)
	}
	key := Key{Type: "player", Id: "fault"}
	handle, err := system.Spawn(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	timer, err := handle.After(time.Hour, func(context.Context) { unexpected.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	const queued = 8
	results := make(chan error, queued+2)
	go func() {
		_, err := localAsk(system, ctx, key, Message{Method: "panic"})
		results <- err
	}()
	waitLoader(t, entered)
	for range queued {
		go func() {
			_, err := localAsk(system, ctx, key, Message{Method: "queued"})
			results <- err
		}()
	}
	go func() {
		results <- handle.WaitResultOnMainLoop(ctx, func(context.Context) error {
			unexpected.Add(1)
			return nil
		})
	}()
	if err := localTell(system, ctx, key, Message{Method: "queued-tell"}); err != nil {
		t.Fatal(err)
	}
	if err := handle.RunOnMainLoop(func(context.Context) { unexpected.Add(1) }); err != nil {
		t.Fatal(err)
	}
	waitMessageQueue(t, handle, queued+3)
	unblock()
	waitLoader(t, stopEntered)
	for range queued + 2 {
		if err := <-results; !errors.Is(err, ErrPanic) {
			t.Fatalf("panic 未终止当前或排队调用: %v", err)
		}
	}
	if _, found, err := system.guard.Owner(ctx, key); err != nil || !found {
		t.Fatalf("OnStop 完成前释放了租约: found=%v error=%v", found, err)
	}
	if _, err := localAsk(system, ctx, healthy, Message{}); err != nil {
		t.Fatalf("其他 Actor 受到影响: %v", err)
	}
	unblockStop()
	r := handle.(*runner)
	waitPanicCleanup(t, r)
	if unexpected.Load() != 0 {
		t.Fatalf("panic 后继续执行业务: %d", unexpected.Load())
	}
	retained := timer.(*actorTimer)
	r.mu.Lock()
	cleaned := retained.canceled && retained.task == nil && retained.index == -1
	r.mu.Unlock()
	if !cleaned {
		t.Fatal("旧定时任务仍持有业务闭包")
	}
	next, err := system.Spawn(ctx, key)
	if err != nil || next.ActivationId() == handle.ActivationId() {
		t.Fatalf("未创建新的激活实例: handle=%v error=%v", next, err)
	}
}

func TestTaskPanicStopsActor(t *testing.T) {
	for _, kind := range []string{"RunOnMainLoop", "WaitResultOnMainLoop", "Tell"} {
		t.Run(kind, func(t *testing.T) {
			value := &messageActor{onMessage: func(MessageContext, Message) ([]byte, error) {
				panic("Tell")
			}}
			system, key, handle := spawnMessageActor(t, value)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			var err error
			switch kind {
			case "RunOnMainLoop":
				err = handle.RunOnMainLoop(func(context.Context) { panic("Task") })
			case "WaitResultOnMainLoop":
				err = handle.WaitResultOnMainLoop(ctx, func(context.Context) error { panic("WaitTask") })
			case "Tell":
				err = localTell(system, ctx, key, Message{})
			}
			if kind == "WaitResultOnMainLoop" {
				if !errors.Is(err, ErrPanic) {
					t.Fatalf("同步闭包未收到 panic 错误: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			waitPanicCleanup(t, handle.(*runner))
		})
	}
}

func TestLoopPanicClearsTimers(t *testing.T) {
	for _, kind := range []string{"OnUpdate", "After", "Every", "OnUpdateAndOnStop"} {
		t.Run(kind, func(t *testing.T) {
			_, redisClient := loaderRedis(t)
			entered, resume := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			fail := func() {
				close(entered)
				<-resume
				panic(kind)
			}
			var timers []Timer
			var unexpected atomic.Int32
			value := &unloadActor{onStop: func(ctx StopContext) {
				if ctx.Reason != StopReasonPanic {
					t.Errorf("OnStop 原因错误: %v", ctx.Reason)
				}
				if kind == "OnUpdateAndOnStop" {
					panic("OnStop")
				}
			}}
			value.onStart = func(ctx StartContext) error {
				switch kind {
				case "After":
					first, err := ctx.Self.After(0, func(context.Context) { fail() })
					if err != nil {
						return err
					}
					second, err := ctx.Self.After(0, func(context.Context) { unexpected.Add(1) })
					if err != nil {
						return err
					}
					timers = []Timer{first, second}
				case "Every":
					timer, err := ctx.Self.Every(time.Millisecond, func(context.Context) { fail() })
					if err != nil {
						return err
					}
					timers = []Timer{timer}
				}
				return nil
			}
			value.onUpdate = func(UpdateContext) time.Duration {
				if kind == "OnUpdate" || kind == "OnUpdateAndOnStop" {
					fail()
				}
				return -1
			}
			system := loaderSystem(t, NewGuard("node", redisClient), func(context.Context, Key) (Actor, error) {
				return value, nil
			}, WithUpdateInterval(time.Millisecond))
			handle, err := system.Spawn(context.Background(), Key{Type: "player", Id: "1"})
			if err != nil {
				t.Fatal(err)
			}
			waitLoader(t, entered)
			unblock()
			r := handle.(*runner)
			waitPanicCleanup(t, r)
			if unexpected.Load() != 0 {
				t.Fatal("panic 后继续执行同批定时任务")
			}
			r.mu.Lock()
			for _, timer := range timers {
				value := timer.(*actorTimer)
				if !value.canceled || value.task != nil || value.index != -1 {
					t.Error("移出调度堆的定时任务未清理")
				}
			}
			r.mu.Unlock()
		})
	}
}

func TestOnStopPanicReleasesActivation(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var loads atomic.Int32
	system := loaderSystem(t, NewGuard("node", redisClient), func(context.Context, Key) (Actor, error) {
		if loads.Add(1) == 1 {
			return &unloadActor{onStop: func(StopContext) { panic("OnStop") }}, nil
		}
		return &loadedActor{}, nil
	}, WithMaxActors(1), WithMaxStarting(1))
	key := Key{Type: "player", Id: "1"}
	handle, err := system.Spawn(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := system.Stop(ctx, key); !errors.Is(err, ErrPanic) {
		t.Fatalf("停止等待者未收到 panic 错误: %v", err)
	}
	waitPanicCleanup(t, handle.(*runner))
	if _, err := system.Spawn(ctx, key); err != nil {
		t.Fatalf("OnStop panic 后无法重新激活: %v", err)
	}
}

func TestRPCPanicDoesNotReplayMessage(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var loads, calls atomic.Int32
	started := make(chan Handle, 1)
	node := newActorRPCNode(t, redisClient, "game", "game", map[string]Loader{"room": func(context.Context, Key) (Actor, error) {
		first := loads.Add(1) == 1
		value := &messageActor{onMessage: func(MessageContext, Message) ([]byte, error) {
			calls.Add(1)
			if first {
				panic("RPC OnMessage")
			}
			return []byte("ok"), nil
		}}
		if first {
			value.onStart = func(ctx StartContext) error {
				started <- ctx.Self
				return nil
			}
		}
		return value, nil
	}})
	client := actorClient(t, redisClient, actorDiscover(t, node), map[string]string{"room": "game"})
	ref := client.Ref(Key{Type: "room", Id: "1"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := ref.Ask(ctx, Message{Method: "panic"}, ActivateIfMissing); status.Code(err) != codes.Internal {
		t.Fatalf("panic 未作为 Internal 返回: %v", err)
	}
	if calls.Load() != 1 || loads.Load() != 1 {
		t.Fatalf("panic 请求被重放: calls=%d loads=%d", calls.Load(), loads.Load())
	}
	old := <-started
	waitPanicCleanup(t, old.(*runner))
	body, err := ref.Ask(ctx, Message{Method: "next"}, ActivateIfMissing)
	if err != nil || string(body) != "ok" || calls.Load() != 2 || loads.Load() != 2 {
		t.Fatalf("原 Ref 无法重新激活: body=%s calls=%d loads=%d error=%v", body, calls.Load(), loads.Load(), err)
	}
}
