package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/2comjie/ngame/app"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type loadedActor struct {
	data    int
	onStart func(StartContext) error
}

func (a *loadedActor) OnStart(ctx StartContext) error {
	if a.onStart != nil {
		return a.onStart(ctx)
	}
	return nil
}
func (*loadedActor) OnUpdate(UpdateContext) time.Duration { return -1 }
func (*loadedActor) OnStop(StopContext)                   {}
func (*loadedActor) OnMessage(MessageContext, Message) ([]byte, error) {
	return nil, nil
}

func loaderRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr:                  server.Addr(),
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

func loaderSystem(t *testing.T, guard *Guard, loader Loader, opts ...Option) *System {
	t.Helper()
	s := New(guard, opts...)
	s.Register("player", loader)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}

func waitLoader(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("等待 Loader 超时")
	}
}

func TestLoaderSharedActivation(t *testing.T) {
	_, client := loaderRedis(t)
	guard := NewGuard("node-a", client)
	key := Key{Type: "player", Id: "1"}
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var loads, starts atomic.Int32
	data := &loadedActor{data: 42}
	data.onStart = func(ctx StartContext) error {
		starts.Add(1)
		if data.data != 42 || ctx.Self.Key() != key {
			t.Error("OnStart 未得到已加载的数据或正确的 Self")
		}
		return nil
	}
	s := loaderSystem(t, guard, func(ctx context.Context, got Key) (Actor, error) {
		loads.Add(1)
		owner, found, err := guard.Owner(ctx, got)
		if err != nil || !found || owner.NodeId != "node-a" || got != key {
			t.Errorf("Loader 未在持有 Guard 时执行: owner=%+v found=%v error=%v", owner, found, err)
		}
		close(entered)
		select {
		case <-resume:
			return data, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := s.Spawn(ctx, key)
		first <- err
	}()
	waitLoader(t, entered)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("取消等待者: %v", err)
	}
	var wait sync.WaitGroup
	handles := make(chan Handle, 32)
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			handle, err := s.Spawn(context.Background(), key)
			if err != nil {
				t.Error(err)
				return
			}
			handles <- handle
		}()
	}
	unblock()
	wait.Wait()
	close(handles)
	var firstHandle Handle
	for handle := range handles {
		if firstHandle != nil && handle != firstHandle {
			t.Error("同 Key 返回不同 Handle")
		}
		firstHandle = handle
	}
	if loads.Load() != 1 || starts.Load() != 1 || firstHandle == nil {
		t.Fatalf("loads=%d starts=%d handle=%v", loads.Load(), starts.Load(), firstHandle)
	}
}

func TestLoaderFailureReleasesOwnership(t *testing.T) {
	_, client := loaderRedis(t)
	guard := NewGuard("node-a", client)
	failed := errors.New("业务实体不存在")
	s1 := loaderSystem(t, guard, func(context.Context, Key) (Actor, error) {
		return nil, fmt.Errorf("加载玩家: %w", failed)
	})
	s2 := loaderSystem(t, NewGuard("node-b", client), func(context.Context, Key) (Actor, error) {
		return &loadedActor{}, nil
	})
	key := Key{Type: "player", Id: "1"}
	if _, err := s1.Spawn(context.Background(), key); !errors.Is(err, failed) {
		t.Fatalf("Loader 错误未返回: %v", err)
	}
	if _, found, err := guard.Owner(context.Background(), key); err != nil || found {
		t.Fatalf("加载失败未释放所有权: found=%v error=%v", found, err)
	}
	if _, err := s2.Spawn(context.Background(), key); err != nil {
		t.Fatalf("其他节点无法重新激活: %v", err)
	}
}

func TestLoaderNotCalledOnOtherNode(t *testing.T) {
	_, client := loaderRedis(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	s1 := loaderSystem(t, NewGuard("node-a", client), func(ctx context.Context, key Key) (Actor, error) {
		close(entered)
		select {
		case <-resume:
			return &loadedActor{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	var loads atomic.Int32
	s2 := loaderSystem(t, NewGuard("node-b", client), func(context.Context, Key) (Actor, error) {
		loads.Add(1)
		return &loadedActor{}, nil
	})
	key := Key{Type: "player", Id: "1"}
	result := make(chan error, 1)
	go func() {
		_, err := s1.Spawn(context.Background(), key)
		result <- err
	}()
	waitLoader(t, entered)
	_, err := s2.Spawn(context.Background(), key)
	var owned *OwnedError
	if !errors.As(err, &owned) || owned.Owner.NodeId != "node-a" || loads.Load() != 0 {
		t.Fatalf("抢占失败仍执行 Loader: loads=%d error=%v", loads.Load(), err)
	}
	unblock()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestLoaderCancelledOnLeaseLoss(t *testing.T) {
	server, client := loaderRedis(t)
	guard := NewGuard("node-a", client, WithGuardTTL(300*time.Millisecond))
	entered := make(chan struct{})
	s := loaderSystem(t, guard, func(ctx context.Context, key Key) (Actor, error) {
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	})
	key := Key{Type: "player", Id: "1"}
	result := make(chan error, 1)
	go func() {
		_, err := s.Spawn(context.Background(), key)
		result <- err
	}()
	waitLoader(t, entered)
	server.FastForward(time.Second)
	select {
	case err := <-result:
		if !errors.Is(err, ErrGuardLost) {
			t.Fatalf("租约失效: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("租约失效未取消 Loader")
	}
	if err := s.Stop(context.Background(), key); err != nil {
		t.Fatal(err)
	}
}

func TestLoaderLifecycleTimeout(t *testing.T) {
	_, client := loaderRedis(t)
	guard := NewGuard("node-a", client)
	s := loaderSystem(t, guard, func(ctx context.Context, key Key) (Actor, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}, WithLifecycleTimeout(30*time.Millisecond))
	key := Key{Type: "player", Id: "1"}
	if _, err := s.Spawn(context.Background(), key); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("加载未受生命周期超时限制: %v", err)
	}
	if _, found, err := guard.Owner(context.Background(), key); err != nil || found {
		t.Fatalf("加载超时未释放所有权: found=%v error=%v", found, err)
	}
}

func TestSpawnCancelledBeforeCreatingRunner(t *testing.T) {
	_, client := loaderRedis(t)
	var loads atomic.Int32
	system := loaderSystem(t, NewGuard("node-a", client), func(context.Context, Key) (Actor, error) {
		loads.Add(1)
		return &loadedActor{}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	key := Key{Type: "player", Id: "1"}
	if _, err := system.Spawn(ctx, key); !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消调用启动了新激活: %v", err)
	}
	system.mutex.Lock()
	count := len(system.actors)
	system.mutex.Unlock()
	if count != 0 || loads.Load() != 0 {
		t.Fatalf("取消后仍创建 runner 或加载实体: runners=%d loads=%d", count, loads.Load())
	}
}

func TestSystemConfigurationAndStateBoundaries(t *testing.T) {
	_, client := loaderRedis(t)
	guard := NewGuard("node", client)
	loader := func(context.Context, Key) (Actor, error) { return &loadedActor{}, nil }
	for _, run := range []func(){
		func() { New(nil) },
		func() { New(guard, WithMaxActors(0)) },
		func() { New(guard, WithMaxActors(-1)) },
		func() { New(guard, WithMaxStarting(0)) },
		func() { New(guard, WithMaxStarting(-1)) },
		func() { New(guard, WithMailboxSize(0)) },
		func() { New(guard, WithUpdateInterval(0)) },
		func() { New(guard, WithLifecycleTimeout(0)) },
	} {
		assertPanic(t, run)
	}
	system := New(guard)
	assertPanic(t, func() { system.Register("", loader) })
	assertPanic(t, func() { system.Register("player", nil) })
	system.Register("player", loader)
	assertPanic(t, func() { system.Register("player", loader) })
	key := Key{Type: "player", Id: "1"}
	if _, err := system.Spawn(context.Background(), key); !errors.Is(err, ErrNotStarted) {
		t.Fatal(err)
	}
	if err := system.Start(); err != nil {
		t.Fatal(err)
	}
	if err := system.Start(); !errors.Is(err, app.ErrStarted) {
		t.Fatal(err)
	}
	assertPanic(t, func() { system.Register("room", loader) })
	if _, err := system.Spawn(context.Background(), Key{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatal(err)
	}
	if _, err := system.Spawn(context.Background(), Key{Type: "missing", Id: "1"}); !errors.Is(err, ErrTypeNotRegistered) {
		t.Fatal(err)
	}
	if err := system.Stop(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := system.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := system.Start(); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	if _, err := system.Spawn(context.Background(), key); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
}

func TestSystemStopWaitCanBeCancelled(t *testing.T) {
	_, client := loaderRedis(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	system := loaderSystem(t, NewGuard("node", client), func(context.Context, Key) (Actor, error) {
		return &unloadActor{onStop: func(StopContext) { close(entered); <-resume }}, nil
	})
	key := Key{Type: "player", Id: "1"}
	handle, err := system.Spawn(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	handle.Stop()
	waitLoader(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := system.Stop(ctx, key); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := system.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	unblock()
	if err := system.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStartupFailureAndLeaseFences(t *testing.T) {
	for _, mode := range []string{"redis-failure", "release-failure", "stop-before-load", "lost-on-missing", "lost-after-load", "lost-on-start", "load-expired", "start-expired", "self-stop", "pending-timer"} {
		t.Run(mode, func(t *testing.T) {
			_, client := loaderRedis(t)
			guard := NewGuard("node", client)
			var system *System
			var stopped atomic.Int32
			if mode == "stop-before-load" {
				client.AddHook(commandHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
					err := next(ctx, cmd)
					if err == nil && (cmd.Name() == "eval" || cmd.Name() == "evalsha") && (cmd.Args()[1] == acquireScript || cmd.Args()[1] == guardAcquire.Hash()) {
						system.RequestStop()
					}
					return err
				}))
			}
			system = loaderSystem(t, guard, func(ctx context.Context, key Key) (Actor, error) {
				r := system.actors[key]
				if mode == "lost-on-missing" {
					r.lease.cancel()
					return nil, ErrEntityNotFound
				}
				if mode == "lost-after-load" {
					r.lease.cancel()
				}
				if mode == "load-expired" {
					<-ctx.Done()
				}
				value := &unloadActor{onStop: func(StopContext) { stopped.Add(1) }}
				value.onStart = func(ctx StartContext) error {
					switch mode {
					case "release-failure":
						_ = client.Close()
						ctx.Self.Stop()
					case "start-expired":
						<-ctx.Done()
					case "lost-on-start":
						r.lease.cancel()
					case "self-stop":
						ctx.Self.Stop()
					case "pending-timer":
						_, _ = ctx.Self.After(time.Hour, func(context.Context) {})
						_ = ctx.Self.RunOnMainLoop(func(context.Context) { ctx.Self.Stop() })
					}
					return nil
				}
				return value, nil
			}, WithLifecycleTimeout(10*time.Millisecond))
			if mode == "lost-on-missing" {
				system.types["player"] = actorType{loader: system.types["player"].loader, onNotFound: func(context.Context, Key) error { t.Error("租约失效仍创建实体"); return nil }}
			}
			if mode == "redis-failure" {
				_ = client.Close()
			}
			key := Key{Type: "player", Id: "1"}
			_, err := system.Spawn(context.Background(), key)
			if mode == "pending-timer" || mode == "release-failure" {
				if err != nil && !errors.Is(err, ErrStopping) {
					t.Fatal(err)
				}
			} else if err == nil {
				t.Fatal("失败激活返回成功")
			}
			if err := system.Stop(context.Background(), key); err != nil {
				t.Fatal(err)
			}
			if (mode == "self-stop" || mode == "start-expired" || mode == "pending-timer" || mode == "release-failure") && stopped.Load() != 1 {
				t.Fatalf("OnStop 次数=%d", stopped.Load())
			}
		})
	}
}

func notFoundSystem(t *testing.T, guard *Guard, loader Loader, opts ...TypeOption) *System {
	t.Helper()
	s := New(guard)
	s.Register("player", loader, opts...)
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestOnNotFoundPolicy(t *testing.T) {
	databaseFailure := errors.New("数据库不可用")
	creationFailure := errors.New("账号未注册")
	cases := []struct {
		name        string
		loadErr     error
		withHandler bool
		handlerErr  error
		reloadErr   error
		wantErr     error
		wantLoads   int32
		wantMissing int32
	}{
		{
			name:      "无回调不自动创建",
			loadErr:   ErrEntityNotFound,
			wantErr:   ErrEntityNotFound,
			wantLoads: 1,
		},
		{
			name:        "包装的缺失错误触发创建并重新加载",
			loadErr:     fmt.Errorf("加载玩家: %w", ErrEntityNotFound),
			withHandler: true,
			wantLoads:   2,
			wantMissing: 1,
		},
		{
			name:        "数据库错误不触发创建",
			loadErr:     databaseFailure,
			withHandler: true,
			wantErr:     databaseFailure,
			wantLoads:   1,
		},
		{
			name:        "创建条件不满足直接返回",
			loadErr:     ErrEntityNotFound,
			withHandler: true,
			handlerErr:  creationFailure,
			wantErr:     creationFailure,
			wantLoads:   1,
			wantMissing: 1,
		},
		{
			name:        "创建后仍缺失只重查一次",
			loadErr:     ErrEntityNotFound,
			withHandler: true,
			reloadErr:   ErrEntityNotFound,
			wantErr:     ErrEntityNotFound,
			wantLoads:   2,
			wantMissing: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, client := loaderRedis(t)
			guard := NewGuard("node-a", client)
			key := Key{Type: "player", Id: "1"}
			var loads, missing atomic.Int32
			var opts []TypeOption
			if tc.withHandler {
				opts = append(opts, WithOnNotFound(func(ctx context.Context, got Key) error {
					missing.Add(1)
					owner, found, err := guard.Owner(ctx, got)
					if err != nil || !found || owner.NodeId != "node-a" || got != key {
						t.Errorf("OnNotFound 未持有归属: owner=%+v found=%v error=%v", owner, found, err)
					}
					return tc.handlerErr
				}))
			}
			s := notFoundSystem(t, guard, func(context.Context, Key) (Actor, error) {
				if loads.Add(1) == 1 {
					return nil, tc.loadErr
				}
				if tc.reloadErr != nil {
					return nil, tc.reloadErr
				}
				return &loadedActor{}, nil
			}, opts...)
			_, err := s.Spawn(context.Background(), key)
			if !errors.Is(err, tc.wantErr) || loads.Load() != tc.wantLoads || missing.Load() != tc.wantMissing {
				t.Fatalf("error=%v loads=%d missing=%d", err, loads.Load(), missing.Load())
			}
			if tc.wantErr != nil {
				if _, found, err := guard.Owner(context.Background(), key); err != nil || found {
					t.Fatalf("激活失败未释放归属: found=%v error=%v", found, err)
				}
			}
		})
	}
}

func TestConcurrentAutoCreate(t *testing.T) {
	_, client := loaderRedis(t)
	var exists atomic.Bool
	var creates atomic.Int32
	s := notFoundSystem(t, NewGuard("node-a", client), func(context.Context, Key) (Actor, error) {
		if !exists.Load() {
			return nil, ErrEntityNotFound
		}
		return &loadedActor{}, nil
	}, WithOnNotFound(func(context.Context, Key) error {
		creates.Add(1)
		exists.Store(true)
		return nil
	}))
	key := Key{Type: "player", Id: "1"}
	var wait sync.WaitGroup
	start := make(chan struct{})
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			if _, err := s.Spawn(context.Background(), key); err != nil {
				t.Error(err)
			}
		}()
	}
	close(start)
	wait.Wait()
	if creates.Load() != 1 {
		t.Fatalf("并发自动创建次数=%d", creates.Load())
	}
}

func TestActivationLimitsConcurrent(t *testing.T) {
	for _, limits := range []struct {
		name        string
		maxActors   int
		maxStarting int
		accepted    int
	}{
		{"actor-count", 8, 16, 8},
		{"starting-count", 32, 4, 4},
	} {
		for _, phase := range []string{"loader", "on-start"} {
			t.Run(limits.name+"/"+phase, func(t *testing.T) {
				_, client := loaderRedis(t)
				const requests = 256
				entered := make(chan struct{}, requests)
				resume := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(resume) })
				defer unblock()
				var active, peak, loads atomic.Int32
				var system *System
				system = loaderSystem(t, NewGuard("node", client), func(ctx context.Context, key Key) (Actor, error) {
					loads.Add(1)
					current := active.Add(1)
					for previous := peak.Load(); current > previous; previous = peak.Load() {
						if peak.CompareAndSwap(previous, current) {
							break
						}
					}
					system.mutex.Lock()
					count := len(system.actors)
					system.mutex.Unlock()
					if count > limits.maxActors || current > int32(limits.maxStarting) {
						t.Errorf("突破限额 actors=%d starting=%d", count, current)
					}
					if phase == "loader" {
						entered <- struct{}{}
						select {
						case <-resume:
						case <-ctx.Done():
							active.Add(-1)
							return nil, ctx.Err()
						}
					}
					return &loadedActor{onStart: func(ctx StartContext) error {
						defer active.Add(-1)
						if phase == "on-start" {
							entered <- struct{}{}
							select {
							case <-resume:
							case <-ctx.Done():
								return ctx.Err()
							}
						}
						return nil
					}}, nil
				}, WithMaxActors(limits.maxActors), WithMaxStarting(limits.maxStarting))
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				results := make(chan error, requests)
				start := make(chan struct{})
				for index := range requests {
					go func() {
						<-start
						_, err := system.Spawn(ctx, Key{Type: "player", Id: fmt.Sprint(index)})
						results <- err
					}()
				}
				close(start)
				for range limits.accepted {
					select {
					case <-entered:
					case <-ctx.Done():
						t.Fatal("未启动到预期数量", ctx.Err())
					}
				}
				for range requests - limits.accepted {
					select {
					case err := <-results:
						if !errors.Is(err, ErrCapacity) {
							t.Fatalf("超额请求未立即拒绝: %v", err)
						}
					case <-ctx.Done():
						t.Fatal("超额请求排队等待了名额", ctx.Err())
					}
				}
				system.mutex.Lock()
				count := len(system.actors)
				system.mutex.Unlock()
				if count != limits.accepted || loads.Load() != int32(limits.accepted) || peak.Load() != int32(limits.accepted) {
					t.Fatalf("actors=%d loads=%d peak=%d", count, loads.Load(), peak.Load())
				}
				unblock()
				for range limits.accepted {
					if err := <-results; err != nil {
						t.Fatal(err)
					}
				}
				if active.Load() != 0 {
					t.Fatal("启动回调未全部结束")
				}
				// 启动完成后可继续填满总名额，已运行的 Actor 不占启动名额
				for index := range limits.maxActors - limits.accepted {
					if _, err := system.Spawn(ctx, Key{Type: "player", Id: fmt.Sprint(requests + index)}); err != nil {
						t.Fatal("启动名额未释放", err)
					}
				}
				if _, err := system.Spawn(ctx, Key{Type: "player", Id: "overflow"}); !errors.Is(err, ErrCapacity) {
					t.Fatal("运行中的实例未计入总上限", err)
				}
			})
		}
	}
}

func TestActivationLimitsSharedAndCancelledWaiter(t *testing.T) {
	_, client := loaderRedis(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var loads atomic.Int32
	system := loaderSystem(t, NewGuard("node", client), func(ctx context.Context, key Key) (Actor, error) {
		if loads.Add(1) == 1 {
			close(entered)
			select {
			case <-resume:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return &loadedActor{}, nil
	}, WithMaxActors(1), WithMaxStarting(1))
	key := Key{Type: "player", Id: "shared"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	first := make(chan error, 1)
	go func() { _, err := system.Spawn(ctx, key); first <- err }()
	waitLoader(t, entered)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := system.Spawn(context.Background(), Key{Type: "player", Id: "other"}); !errors.Is(err, ErrCapacity) {
		t.Fatal("取消等待提前释放了名额", err)
	}
	const requests = 64
	results := make(chan Handle, requests)
	var wait sync.WaitGroup
	for range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			handle, err := system.Spawn(context.Background(), key)
			if err != nil {
				t.Error("满额时未复用同 Key", err)
				return
			}
			results <- handle
		}()
	}
	unblock()
	wait.Wait()
	close(results)
	var shared Handle
	for handle := range results {
		if shared != nil && shared != handle {
			t.Fatal("同 Key 返回不同实例")
		}
		shared = handle
	}
	if shared == nil || loads.Load() != 1 {
		t.Fatalf("handle=%v loads=%d", shared, loads.Load())
	}
	if err := system.Stop(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := system.Spawn(context.Background(), Key{Type: "player", Id: "other"}); err != nil {
		t.Fatal("停止后名额未释放", err)
	}
}

func TestActivationLimitsReleaseOnFailure(t *testing.T) {
	for _, phase := range []string{"guard", "owned", "loader", "on-start"} {
		t.Run(phase, func(t *testing.T) {
			_, client := loaderRedis(t)
			failed := errors.New("激活失败")
			key := Key{Type: "player", Id: "failed"}
			if phase == "guard" {
				var fail atomic.Bool
				client.AddHook(commandHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
					if (cmd.Name() == "eval" || cmd.Name() == "evalsha") && !fail.Swap(true) {
						return failed
					}
					return next(ctx, cmd)
				}))
			}
			if phase == "owned" {
				other := loaderSystem(t, NewGuard("other", client), func(context.Context, Key) (Actor, error) { return &loadedActor{}, nil })
				if _, err := other.Spawn(context.Background(), key); err != nil {
					t.Fatal(err)
				}
			}
			system := loaderSystem(t, NewGuard("node", client), func(ctx context.Context, current Key) (Actor, error) {
				if current == key && phase == "loader" {
					return nil, failed
				}
				return &loadedActor{onStart: func(StartContext) error {
					if current == key && phase == "on-start" {
						return failed
					}
					return nil
				}}, nil
			}, WithMaxActors(1), WithMaxStarting(1))
			_, err := system.Spawn(context.Background(), key)
			if phase == "owned" {
				if _, ok := errors.AsType[*OwnedError](err); !ok {
					t.Fatal(err)
				}
			} else if !errors.Is(err, failed) {
				t.Fatal(err)
			}
			if _, err := system.Spawn(context.Background(), Key{Type: "player", Id: "next"}); err != nil {
				t.Fatal("失败后名额未归还", err)
			}
		})
	}
}

func TestActivationLimitsHeldDuringStop(t *testing.T) {
	_, client := loaderRedis(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	system := loaderSystem(t, NewGuard("node", client), func(_ context.Context, key Key) (Actor, error) {
		return &unloadActor{onStop: func(StopContext) {
			if key.Id == "stopping" {
				close(entered)
				<-resume
			}
		}}, nil
	}, WithMaxActors(1), WithMaxStarting(1))
	key := Key{Type: "player", Id: "stopping"}
	handle, err := system.Spawn(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	handle.Stop()
	waitLoader(t, entered)
	if _, err := system.Spawn(context.Background(), Key{Type: "player", Id: "next"}); !errors.Is(err, ErrCapacity) {
		t.Fatal("卸载完成前归还了名额", err)
	}
	if _, err := system.Spawn(context.Background(), key); !errors.Is(err, ErrStopping) {
		t.Fatal("卸载中的同 Key 未返回 ErrStopping", err)
	}
	unblock()
	if err := system.Stop(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := system.Spawn(context.Background(), Key{Type: "player", Id: "next"}); err != nil {
		t.Fatal("卸载完成后名额未归还", err)
	}
}
