package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/2comjie/ngame/app"
)

func reporterSystem(t *testing.T, loader Loader, opts ...Option) *System {
	t.Helper()
	_, redisClient := loaderRedis(t)
	system := New(NewGuard("node", redisClient), opts...)
	system.Register("player", loader)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := system.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return system
}

func managedReporter(t *testing.T, system *System, report func(context.Context, map[string]string) error, opts ...ReporterOption) *Reporter {
	t.Helper()
	options := []ReporterOption{WithResourceCapacity(1024, 4<<30)}
	options = append(options, opts...)
	r := NewReporter(system, report, options...)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := r.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return r
}

func TestReporterLifecycle(t *testing.T) {
	system := reporterSystem(t, func(context.Context, Key) (Actor, error) { return &loadedActor{}, nil }, WithLifecycleTimeout(time.Nanosecond))
	updates := make(chan NodeLoad, 64)
	var calls atomic.Int32
	r := managedReporter(t, system, func(ctx context.Context, meta map[string]string) error {
		calls.Add(1)
		var load NodeLoad
		if err := json.Unmarshal([]byte(meta[NodeLoadMetaKey]), &load); err != nil {
			return err
		}
		if current := system.Stats(); current.ActorLimit != load.ActorLimit {
			t.Error("上报与准入的上限不一致")
		}
		select {
		case updates <- load:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, WithLoadInterval(5*time.Millisecond))
	if err := system.Start(); err != nil {
		t.Fatal(err)
	}
	if _, err := system.Spawn(context.Background(), Key{Type: "player", Id: "before-sample"}); !errors.Is(err, ErrCapacity) {
		t.Fatal("没有资源采样时仍允许激活", err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); !errors.Is(err, app.ErrStarted) {
		t.Fatal(err)
	}
	first := <-updates
	if first.ActorCount != 0 || first.ActorLimit != 10000 || first.MemoryUsage <= 0 || first.CPUUsage < 0 || first.SampledAt.IsZero() {
		t.Fatal("初始负载无效", first)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	select {
	case load := <-updates:
		if !load.SampledAt.After(first.SampledAt) || math.IsNaN(load.CPUUsage) || load.MemoryUsage <= 0 {
			t.Fatal("周期负载无效", load)
		}
	case <-ctx.Done():
		t.Fatal("没有周期上报", ctx.Err())
	}
	if err := r.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); !errors.Is(err, app.ErrClosed) {
		t.Fatal(err)
	}
	count := calls.Load()
	select {
	case <-time.After(20 * time.Millisecond):
		if calls.Load() != count {
			t.Fatal("Shutdown 后仍在上报")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if system.ctx.Err() != nil {
		t.Fatal("停止 Reporter 同时停止了 System")
	}
}

func TestReporterErrorsDoNotStopSystem(t *testing.T) {
	for _, mode := range []string{"initial", "periodic", "timeout"} {
		t.Run(mode, func(t *testing.T) {
			system := reporterSystem(t, func(context.Context, Key) (Actor, error) { return &loadedActor{}, nil })
			failed := errors.New("测试上报失败")
			called := make(chan struct{}, 1)
			var calls atomic.Int32
			r := managedReporter(t, system, func(ctx context.Context, meta map[string]string) error {
				count := calls.Add(1)
				if mode == "timeout" {
					<-ctx.Done()
					return ctx.Err()
				}
				if mode == "initial" || count == 2 {
					called <- struct{}{}
					return failed
				}
				return nil
			}, WithLoadInterval(5*time.Millisecond), WithReportTimeout(50*time.Millisecond))
			if err := system.Start(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := r.Start()
			switch mode {
			case "initial":
				if !errors.Is(err, failed) {
					t.Fatal(err)
				}
			case "timeout":
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
			case "periodic":
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-called:
					if !r.admit() {
						t.Fatal("发布失败丢弃了有效的本地采样")
					}
				case <-ctx.Done():
					t.Fatal("没有周期上报", ctx.Err())
				}
			}
			if system.ctx.Err() != nil {
				t.Fatal("上报失败停止了 System")
			}
			if err := r.Shutdown(ctx); err != nil {
				t.Fatal("上报失败后无法清理", err)
			}
		})
	}
}

func TestReporterStopDuringStart(t *testing.T) {
	system := reporterSystem(t, func(context.Context, Key) (Actor, error) { return &loadedActor{}, nil })
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	r := managedReporter(t, system, func(context.Context, map[string]string) error {
		close(entered)
		<-resume
		return nil
	})
	if err := system.Start(); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- r.Start() }()
	waitLoader(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("提前释放了仍在初始化的 Reporter", err)
	}
	unblock()
	if err := <-result; !errors.Is(err, app.ErrClosed) {
		t.Fatal(err)
	}
	if err := r.Shutdown(context.Background()); err != nil || r.admit() {
		t.Fatal("启动期间停止后仍允许准入", err)
	}
}

func TestReporterOptions(t *testing.T) {
	_, redisClient := loaderRedis(t)
	guard := NewGuard("node", redisClient)
	report := func(context.Context, map[string]string) error { return nil }
	for _, run := range []func(){
		func() { NewReporter(nil, report) },
		func() { NewReporter(New(guard), nil) },
		func() { NewReporter(New(guard), report) },
		func() { NewReporter(New(guard), report, WithResourceCapacity(0, 1)) },
		func() { NewReporter(New(guard), report, WithResourceCapacity(math.NaN(), 1)) },
		func() { NewReporter(New(guard), report, WithResourceCapacity(math.Inf(1), 1)) },
		func() { NewReporter(New(guard), report, WithResourceCapacity(1, 1), WithLoadInterval(0)) },
		func() { NewReporter(New(guard), report, WithResourceCapacity(1, 1), WithReportTimeout(0)) },
		func() { NewReporter(New(guard), report, WithResourceCapacity(1, 1), WithResourceMaxAge(0)) },
	} {
		assertPanic(t, run)
	}
	system := New(guard)
	r := managedReporter(t, system, report)
	assertPanic(t, func() { NewReporter(system, report, WithResourceCapacity(1, 1)) })
	if err := r.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); !errors.Is(err, app.ErrClosed) {
		t.Fatal(err)
	}
	started := New(guard)
	if err := started.Start(); err != nil {
		t.Fatal(err)
	}
	assertPanic(t, func() { NewReporter(started, report, WithResourceCapacity(1, 1)) })
	if err := started.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestServerResourceAdmission(t *testing.T) {
	for _, mode := range []string{"unknown", "cpu", "memory", "expired", "lost-sample", "stopped", "healthy"} {
		t.Run(mode, func(t *testing.T) {
			var loads atomic.Int32
			system := reporterSystem(t, func(context.Context, Key) (Actor, error) {
				loads.Add(1)
				return &loadedActor{}, nil
			})
			r := managedReporter(t, system, func(context.Context, map[string]string) error { return nil }, WithLoadInterval(time.Hour))
			if err := system.Start(); err != nil {
				t.Fatal(err)
			}
			if mode != "unknown" {
				if err := r.Start(); err != nil {
					t.Fatal(err)
				}
				load := &NodeLoad{CPUUsage: 0.2, MemoryUsage: 0.2, SampledAt: time.Now()}
				switch mode {
				case "cpu":
					load.CPUUsage = 1
				case "memory":
					load.MemoryUsage = 1
				case "expired":
					load.SampledAt = time.Now().Add(-time.Hour)
				}
				r.latest.Store(load)
				if mode == "lost-sample" {
					r.latest.Store(nil)
				}
				if mode == "stopped" {
					r.RequestStop()
				}
			}
			_, err := system.Spawn(context.Background(), Key{Type: "player", Id: "1"})
			if mode == "healthy" {
				if err != nil || loads.Load() != 1 {
					t.Fatal("有效资源未允许激活", err)
				}
			} else if !errors.Is(err, ErrCapacity) || loads.Load() != 0 || system.Stats().ActorCount != 0 {
				t.Fatalf("资源检查未在加载前拒绝: loads=%d error=%v", loads.Load(), err)
			}
		})
	}
}

func TestServerMemoryAdmissionUsesRSS(t *testing.T) {
	var loads atomic.Int32
	system := reporterSystem(t, func(context.Context, Key) (Actor, error) { loads.Add(1); return &loadedActor{}, nil })
	r := managedReporter(t, system, func(context.Context, map[string]string) error { return nil }, WithResourceCapacity(1024, 1))
	if err := system.Start(); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	if load := r.latest.Load(); load.MemoryUsage <= 1 {
		t.Fatal("没有使用实际进程 RSS", load)
	}
	if _, err := system.Spawn(context.Background(), Key{Type: "player", Id: "1"}); !errors.Is(err, ErrCapacity) || loads.Load() != 0 {
		t.Fatal("超过内存额度仍执行了加载", err)
	}
}

func TestResourceSampleFailureClosesAdmission(t *testing.T) {
	system := reporterSystem(t, func(context.Context, Key) (Actor, error) { return &loadedActor{}, nil })
	r := managedReporter(t, system, func(context.Context, map[string]string) error { return nil }, WithLoadInterval(time.Hour))
	if err := system.Start(); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	pid := r.process.Pid
	r.process.Pid = -1
	if err := r.publish(); err == nil || r.latest.Load() != nil || r.admit() {
		t.Fatal("采样失败后仍使用旧数据准入", err)
	}
	if _, err := system.Spawn(context.Background(), Key{Type: "player", Id: "unknown"}); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	r.process.Pid = pid
	if err := r.publish(); err != nil || !r.admit() {
		t.Fatal("有效采样恢复后仍拒绝准入", err)
	}
}

func TestResourcePressureChangeDuringConcurrentActivation(t *testing.T) {
	const requests = 128
	entered := make(chan struct{}, requests)
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var active, loads atomic.Int32
	system := reporterSystem(t, func(ctx context.Context, key Key) (Actor, error) {
		loads.Add(1)
		if current := active.Add(1); current > 4 {
			t.Error("突破启动并发", current)
		}
		defer active.Add(-1)
		entered <- struct{}{}
		select {
		case <-resume:
			return &loadedActor{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}, WithMaxActors(32), WithMaxStarting(4))
	r := managedReporter(t, system, func(context.Context, map[string]string) error { return nil }, WithLoadInterval(time.Hour))
	if err := system.Start(); err != nil {
		t.Fatal(err)
	}
	if err := r.Start(); err != nil {
		t.Fatal(err)
	}
	r.latest.Store(&NodeLoad{CPUUsage: 0.1, MemoryUsage: 0.1, SampledAt: time.Now()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	for range 4 {
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for range requests - 4 {
		if err := <-results; !errors.Is(err, ErrCapacity) {
			t.Fatal(err)
		}
	}
	r.latest.Store(&NodeLoad{CPUUsage: 0.1, MemoryUsage: 1, SampledAt: time.Now()})
	unblock()
	for range 4 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	var wait sync.WaitGroup
	for index := range requests {
		wait.Add(1)
		go func() {
			defer wait.Done()
			_, err := system.Spawn(ctx, Key{Type: "player", Id: fmt.Sprintf("pressure-%d", index)})
			if !errors.Is(err, ErrCapacity) {
				t.Error("内存超限后仍放行", err)
			}
		}()
	}
	wait.Wait()
	if loads.Load() != 4 || system.Stats().ActorCount != 4 {
		t.Fatal("资源超限时执行了新加载", loads.Load())
	}
	r.latest.Store(&NodeLoad{CPUUsage: 0.1, MemoryUsage: 0.1, SampledAt: time.Now()})
	if _, err := system.Spawn(ctx, Key{Type: "player", Id: "recovered"}); err != nil {
		t.Fatal("资源恢复后未放行", err)
	}
}

func TestRPCResourceAdmissionWithStaleClientLoad(t *testing.T) {
	for _, resource := range []string{"cpu", "memory"} {
		t.Run(resource, func(t *testing.T) {
			_, redisClient := loaderRedis(t)
			var loads atomic.Int32
			system := New(NewGuard("node", redisClient), WithMaxActors(16), WithMaxStarting(4))
			system.Register("player", func(context.Context, Key) (Actor, error) {
				loads.Add(1)
				return &loadedActor{}, nil
			})
			r := managedReporter(t, system, func(context.Context, map[string]string) error { return nil }, WithLoadInterval(time.Hour))
			node := startActorRPCNode(t, system, "game")
			if err := r.Start(); err != nil {
				t.Fatal(err)
			}
			r.latest.Store(&NodeLoad{CPUUsage: 0.1, MemoryUsage: 0.1, SampledAt: time.Now()})
			key := Key{Type: "player", Id: "existing"}
			handle, err := system.Spawn(context.Background(), key)
			if err != nil {
				t.Fatal(err)
			}
			load := &NodeLoad{CPUUsage: 0.1, MemoryUsage: 0.1, SampledAt: time.Now()}
			if resource == "cpu" {
				load.CPUUsage = 1
			} else {
				load.MemoryUsage = 1
			}
			r.latest.Store(load)
			// 调用方仍持有旧的空闲快照，本节点已观察到资源超限
			node.instance.Meta = placementInstance(t, node.instance.Id, NodeLoad{ActorLimit: 16, SampledAt: time.Now()}).Meta
			discover := actorDiscover(t, node)
			callers := make([]*Client, 4)
			for index := range callers {
				callers[index] = actorClient(t, redisClient, discover, map[string]string{"player": "game"})
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var wait sync.WaitGroup
			start := make(chan struct{})
			for index := range 256 {
				wait.Add(1)
				go func() {
					defer wait.Done()
					<-start
					caller := callers[index%len(callers)]
					if index%2 == 0 {
						owner, err := caller.activate(ctx, key, Owner{})
						if err != nil || owner.ActivationId != handle.ActivationId() {
							t.Errorf("资源超限影响了已有实例: owner=%+v error=%v", owner, err)
						}
					} else {
						_, err := caller.activate(ctx, Key{Type: "player", Id: fmt.Sprintf("new-%d", index)}, Owner{})
						if !errors.Is(err, ErrCapacity) {
							t.Error("旧快照绕过了资源准入", err)
						}
					}
				}()
			}
			close(start)
			wait.Wait()
			if loads.Load() != 1 || system.Stats().ActorCount != 1 {
				t.Fatal("资源超限期间产生了新实例", loads.Load())
			}
		})
	}
}

func TestActivationWithoutCandidateIsNotCapacity(t *testing.T) {
	_, redisClient := loaderRedis(t)
	caller := actorClient(t, redisClient, actorDiscover(t), map[string]string{"player": "game"})
	_, err := caller.activate(context.Background(), Key{Type: "player", Id: "1"}, Owner{})
	if !errors.Is(err, ErrNoAvailableNode) || errors.Is(err, ErrCapacity) {
		t.Fatal("无候选节点被误报为容量不足", err)
	}
}
