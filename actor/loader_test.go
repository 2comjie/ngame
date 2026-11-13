package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
