package actor

import (
	"context"
	"errors"
	"hash/maphash"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type ownerReadHook struct {
	reads   atomic.Int32
	entered chan struct{}
	resume  <-chan struct{}
	once    sync.Once
}

func (*ownerReadHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (*ownerReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h *ownerReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error {
		if command.Name() == "get" {
			h.reads.Add(1)
			if h.resume != nil {
				h.once.Do(func() { close(h.entered) })
				select {
				case <-h.resume:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return next(ctx, command)
	}
}

func TestLocatorCachesAndCoalescesRefresh(t *testing.T) {
	_, client := loaderRedis(t)
	hook := &ownerReadHook{}
	client.AddHook(hook)
	ctx := context.Background()
	key := Key{Type: "room/with/slash", Id: "1/2"}
	guardA := NewGuard("node-a", client)
	leaseA, ownerA, acquired, err := guardA.TryAcquire(ctx, key, "activation-a")
	if err != nil || !acquired {
		t.Fatal("抢占初始归属失败", err)
	}
	t.Cleanup(func() { _ = leaseA.Release(ctx) })
	locator := NewLocator(client)
	t.Cleanup(locator.Close)
	run := func(previous *Owner, expected Owner) {
		t.Helper()
		var wait sync.WaitGroup
		start := make(chan struct{})
		for range 128 {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				var owner Owner
				var found bool
				var err error
				if previous == nil {
					owner, found, err = locator.Locate(ctx, key)
				} else {
					owner, found, err = locator.Refresh(ctx, key, *previous)
				}
				if err != nil || !found || owner != expected {
					t.Errorf("路由查询错误: owner=%+v found=%v error=%v", owner, found, err)
				}
			}()
		}
		close(start)
		wait.Wait()
	}
	run(nil, ownerA)
	if hook.reads.Load() != 1 {
		t.Fatalf("并发 miss 没有合并: GET=%d", hook.reads.Load())
	}
	run(nil, ownerA)
	if hook.reads.Load() != 1 {
		t.Fatalf("缓存命中仍访问 Redis: GET=%d", hook.reads.Load())
	}
	if err := leaseA.Release(ctx); err != nil {
		t.Fatal(err)
	}
	leaseB, ownerB, acquired, err := NewGuard("node-b", client).TryAcquire(ctx, key, "activation-b")
	if err != nil || !acquired {
		t.Fatal("换绑失败", err)
	}
	t.Cleanup(func() { _ = leaseB.Release(ctx) })
	run(&ownerA, ownerB)
	if hook.reads.Load() != 2 {
		t.Fatalf("并发刷新重复回源: GET=%d", hook.reads.Load())
	}
	run(&ownerA, ownerB)
	if hook.reads.Load() != 2 {
		t.Fatalf("已刷新过的路由再次回源: GET=%d", hook.reads.Load())
	}
	if err := leaseB.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if _, found, err := locator.Refresh(ctx, key, ownerB); err != nil || found {
		t.Fatalf("已删除的权威路由未清理缓存: found=%v error=%v", found, err)
	}
	leaseC, ownerC, acquired, err := guardA.TryAcquire(ctx, key, "activation-c")
	if err != nil || !acquired {
		t.Fatal("重新激活失败", err)
	}
	t.Cleanup(func() { _ = leaseC.Release(ctx) })
	if owner, found, err := locator.Locate(ctx, key); err != nil || !found || owner != ownerC {
		t.Fatalf("不存在结果被缓存: owner=%+v found=%v error=%v", owner, found, err)
	}
}

func TestLocatorWaiterCancellationDoesNotCancelSharedQuery(t *testing.T) {
	_, client := loaderRedis(t)
	key := Key{Type: "room", Id: "1"}
	lease, owner, _, err := NewGuard("node-a", client).TryAcquire(context.Background(), key, "activation-a")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	hook := &ownerReadHook{entered: make(chan struct{}), resume: resume}
	client.AddHook(hook)
	locator := NewLocator(client, WithLocatorTimeout(time.Second))
	t.Cleanup(locator.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, _, err := locator.Locate(ctx, key)
		result <- err
	}()
	waitLoader(t, hook.entered)
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("等待者未取消: %v", err)
	}
	unblock()
	if got, found, err := locator.Locate(context.Background(), key); err != nil || !found || got != owner {
		t.Fatalf("等待者取消了共享回源: owner=%+v found=%v error=%v", got, found, err)
	}
	if hook.reads.Load() != 1 {
		t.Fatalf("共享查询被重建: GET=%d", hook.reads.Load())
	}
}

func TestLocatorConfigurationAndReadFailures(t *testing.T) {
	server, client := loaderRedis(t)
	for _, run := range []func(){
		func() { NewLocator(nil) },
		func() { NewLocator(client, WithLocatorPrefix("")) },
		func() { NewLocator(client, WithLocatorCacheSize(0)) },
		func() { NewLocator(client, WithLocatorCacheSize(int(^uint(0)>>1))) },
		func() { NewLocator(client, WithLocatorCacheTTL(0)) },
		func() { NewLocator(client, WithLocatorTimeout(0)) },
	} {
		assertPanic(t, run)
	}
	locator := NewLocator(client, WithLocatorPrefix("test:locator:"), WithLocatorCacheSize(32), WithLocatorCacheTTL(time.Minute))
	t.Cleanup(locator.Close)
	ctx := context.Background()
	key := Key{Type: "room", Id: "1"}
	if _, _, err := locator.Locate(ctx, Key{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatal(err)
	}
	server.Set("test:locator:"+routeKey(key), "corrupt")
	if _, _, err := locator.Locate(ctx, key); err == nil {
		t.Fatal("损坏数据没有返回错误")
	}
}

func TestLocatorCloseCancelsPendingLookup(t *testing.T) {
	_, client := loaderRedis(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	client.AddHook(commandHook(func(ctx context.Context, cmd redis.Cmder, next redis.ProcessHook) error {
		if cmd.Name() == "get" {
			close(entered)
			<-resume
		}
		return next(ctx, cmd)
	}))
	locator := NewLocator(client)
	result := make(chan error, 1)
	go func() { _, _, err := locator.Locate(context.Background(), Key{Type: "room", Id: "1"}); result <- err }()
	waitLoader(t, entered)
	closed := make(chan struct{})
	go func() { locator.Close(); close(closed) }()
	if err := <-result; !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	unblock()
	waitLoader(t, closed)
}

func TestLocatorUsesCacheFilledWhileWaitingForLock(t *testing.T) {
	_, client := loaderRedis(t)
	locator := NewLocator(client)
	t.Cleanup(locator.Close)
	key := Key{Type: "room", Id: "1"}
	suffix := routeKey(key)
	lock := &locator.locks[maphash.String(locator.seed, suffix)%uint64(len(locator.locks))]
	lock.Lock()
	result := make(chan error, 1)
	waiting := &loopWaitContext{Context: context.Background(), ready: make(chan struct{})}
	go func() { _, _, err := locator.Locate(waiting, key); result <- err }()
	waitLoader(t, waiting.ready)
	owner := Owner{NodeId: "node", ActivationId: "a"}
	locator.cache.SetWithTTL(suffix, owner, 1, time.Minute)
	locator.cache.Wait()
	lock.Unlock()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}
