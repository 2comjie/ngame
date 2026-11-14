package actor

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type commandHook func(context.Context, redis.Cmder, redis.ProcessHook) error

func (h commandHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h commandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}
func (h commandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, command redis.Cmder) error { return h(ctx, command, next) }
}

func assertPanic(t *testing.T, run func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Error("应当因无效配置 panic")
		}
	}()
	run()
}

func TestGuardValidationAndCorruptOwners(t *testing.T) {
	server, client := loaderRedis(t)
	for _, run := range []func(){
		func() { NewGuard("", client) },
		func() { NewGuard("node", nil) },
		func() { NewGuard("node", client, WithGuardPrefix("")) },
		func() { NewGuard("node", client, WithGuardTTL(time.Millisecond)) },
		func() { NewGuard("node", client, WithGuardTTL(time.Millisecond+1)) },
	} {
		assertPanic(t, run)
	}
	guard := NewGuard("node", client, WithGuardPrefix("test:guard:"))
	ctx := context.Background()
	key := Key{Type: "room", Id: "1"}
	if _, _, _, err := guard.TryAcquire(ctx, Key{}, "a"); !errors.Is(err, ErrInvalidKey) {
		t.Fatal(err)
	}
	if _, _, err := guard.Owner(ctx, Key{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatal(err)
	}
	assertPanic(t, func() { _, _, _, _ = guard.TryAcquire(ctx, key, "") })
	for _, value := range []string{"invalid-json", `{}`, `{"node_id":"node"}`} {
		server.Set(guard.options.prefix+routeKey(key), value)
		if _, _, _, err := guard.TryAcquire(ctx, key, "a"); err == nil {
			t.Fatal("接受损坏的归属")
		}
		if _, _, err := guard.Owner(ctx, key); err == nil {
			t.Fatal("读取损坏归属没有报错")
		}
	}
	_ = client.Close()
	if _, _, _, err := guard.TryAcquire(ctx, key, "a"); err == nil {
		t.Fatal("关闭的 Redis 被当成成功")
	}
	if _, _, err := guard.Owner(ctx, key); err == nil {
		t.Fatal("关闭的 Redis 被当成成功")
	}
	owned := &OwnedError{Key: key, Owner: Owner{NodeId: "node", ActivationId: "a"}}
	if !strings.Contains(owned.Error(), "room/1") {
		t.Fatal(owned)
	}
}

func TestGuardDelayedAcquireReplyIsRejected(t *testing.T) {
	_, client := loaderRedis(t)
	client.AddHook(commandHook(func(ctx context.Context, command redis.Cmder, next redis.ProcessHook) error {
		err := next(ctx, command)
		if err == nil && (command.Name() == "evalsha" || command.Name() == "eval") {
			time.Sleep(120 * time.Millisecond)
		}
		return err
	}))
	guard := NewGuard("node", client, WithGuardTTL(90*time.Millisecond))
	if lease, _, _, err := guard.TryAcquire(context.Background(), Key{Type: "room", Id: "1"}, "a"); !errors.Is(err, ErrGuardLost) || lease != nil {
		t.Fatalf("已过期的抢占应拒绝: lease=%v error=%v", lease, err)
	}
}

func TestOldLeaseCannotReleaseOrRenewNewOwner(t *testing.T) {
	server, client := loaderRedis(t)
	ctx := context.Background()
	key := Key{Type: "room", Id: "1"}
	old, _, acquired, err := NewGuard("old", client).TryAcquire(ctx, key, "old-activation")
	if err != nil || !acquired {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = old.Release(ctx) })
	server.FastForward(time.Minute)
	guard := NewGuard("new", client)
	current, expected, acquired, err := guard.TryAcquire(ctx, key, "new-activation")
	if err != nil || !acquired {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = current.Release(ctx) })
	if renewed, err := guardRenew.Run(ctx, client, []string{old.key}, old.value, old.guard.options.ttl.Milliseconds()).Int(); err != nil || renewed != 0 {
		t.Fatalf("旧租约续租了新记录: renewed=%d error=%v", renewed, err)
	}
	if err := old.Release(ctx); !errors.Is(err, ErrGuardLost) {
		t.Fatalf("旧租约删除了新归属: %v", err)
	}
	owner, found, err := guard.Owner(ctx, key)
	if err != nil || !found || owner != expected {
		t.Fatalf("新归属被旧实例破坏: owner=%v found=%v error=%v", owner, found, err)
	}
}

func TestLeaseRenewalAndFailure(t *testing.T) {
	for _, mode := range []string{"renew", "redis-failure", "local-expired"} {
		t.Run(mode, func(t *testing.T) {
			_, client := loaderRedis(t)
			var renewals atomic.Int32
			client.AddHook(commandHook(func(ctx context.Context, command redis.Cmder, next redis.ProcessHook) error {
				if command.Name() == "evalsha" && command.Args()[1] == guardRenew.Hash() {
					renewals.Add(1)
				}
				return next(ctx, command)
			}))
			lease, _, ok, err := NewGuard("node", client, WithGuardTTL(90*time.Millisecond)).TryAcquire(context.Background(), Key{Type: "room", Id: "1"}, "a")
			if err != nil || !ok {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lease.Release(context.Background()) })
			if mode == "redis-failure" {
				_ = client.Close()
				waitLoader(t, lease.Done())
				if err := lease.Release(context.Background()); err == nil {
					t.Fatal("释放失败未返回错误")
				}
			} else if mode == "local-expired" {
				lease.expiry.Stop()
				lease.mu.Lock()
				lease.deadline = time.Now().Add(-time.Second)
				lease.mu.Unlock()
				waitLoader(t, lease.Done())
				if lease.Active() {
					t.Fatal("本地过期后仍有效")
				}
			} else {
				deadline := time.NewTimer(time.Second)
				defer deadline.Stop()
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for renewals.Load() < 2 {
					select {
					case <-ticker.C:
					case <-deadline.C:
						t.Fatal("未续租")
					}
				}
				if !lease.Active() {
					t.Fatal("续租后失效")
				}
				if err := lease.Release(context.Background()); err != nil {
					t.Fatal(err)
				}
				waitLoader(t, lease.Done())
			}
		})
	}
}
