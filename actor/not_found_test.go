package actor

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

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
