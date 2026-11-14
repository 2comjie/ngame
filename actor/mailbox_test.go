package actor

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 本地执行测试直接覆盖内部队列，业务调用只通过 Ref 暴露
func localAsk(s *System, ctx context.Context, key Key, message Message) ([]byte, error) {
	reply := make(chan messageResult, 1)
	r, err := s.enqueue(ctx, key, message, reply, "")
	if err != nil {
		return nil, err
	}
	return r.waitResult(ctx, reply)
}

func localTell(s *System, ctx context.Context, key Key, message Message) error {
	_, err := s.enqueue(ctx, key, message, nil, "")
	return err
}

type messageActor struct {
	loadedActor
	onMessage func(MessageContext, Message) ([]byte, error)
	onUpdate  func(UpdateContext) time.Duration
}

func (a *messageActor) OnMessage(ctx MessageContext, message Message) ([]byte, error) {
	return a.onMessage(ctx, message)
}

func (a *messageActor) OnUpdate(ctx UpdateContext) time.Duration {
	if a.onUpdate != nil {
		return a.onUpdate(ctx)
	}
	return -1
}

func spawnMessageActor(t *testing.T, value *messageActor, opts ...Option) (*System, Key, Handle) {
	t.Helper()
	_, client := loaderRedis(t)
	s := loaderSystem(t, NewGuard("node-a", client), func(context.Context, Key) (Actor, error) {
		return value, nil
	}, opts...)
	key := Key{Type: "player", Id: "1"}
	handle, err := s.Spawn(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	return s, key, handle
}

// 仅用于把停止操作精确安排在所有 Ask 入队之后
func waitMessageQueue(t *testing.T, handle Handle, count int) {
	t.Helper()
	timeout := time.NewTimer(3 * time.Second)
	defer timeout.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for len(handle.(*runner).mailbox) != count {
		select {
		case <-timeout.C:
			t.Fatal("等待消息入队超时")
		case <-ticker.C:
		}
	}
}

func TestMessagesSerialized(t *testing.T) {
	var active atomic.Bool
	enter := func() {
		if !active.CompareAndSwap(false, true) {
			t.Error("Actor 回调并发执行")
		}
		runtime.Gosched()
	}
	count := 0
	updated, timed := make(chan struct{}), make(chan struct{})
	onceUpdate, onceTimer := sync.Once{}, sync.Once{}
	businessErr := errors.New("余额不足")
	value := &messageActor{
		onMessage: func(ctx MessageContext, message Message) ([]byte, error) {
			enter()
			defer active.Store(false)
			switch message.Method {
			case "increment":
				count++
			case "get":
				return fmt.Appendf(nil, "%d", count), nil
			case "fail":
				return nil, businessErr
			}
			return nil, nil
		},
		onUpdate: func(UpdateContext) time.Duration {
			enter()
			defer active.Store(false)
			onceUpdate.Do(func() { close(updated) })
			return 0
		},
	}
	s, key, handle := spawnMessageActor(t, value, WithMailboxSize(512), WithUpdateInterval(time.Millisecond))
	timer, err := handle.Every(time.Millisecond, func(context.Context) {
		enter()
		defer active.Store(false)
		onceTimer.Do(func() { close(timed) })
	})
	if err != nil {
		t.Fatal(err)
	}
	defer timer.Cancel()
	if err := handle.RunOnMainLoop(func(context.Context) {
		enter()
		defer active.Store(false)
		count++
	}); err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for index := range 128 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			message := Message{Method: "increment"}
			var err error
			if index%2 == 0 {
				_, err = localAsk(s, context.Background(), key, message)
			} else {
				err = localTell(s, context.Background(), key, message)
			}
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wait.Wait()
	body, err := localAsk(s, context.Background(), key, Message{Method: "get"})
	if err != nil || string(body) != "129" {
		t.Fatalf("消息遗漏或重复执行: body=%s error=%v", body, err)
	}
	if _, err := localAsk(s, context.Background(), key, Message{Method: "fail"}); !errors.Is(err, businessErr) {
		t.Fatalf("Ask 未返回业务错误: %v", err)
	}
	waitLoader(t, updated)
	waitLoader(t, timed)
}

func TestMessageBufferOwnershipAndTellLifetime(t *testing.T) {
	var received string
	var buffer []byte
	s, key, handle := spawnMessageActor(t, &messageActor{
		onMessage: func(ctx MessageContext, message Message) ([]byte, error) {
			switch message.Method {
			case "store":
				if err := ctx.Err(); err != nil {
					t.Errorf("Tell 被投递方取消: %v", err)
				}
				received = string(message.Body)
			case "get":
				buffer = append(buffer[:0], received...)
				return buffer, nil
			case "overwrite":
				copy(buffer, "other")
			}
			return nil, nil
		},
	})
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	if err := handle.RunOnMainLoop(func(context.Context) { close(entered); <-resume }); err != nil {
		t.Fatal(err)
	}
	waitLoader(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	payload := []byte("hello")
	if err := localTell(s, ctx, key, Message{Method: "store", Body: payload}); err != nil {
		t.Fatal(err)
	}
	cancel()
	copy(payload, "wrong")
	unblock()
	body, err := localAsk(s, context.Background(), key, Message{Method: "get"})
	if err != nil || string(body) != "hello" {
		t.Fatalf("Tell 未复制消息或被取消: body=%s error=%v", body, err)
	}
	if _, err := localAsk(s, context.Background(), key, Message{Method: "overwrite"}); err != nil {
		t.Fatal(err)
	}
	if string(body) != "hello" {
		t.Fatalf("Ask 回包被后续消息修改: %s", body)
	}
}

func TestAskCancellation(t *testing.T) {
	t.Run("排队超时不执行业务", func(t *testing.T) {
		count := 0
		s, key, handle := spawnMessageActor(t, &messageActor{
			onMessage: func(ctx MessageContext, message Message) ([]byte, error) {
				if message.Method == "increment" {
					count++
				}
				return fmt.Appendf(nil, "%d", count), nil
			},
		})
		entered, resume := make(chan struct{}), make(chan struct{})
		unblock := sync.OnceFunc(func() { close(resume) })
		defer unblock()
		if err := handle.RunOnMainLoop(func(context.Context) { close(entered); <-resume }); err != nil {
			t.Fatal(err)
		}
		waitLoader(t, entered)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel()
		if _, err := localAsk(s, ctx, key, Message{Method: "increment"}); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("排队 Ask 未超时: %v", err)
		}
		unblock()
		body, err := localAsk(s, context.Background(), key, Message{Method: "get"})
		if err != nil || string(body) != "0" {
			t.Fatalf("已超时 Ask 仍执行: body=%s error=%v", body, err)
		}
	})
	t.Run("执行中的消息收到取消", func(t *testing.T) {
		entered, cancelled := make(chan struct{}), make(chan struct{})
		s, key, _ := spawnMessageActor(t, &messageActor{
			onMessage: func(ctx MessageContext, message Message) ([]byte, error) {
				if message.Method == "wait" {
					close(entered)
					<-ctx.Done()
					close(cancelled)
					return nil, ctx.Err()
				}
				return nil, nil
			},
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			_, err := localAsk(s, ctx, key, Message{Method: "wait"})
			result <- err
		}()
		waitLoader(t, entered)
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Ask 未取消: %v", err)
		}
		waitLoader(t, cancelled)
		if _, err := localAsk(s, context.Background(), key, Message{Method: "ok"}); err != nil {
			t.Fatalf("取消一个 Ask 导致 Actor 无法继续处理消息: %v", err)
		}
	})
}

func TestStopRejectsQueuedAsks(t *testing.T) {
	s, key, handle := spawnMessageActor(t, &messageActor{
		onMessage: func(MessageContext, Message) ([]byte, error) {
			t.Error("停止后执行了排队消息")
			return nil, nil
		},
	})
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	if err := handle.RunOnMainLoop(func(context.Context) { close(entered); <-resume }); err != nil {
		t.Fatal(err)
	}
	waitLoader(t, entered)
	results := make(chan error, 16)
	for range 16 {
		go func() {
			_, err := localAsk(s, context.Background(), key, Message{})
			results <- err
		}()
	}
	waitMessageQueue(t, handle, 16)
	handle.Stop()
	for range 16 {
		select {
		case err := <-results:
			if !errors.Is(err, ErrStopping) {
				t.Errorf("停止后的排队 Ask: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("排队 Ask 未及时结束")
		}
	}
	if err := localTell(s, context.Background(), key, Message{}); !errors.Is(err, ErrStopping) {
		t.Fatalf("停止后仍能投递: %v", err)
	}
	unblock()
	if err := s.Stop(context.Background(), key); err != nil {
		t.Fatal(err)
	}
}

func TestMessageCancelledWithActor(t *testing.T) {
	for _, reason := range []string{"shutdown", "lease-lost"} {
		t.Run(reason, func(t *testing.T) {
			server, client := loaderRedis(t)
			entered, cancelled, resume := make(chan struct{}), make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			s := loaderSystem(t, NewGuard("node-a", client, WithGuardTTL(300*time.Millisecond)), func(context.Context, Key) (Actor, error) {
				return &messageActor{
					onMessage: func(ctx MessageContext, message Message) ([]byte, error) {
						close(entered)
						<-ctx.Done()
						close(cancelled)
						<-resume
						return nil, ctx.Err()
					},
				}, nil
			})
			key := Key{Type: "player", Id: "1"}
			if _, err := s.Spawn(context.Background(), key); err != nil {
				t.Fatal(err)
			}
			result := make(chan error, 1)
			go func() {
				_, err := localAsk(s, context.Background(), key, Message{})
				result <- err
			}()
			waitLoader(t, entered)
			wantErr := ErrStopping
			if reason == "shutdown" {
				s.RequestStop()
			} else {
				server.FastForward(time.Second)
				wantErr = ErrGuardLost
			}
			waitLoader(t, cancelled)
			select {
			case err := <-result:
				if !errors.Is(err, wantErr) {
					t.Fatalf("Actor 取消未通知 Ask: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("Actor 取消后 Ask 仍在等待业务回调结束")
			}
		})
	}
}

func TestSelfAskRejectedAndSelfTellAllowed(t *testing.T) {
	var s *System
	var key Key
	count := 0
	value := &messageActor{
		onMessage: func(ctx MessageContext, message Message) ([]byte, error) {
			switch message.Method {
			case "ask-self":
				return localAsk(s, ctx, key, Message{Method: "increment"})
			case "tell-self":
				return nil, localTell(s, ctx, key, Message{Method: "increment"})
			case "increment":
				count++
			}
			return fmt.Appendf(nil, "%d", count), nil
		},
	}
	var handle Handle
	s, key, handle = spawnMessageActor(t, value)
	if _, err := localAsk(s, context.Background(), key, Message{Method: "ask-self"}); !errors.Is(err, ErrSelfAsk) {
		t.Fatalf("同步 Ask 自己未被拒绝: %v", err)
	}
	if _, err := localAsk(s, context.Background(), key, Message{Method: "tell-self"}); err != nil {
		t.Fatalf("Tell 自己失败: %v", err)
	}
	body, err := localAsk(s, context.Background(), key, Message{Method: "get"})
	if err != nil || string(body) != "1" {
		t.Fatalf("Tell 自己未被执行: body=%s error=%v", body, err)
	}
	result := make(chan error, 1)
	if err := handle.RunOnMainLoop(func(ctx context.Context) {
		_, err := localAsk(s, ctx, key, Message{})
		result <- err
	}); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrSelfAsk) {
		t.Fatalf("Task 中 Ask 自己未被拒绝: %v", err)
	}
}

func TestMessageAdmission(t *testing.T) {
	_, client := loaderRedis(t)
	s := New(NewGuard("node-a", client), WithMailboxSize(1))
	key := Key{Type: "player", Id: "1"}
	loads := 0
	s.Register("player", func(context.Context, Key) (Actor, error) {
		loads++
		return &loadedActor{}, nil
	})
	if err := localTell(s, context.Background(), key, Message{}); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("未启动的 System 允许投递: %v", err)
	}
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
	if _, err := localAsk(s, context.Background(), key, Message{}); !errors.Is(err, ErrNotFound) || loads != 0 {
		t.Fatalf("Ask 不应隐式激活: error=%v loads=%d", err, loads)
	}
	if err := localTell(s, context.Background(), Key{}, Message{}); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("无效 Key: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := localAsk(s, ctx, key, Message{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消 Ask: %v", err)
	}
	if err := localTell(s, ctx, key, Message{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消 Tell: %v", err)
	}
	handle, err := s.Spawn(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	if err := handle.RunOnMainLoop(func(context.Context) { close(entered); <-resume }); err != nil {
		t.Fatal(err)
	}
	waitLoader(t, entered)
	if err := localTell(s, context.Background(), key, Message{}); err != nil {
		t.Fatal(err)
	}
	if _, err := localAsk(s, context.Background(), key, Message{}); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("Ask 满队列未被拒绝: %v", err)
	}
	if err := localTell(s, context.Background(), key, Message{}); !errors.Is(err, ErrMailboxFull) {
		t.Fatalf("Tell 满队列未被拒绝: %v", err)
	}
	s.RequestStop()
	if _, err := localAsk(s, context.Background(), key, Message{}); !errors.Is(err, ErrStopped) {
		t.Fatalf("System 停止后仍能调用: %v", err)
	}
}

func TestWaitResultOnMainLoopBoundaries(t *testing.T) {
	system, key, handle := spawnMessageActor(t, &messageActor{}, WithMailboxSize(1))
	assertPanic(t, func() { _ = handle.RunOnMainLoop(nil) })
	assertPanic(t, func() { _ = handle.WaitResultOnMainLoop(context.Background(), nil) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := handle.WaitResultOnMainLoop(ctx, func(context.Context) error { t.Error("取消后执行"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	want := errors.New("保存失败")
	if err := handle.WaitResultOnMainLoop(context.Background(), func(ctx context.Context) error {
		if err := handle.WaitResultOnMainLoop(ctx, func(context.Context) error { return nil }); !errors.Is(err, ErrSelfWait) {
			t.Error(err)
		}
		return want
	}); !errors.Is(err, want) {
		t.Fatal(err)
	}
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	if err := handle.RunOnMainLoop(func(context.Context) { close(entered); <-resume }); err != nil {
		t.Fatal(err)
	}
	waitLoader(t, entered)
	if err := handle.RunOnMainLoop(func(context.Context) {}); err != nil {
		t.Fatal(err)
	}
	if err := handle.RunOnMainLoop(func(context.Context) {}); !errors.Is(err, ErrMailboxFull) {
		t.Fatal(err)
	}
	if err := handle.WaitResultOnMainLoop(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrMailboxFull) {
		t.Fatal(err)
	}
	handle.Stop()
	if err := handle.RunOnMainLoop(func(context.Context) {}); !errors.Is(err, ErrStopping) {
		t.Fatal(err)
	}
	if err := handle.WaitResultOnMainLoop(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrStopping) {
		t.Fatal(err)
	}
	unblock()
	if err := system.Stop(context.Background(), key); err != nil {
		t.Fatal(err)
	}
}

func TestStartingActorRejectsMessagesAndWaitTasks(t *testing.T) {
	_, client := loaderRedis(t)
	entered := make(chan Handle, 1)
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	value := &loadedActor{onStart: func(ctx StartContext) error { entered <- ctx.Self; <-resume; return nil }}
	system := loaderSystem(t, NewGuard("node", client), func(context.Context, Key) (Actor, error) { return value, nil })
	key := Key{Type: "player", Id: "1"}
	ready := make(chan error, 1)
	go func() { _, err := system.Spawn(context.Background(), key); ready <- err }()
	handle := <-entered
	if _, err := localAsk(system, context.Background(), key, Message{}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := handle.WaitResultOnMainLoop(context.Background(), func(context.Context) error { return nil }); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	unblock()
	if err := <-ready; err != nil {
		t.Fatal(err)
	}
}

func TestTellBusinessFailureDoesNotStopActor(t *testing.T) {
	want := errors.New("业务失败")
	system, key, handle := spawnMessageActor(t, &messageActor{onMessage: func(MessageContext, Message) ([]byte, error) { return nil, want }})
	if err := localTell(system, context.Background(), key, Message{}); err != nil {
		t.Fatal(err)
	}
	if err := handle.WaitResultOnMainLoop(context.Background(), func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
}
