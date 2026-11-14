package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	actorpb "github.com/2comjie/ngame/actor/internal/protocol"
	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ngame/registry"
	"github.com/2comjie/ngame/rpc"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type actorRPCNode struct {
	system   *System
	instance endpoint.ServiceInst
	server   *grpc.Server
}

func newActorRPCNode(t *testing.T, client *redis.Client, id, service string, loaders map[string]Loader, options ...grpc.ServerOption) *actorRPCNode {
	t.Helper()
	system := New(NewGuard(id, client), WithMailboxSize(256))
	for name, loader := range loaders {
		system.Register(name, loader)
	}
	if err := system.Start(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := rpc.NewServer(id, options...)
	system.RegisterRPC(server)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Errorf("Actor RPC Serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		server.Stop()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := system.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return &actorRPCNode{
		system: system,
		server: server,
		instance: endpoint.ServiceInst{
			Id:      id,
			Name:    service,
			RpcHost: "127.0.0.1",
			RpcPort: listener.Addr().(*net.TCPAddr).Port,
			Weight:  1,
		},
	}
}

func actorDiscover(t *testing.T, nodes ...*actorRPCNode) registry.Discover {
	t.Helper()
	instances := make([]endpoint.ServiceInst, 0, len(nodes))
	for _, node := range nodes {
		instances = append(instances, node.instance)
	}
	discover := registry.NewDiscover(func(ctx context.Context, publish func([]endpoint.ServiceInst)) error {
		publish(instances)
		<-ctx.Done()
		return ctx.Err()
	})
	t.Cleanup(discover.Close)
	return discover
}

func actorClient(t *testing.T, client *redis.Client, discover registry.Discover, services map[string]string) *Client {
	t.Helper()
	locator := NewLocator(client)
	t.Cleanup(locator.Close)
	caller, err := NewClient(discover, locator, services,
		WithClientDialOptions(grpc.WithTransportCredentials(insecure.NewCredentials())),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	return caller
}

func TestRefServiceRoutingAndReactivation(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var loads atomic.Int32
	loader := func(nodeId string) Loader {
		return func(context.Context, Key) (Actor, error) {
			loads.Add(1)
			return &messageActor{
				onMessage: func(ctx MessageContext, message Message) ([]byte, error) {
					return []byte(nodeId + ":" + message.Method), nil
				},
			}, nil
		}
	}
	a := newActorRPCNode(t, redisClient, "game-a", "game", map[string]Loader{
		"room": loader("game-a"),
		"hall": loader("game-a"),
	})
	b := newActorRPCNode(t, redisClient, "game-b", "game", map[string]Loader{
		"room": loader("game-b"),
		"hall": loader("game-b"),
	})
	player := newActorRPCNode(t, redisClient, "player-a", "player", map[string]Loader{"player": loader("player-a")})
	caller := actorClient(t, redisClient, actorDiscover(t, a, b, player), map[string]string{
		"room":   "game",
		"hall":   "game",
		"player": "player",
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key := Key{Type: "room", Id: "1"}
	ref := caller.Ref(key)
	if ref.Key() != key || loads.Load() != 0 {
		t.Fatal("构造 Ref 触发了激活")
	}
	if _, err := ref.Ask(ctx, Message{}, ExistingOnly); !errors.Is(err, ErrNotFound) || loads.Load() != 0 {
		t.Fatalf("ExistingOnly 触发激活: loads=%d error=%v", loads.Load(), err)
	}
	body, err := ref.Ask(ctx, Message{Method: "hello"}, ActivateIfMissing)
	if err != nil || !strings.HasPrefix(string(body), "game-") || loads.Load() != 1 {
		t.Fatalf("未按 room -> game 激活: body=%s loads=%d error=%v", body, loads.Load(), err)
	}
	body, err = caller.Ref(Key{Type: "player", Id: "1"}).Ask(ctx, Message{Method: "hello"}, ActivateIfMissing)
	if err != nil || string(body) != "player-a:hello" {
		t.Fatalf("未按 player -> player 激活: body=%s error=%v", body, err)
	}
	body, err = caller.Ref(Key{Type: "hall", Id: "1"}).Ask(ctx, Message{Method: "hello"}, ActivateIfMissing)
	if err != nil || !strings.HasPrefix(string(body), "game-") {
		t.Fatalf("同一服务的第二种 Actor 类型无法调用: body=%s error=%v", body, err)
	}
	owner, found, err := caller.locator.Locate(ctx, key)
	if err != nil || !found {
		t.Fatal("缺少激活后的路由", err)
	}
	source, destination := a, b
	if owner.NodeId == b.instance.Id {
		source, destination = b, a
	}
	if err := source.system.Stop(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := destination.system.Spawn(ctx, key); err != nil {
		t.Fatal(err)
	}
	// 仍持有旧节点缓存，ExistingOnly 也应透明转向已经运行的新节点
	body, err = ref.Ask(ctx, Message{Method: "moved"}, ExistingOnly)
	if err != nil || string(body) != destination.instance.Id+":moved" {
		t.Fatalf("旧 Ref 未转向新节点: body=%s error=%v", body, err)
	}
	before, _, _ := caller.locator.Locate(ctx, key)
	if err := destination.system.Stop(ctx, key); err != nil {
		t.Fatal(err)
	}
	handle, err := destination.system.Spawn(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if handle.ActivationId() == before.ActivationId {
		t.Fatal("重新激活复用了 ActivationId")
	}
	response, err := caller.clients["room"].Ask(rpc.WithInstance(ctx, destination.instance.Id), &actorpb.CallRequest{
		Type: key.Type, Id: key.Id, ActivationId: before.ActivationId, Method: "stale",
	})
	if err != nil || response.Code != actorpb.CallCode_CALL_NOT_HERE {
		t.Fatalf("旧 ActivationId 未被拒绝: response=%v error=%v", response, err)
	}
	body, err = ref.Ask(ctx, Message{Method: "restarted"}, ExistingOnly)
	if err != nil || string(body) != destination.instance.Id+":restarted" {
		t.Fatalf("旧 Ref 未识别同节点重新激活: body=%s error=%v", body, err)
	}
	if err := destination.system.Stop(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := ref.Tell(ctx, Message{Method: "after-unload"}, ExistingOnly); !errors.Is(err, ErrNotFound) {
		t.Fatalf("卸载后 ExistingOnly 未返回不存在: %v", err)
	}
	if err := ref.Tell(ctx, Message{Method: "after-unload"}, ActivateIfMissing); err != nil {
		t.Fatalf("旧 Ref 无法重新激活: %v", err)
	}
}

func TestRefConcurrentActivation(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var loads, starts, calls atomic.Int32
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	loader := func(ctx context.Context, key Key) (Actor, error) {
		if loads.Add(1) == 1 {
			close(entered)
		}
		select {
		case <-resume:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		value := &messageActor{
			onMessage: func(MessageContext, Message) ([]byte, error) {
				calls.Add(1)
				return []byte("ok"), nil
			},
		}
		value.onStart = func(StartContext) error { starts.Add(1); return nil }
		return value, nil
	}
	a := newActorRPCNode(t, redisClient, "game-a", "game", map[string]Loader{"room": loader})
	b := newActorRPCNode(t, redisClient, "game-b", "game", map[string]Loader{"room": loader})
	discover := actorDiscover(t, a, b)
	clients := []*Client{
		actorClient(t, redisClient, discover, map[string]string{"room": "game"}),
		actorClient(t, redisClient, discover, map[string]string{"room": "game"}),
	}
	key := Key{Type: "room", Id: "1"}
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := clients[0].Ref(key).Ask(ctx, Message{}, ActivateIfMissing)
		first <- err
	}()
	waitLoader(t, entered)
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("取消共享激活的等待者: %v", err)
	}
	// 对另一节点直接激活必须返回 Owner，不允许执行第二次 Loader
	owner, _, err := a.system.guard.Owner(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	other := b.instance.Id
	if owner.NodeId == other {
		other = a.instance.Id
	}
	response, err := clients[0].clients["room"].Activate(rpc.WithInstance(context.Background(), other), &actorpb.ActivateRequest{Type: key.Type, Id: key.Id})
	if err != nil || response.Code != actorpb.ActivationCode_ACTIVATION_OWNED || response.NodeId != owner.NodeId {
		t.Fatalf("另一节点未返回正确 Owner: response=%v error=%v", response, err)
	}
	var wait sync.WaitGroup
	for index := range 64 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			body, err := clients[index%2].Ref(key).Ask(ctx, Message{}, ActivateIfMissing)
			if err != nil || string(body) != "ok" {
				t.Errorf("并发调用失败: body=%s error=%v", body, err)
			}
		}()
	}
	unblock()
	wait.Wait()
	if loads.Load() != 1 || starts.Load() != 1 || calls.Load() != 64 {
		t.Fatalf("并发激活或调用重复: loads=%d starts=%d calls=%d", loads.Load(), starts.Load(), calls.Load())
	}
}

func TestRefBusinessErrorsAreNotReplayed(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var executions atomic.Int32
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var ref *Ref
	loader := func(context.Context, Key) (Actor, error) {
		return &messageActor{
			onMessage: func(ctx MessageContext, message Message) ([]byte, error) {
				executions.Add(1)
				switch message.Method {
				case "unavailable":
					return nil, status.Error(codes.Unavailable, "业务暂不可用")
				case "not-found":
					return nil, ErrNotFound
				case "self":
					return ref.Ask(ctx, Message{}, ActivateIfMissing)
				case "timeout":
					close(entered)
					<-ctx.Done()
					<-resume
					return nil, ctx.Err()
				}
				return nil, nil
			},
		}, nil
	}
	node := newActorRPCNode(t, redisClient, "game-a", "game", map[string]Loader{"room": loader})
	caller := actorClient(t, redisClient, actorDiscover(t, node), map[string]string{"room": "game"})
	ref = caller.Ref(Key{Type: "room", Id: "1"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tc := range []struct {
		method string
		code   codes.Code
	}{{"unavailable", codes.Unavailable}, {"not-found", codes.NotFound}, {"self", codes.Unknown}} {
		before := executions.Load()
		_, err := ref.Ask(ctx, Message{Method: tc.method}, ActivateIfMissing)
		if status.Code(err) != tc.code || executions.Load() != before+1 {
			t.Fatalf("业务失败被当成路由错误重放: method=%s executions=%d error=%v", tc.method, executions.Load()-before, err)
		}
	}
	callCtx, stop := context.WithCancel(ctx)
	result := make(chan error, 1)
	go func() {
		_, err := ref.Ask(callCtx, Message{Method: "timeout"}, ActivateIfMissing)
		result <- err
	}()
	waitLoader(t, entered)
	stop()
	if err := <-result; status.Code(err) != codes.Canceled {
		t.Fatalf("取消 RPC: %v", err)
	}
	if executions.Load() != 4 {
		t.Fatalf("超时导致重复调用: %d", executions.Load())
	}
}

func TestRefEntityMissingDoesNotCreate(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var exists atomic.Bool
	node := newActorRPCNode(t, redisClient, "player-a", "player", map[string]Loader{
		"player": func(context.Context, Key) (Actor, error) {
			if !exists.Load() {
				return nil, ErrEntityNotFound
			}
			return &loadedActor{}, nil
		},
	})
	caller := actorClient(t, redisClient, actorDiscover(t, node), map[string]string{"player": "player"})
	ref := caller.Ref(Key{Type: "player", Id: "unregistered"})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := ref.Ask(ctx, Message{}, ActivateIfMissing); status.Code(err) != codes.NotFound {
		t.Fatalf("缺失实体没有返回 NotFound: %v", err)
	}
	if _, found, err := node.system.guard.Owner(ctx, ref.Key()); err != nil || found {
		t.Fatalf("加载失败未释放路由: found=%v error=%v", found, err)
	}
	exists.Store(true)
	if err := ref.Tell(ctx, Message{}, ActivateIfMissing); err != nil {
		t.Fatal(fmt.Errorf("业务创建实体后不能激活: %w", err))
	}
}

func TestRefDoesNotActivateItselfDuringOnStart(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var ref *Ref
	var started atomic.Bool
	loader := func(context.Context, Key) (Actor, error) {
		value := &loadedActor{}
		value.onStart = func(ctx StartContext) error {
			if err := ref.Tell(ctx, Message{}, ActivateIfMissing); !errors.Is(err, ErrNotFound) {
				t.Errorf("OnStart 等待激活自己未被拒绝: %v", err)
			}
			started.Store(true)
			return nil
		}
		return value, nil
	}
	node := newActorRPCNode(t, redisClient, "player-a", "player", map[string]Loader{"player": loader})
	caller := actorClient(t, redisClient, actorDiscover(t, node), map[string]string{"player": "player"})
	ref = caller.Ref(Key{Type: "player", Id: "1"})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := ref.Ask(ctx, Message{}, ActivateIfMissing); err != nil || !started.Load() {
		t.Fatalf("OnStart 自激活导致阻塞: started=%v error=%v", started.Load(), err)
	}
}

func TestRefCancelledDoesNotExecuteMessages(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var calls atomic.Int32
	node := newActorRPCNode(t, redisClient, "player-a", "player", map[string]Loader{
		"player": func(context.Context, Key) (Actor, error) {
			return &messageActor{
				onMessage: func(MessageContext, Message) ([]byte, error) {
					calls.Add(1)
					return nil, nil
				},
			}, nil
		},
	})
	caller := actorClient(t, redisClient, actorDiscover(t, node), map[string]string{"player": "player"})
	warm := caller.Ref(Key{Type: "player", Id: "cached"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := warm.Ask(ctx, Message{}, ActivateIfMissing); err != nil {
		t.Fatal(err)
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	for _, ref := range []*Ref{warm, caller.Ref(Key{Type: "player", Id: "missing"})} {
		if _, err := ref.Ask(cancelled, Message{}, ActivateIfMissing); !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
			t.Fatalf("已取消 Ask 不应执行消息: %v", err)
		}
		if err := ref.Tell(cancelled, Message{}, ActivateIfMissing); !errors.Is(err, context.Canceled) && status.Code(err) != codes.Canceled {
			t.Fatalf("已取消 Tell 不应执行消息: %v", err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("已取消调用仍然执行消息: calls=%d", calls.Load())
	}
}

type scriptedActorClient struct {
	activate func(context.Context, *actorpb.ActivateRequest) (*actorpb.ActivateResponse, error)
	call     func(context.Context, *actorpb.CallRequest) (*actorpb.CallResponse, error)
}

func (c scriptedActorClient) Activate(ctx context.Context, req *actorpb.ActivateRequest, _ ...grpc.CallOption) (*actorpb.ActivateResponse, error) {
	return c.activate(ctx, req)
}
func (c scriptedActorClient) Ask(ctx context.Context, req *actorpb.CallRequest, _ ...grpc.CallOption) (*actorpb.CallResponse, error) {
	return c.call(ctx, req)
}
func (c scriptedActorClient) Tell(ctx context.Context, req *actorpb.CallRequest, _ ...grpc.CallOption) (*actorpb.CallResponse, error) {
	return c.call(ctx, req)
}

func TestClientConfigurationFailures(t *testing.T) {
	_, redisClient := loaderRedis(t)
	discover := actorDiscover(t)
	locator := NewLocator(redisClient)
	t.Cleanup(locator.Close)
	services := map[string]string{"room": "game"}
	for _, run := range []func(){
		func() { _, _ = NewClient(nil, locator, services) },
		func() { _, _ = NewClient(discover, nil, services) },
		func() { _, _ = NewClient(discover, locator, nil) },
		func() { _, _ = NewClient(discover, locator, services, WithActivationTimeout(0)) },
		func() { _, _ = NewClient(discover, locator, map[string]string{"room": ""}) },
	} {
		assertPanic(t, run)
	}
	if _, err := NewClient(discover, locator, services); err == nil {
		t.Fatal("缺少传输凭证却构造成功")
	}
}

func TestActivationProtocolAndRetryBoundaries(t *testing.T) {
	for _, mode := range []string{"invalid-owner", "invalid-code", "owned-loop", "owned-ready", "closed"} {
		t.Run(mode, func(t *testing.T) {
			_, redisClient := loaderRedis(t)
			caller := actorClient(t, redisClient, actorDiscover(t), map[string]string{"room": "game"})
			entered, resume := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(resume) })
			defer unblock()
			calls := 0
			caller.clients["room"] = scriptedActorClient{activate: func(ctx context.Context, req *actorpb.ActivateRequest) (*actorpb.ActivateResponse, error) {
				calls++
				response := &actorpb.ActivateResponse{NodeId: "node", ActivationId: "a"}
				switch mode {
				case "invalid-owner":
					response.NodeId = ""
				case "invalid-code":
					response.Code = 99
				case "owned-loop":
					response.Code = actorpb.ActivationCode_ACTIVATION_OWNED
				case "owned-ready":
					if calls == 1 {
						response.Code = actorpb.ActivationCode_ACTIVATION_OWNED
					}
				case "closed":
					close(entered)
					<-ctx.Done()
					<-resume
					return nil, ctx.Err()
				}
				return response, nil
			}}
			key := Key{Type: "room", Id: "1"}
			if mode == "closed" {
				result := make(chan error, 1)
				go func() { _, err := caller.activate(context.Background(), key, Owner{}); result <- err }()
				waitLoader(t, entered)
				_ = caller.Close()
				if err := <-result; !errors.Is(err, ErrStopped) {
					t.Fatal(err)
				}
				return
			}
			owner, err := caller.activate(context.Background(), key, Owner{})
			if mode == "owned-ready" {
				if err != nil || calls != 2 || owner.NodeId != "node" {
					t.Fatalf("calls=%d owner=%v error=%v", calls, owner, err)
				}
			} else if err == nil {
				t.Fatal("无效响应或循环路由未报错")
			}
			if mode == "owned-loop" && (!errors.Is(err, ErrRouteChanged) || calls != maxRouteAttempts) {
				t.Fatalf("calls=%d error=%v", calls, err)
			}
		})
	}
}

func TestRefProtocolAndRouteFailures(t *testing.T) {
	for _, mode := range []string{"invalid-code", "refresh-failure", "unavailable-refresh-failure", "route-loop"} {
		t.Run(mode, func(t *testing.T) {
			server, redisClient := loaderRedis(t)
			caller := actorClient(t, redisClient, actorDiscover(t), map[string]string{"room": "game"})
			key := Key{Type: "room", Id: "1"}
			caller.locator.remember(key, Owner{NodeId: "node", ActivationId: "initial"})
			calls := 0
			caller.clients["room"] = scriptedActorClient{call: func(context.Context, *actorpb.CallRequest) (*actorpb.CallResponse, error) {
				calls++
				switch mode {
				case "invalid-code":
					return &actorpb.CallResponse{Code: 99}, nil
				case "refresh-failure":
					_ = redisClient.Close()
				case "unavailable-refresh-failure":
					_ = redisClient.Close()
					return nil, status.Error(codes.Unavailable, "断链")
				case "route-loop":
					value, _ := json.Marshal(Owner{NodeId: "node", ActivationId: fmt.Sprint(calls)})
					server.Set("ngame:actor:"+routeKey(key), string(value))
				}
				return &actorpb.CallResponse{Code: actorpb.CallCode_CALL_NOT_HERE}, nil
			}}
			_, err := caller.Ref(key).Ask(context.Background(), Message{}, ExistingOnly)
			if err == nil {
				t.Fatal("错误响应未返回错误")
			}
			if mode == "route-loop" {
				if !errors.Is(err, ErrRouteChanged) || calls != maxRouteAttempts {
					t.Fatalf("calls=%d error=%v", calls, err)
				}
			} else if calls != 1 {
				t.Fatalf("错误请求被重放 %d 次", calls)
			}
		})
	}
}

func TestRefInputAndStoppedSelf(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var ref *Ref
	stopped := make(chan error, 1)
	node := newActorRPCNode(t, redisClient, "node", "game", map[string]Loader{"room": func(context.Context, Key) (Actor, error) {
		value := &unloadActor{onStop: func(ctx StopContext) { stopped <- ref.Tell(ctx, Message{}, ActivateIfMissing) }}
		value.onMessage = func(MessageContext, Message) ([]byte, error) { return nil, nil }
		return value, nil
	}})
	caller := actorClient(t, redisClient, actorDiscover(t, node), map[string]string{"room": "game"})
	if _, err := caller.Ref(Key{}).Ask(context.Background(), Message{}, ExistingOnly); !errors.Is(err, ErrInvalidKey) {
		t.Fatal(err)
	}
	if _, err := caller.Ref(Key{Type: "other", Id: "1"}).Ask(context.Background(), Message{}, ExistingOnly); !errors.Is(err, ErrTypeNotRegistered) {
		t.Fatal(err)
	}
	ref = caller.Ref(Key{Type: "room", Id: "1"})
	if _, err := ref.Ask(context.Background(), Message{}, 99); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := ref.Ask(context.Background(), Message{}, ActivateIfMissing); err != nil {
		t.Fatal(err)
	}
	if err := node.system.Stop(context.Background(), ref.Key()); err != nil {
		t.Fatal(err)
	}
	if err := <-stopped; !errors.Is(err, ErrStopping) {
		t.Fatal(err)
	}
}

func TestActorRPCValidationAndErrorCodes(t *testing.T) {
	_, client := loaderRedis(t)
	system := New(NewGuard("node", client))
	server := &rpcServer{system: system}
	t.Cleanup(func() { _ = system.Shutdown(context.Background()) })
	if _, err := server.Ask(context.Background(), &actorpb.CallRequest{}); status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
	if _, err := server.Tell(context.Background(), &actorpb.CallRequest{Type: "room", Id: "1", ActivationId: "a"}); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		err  error
		code codes.Code
	}{
		{ErrInvalidKey, codes.InvalidArgument},
		{ErrTypeNotRegistered, codes.FailedPrecondition},
		{ErrMailboxFull, codes.ResourceExhausted},
		{ErrNotStarted, codes.Unavailable},
		{ErrStopped, codes.Unavailable},
		{ErrStopping, codes.Unavailable},
		{ErrGuardLost, codes.Unavailable},
		{context.DeadlineExceeded, codes.DeadlineExceeded},
	} {
		if got := status.Code(actorRPCError(tc.err)); got != tc.code {
			t.Fatalf("error=%v code=%v want=%v", tc.err, got, tc.code)
		}
	}
}

func TestActorRPCStopWaitCancellation(t *testing.T) {
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
	if _, err := (&rpcServer{system: system}).Activate(ctx, &actorpb.ActivateRequest{Type: key.Type, Id: key.Id}); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
}

func TestActorRPCStopDuringOnStartKeepsCapturedRunner(t *testing.T) {
	_, client := loaderRedis(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	system := loaderSystem(t, NewGuard("node", client), func(context.Context, Key) (Actor, error) {
		value := &unloadActor{onStop: func(StopContext) { close(entered); <-resume }}
		value.onStart = func(ctx StartContext) error { ctx.Self.Stop(); return nil }
		return value, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := (&rpcServer{system: system}).Activate(ctx, &actorpb.ActivateRequest{Type: "player", Id: "1"})
		result <- err
	}()
	waitLoader(t, entered)
	cancel()
	if err := <-result; status.Code(err) != codes.Canceled {
		t.Fatalf("启动中停止没有等待原 runner: %v", err)
	}
}
