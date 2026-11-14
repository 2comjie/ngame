package gate

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/2comjie/ngame/endpoint"
	gatepb "github.com/2comjie/ngame/gate/protocol"
	"github.com/2comjie/ngame/network"
	netTcp "github.com/2comjie/ngame/network/transport/tcp"
	"github.com/2comjie/ngame/packet"
	"github.com/2comjie/ngame/registry"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

type uidRegistry struct {
	registry.Registry
	instance endpoint.ServiceInst
}

type uidRegistration struct {
	registry.Registration
	done chan struct{}
	once sync.Once
}

func (r *uidRegistry) Register(ctx context.Context, instance endpoint.ServiceInst) (registry.Registration, error) {
	r.instance = instance
	return &uidRegistration{done: make(chan struct{})}, nil
}
func (r *uidRegistration) Done() <-chan struct{} { return r.done }
func (r *uidRegistration) Unregister(context.Context) error {
	r.once.Do(func() { close(r.done) })
	return nil
}

func uidWait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatal("等待连接事件超时")
		var zero T
		return zero
	}
}

func uidRedis(t *testing.T) *redis.Client {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr:                  server.Addr(),
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

type uidGate struct {
	app     *App
	address string
	bound   chan *network.Session
	ended   chan *network.Session
}

func newUidGate(t *testing.T, client *redis.Client, forward ForwardFunc) *uidGate {
	t.Helper()
	listener, err := netTcp.Listen("127.0.0.1:0", netTcp.WithoutTLS())
	if err != nil {
		t.Fatal(err)
	}
	rpcListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	fixture := &uidGate{
		address: listener.Addr().String(),
		bound:   make(chan *network.Session, 16),
		ended:   make(chan *network.Session, 16),
	}
	if forward == nil {
		forward = func(context.Context, *Request) (*Response, error) {
			return &Response{Replied: true, Body: []byte("ok")}, nil
		}
	}
	fixture.app = New(Config{
		Instance: endpoint.ServiceInst{
			Id:      "gate-01",
			Name:    "gate",
			RpcHost: "127.0.0.1",
			RpcPort: rpcListener.Addr().(*net.TCPAddr).Port,
			Weight:  1,
		},
		Registry: &uidRegistry{},
		Binder: NewBinder(BinderConfig{
			Redis:        client,
			TTL:          3 * time.Second,
			SyncInterval: 20 * time.Millisecond,
		}),
		RPCListener: rpcListener,
		NetworkOptions: []network.Option{
			network.WithListener(listener),
			network.WithAuther(network.AuthFunc(func(context.Context, []byte) (uint64, error) { return 42, nil })),
			network.OnSessionBind(func(s *network.Session) { fixture.bound <- s }),
			network.OnSessionEnd(func(s *network.Session) { fixture.ended <- s }),
		},
		Forward: forward,
		ErrorHandler: func(c *network.ReqContext, err error) {
			t.Errorf("请求处理错误: %v", err)
		},
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := fixture.app.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	if err := fixture.app.Start(); err != nil {
		t.Fatal(err)
	}
	return fixture
}

type uidConnection struct {
	client  *network.Client
	session *network.Session
	pushes  chan *packet.Message
}

func (g *uidGate) connect(t *testing.T) *uidConnection {
	t.Helper()
	connection := &uidConnection{
		client: network.NewClient(network.WithDialer(netTcp.NewDialer(g.address, netTcp.WithoutTLS()))),
		pushes: make(chan *packet.Message, 8),
	}
	connection.client.OnPush(1, func(ctx context.Context, message *packet.Message) { connection.pushes <- message })
	t.Cleanup(func() { _ = connection.client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := connection.client.Connect(ctx, []byte("ticket")); err != nil {
		t.Fatal(err)
	}
	connection.session = uidWait(t, g.bound)
	return connection
}

func TestUidReconnectKeepsNewBinding(t *testing.T) {
	client := uidRedis(t)
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	fixture := newUidGate(t, client, func(ctx context.Context, req *Request) (*Response, error) {
		if req.Route == 2 {
			close(entered)
			<-resume // 延迟旧连接退出回调，确保新连接已接管后才解绑。
		}
		return &Response{Replied: req.NeedReply, Body: []byte("ok")}, nil
	})
	old := fixture.connect(t)
	if err := old.client.Tell(context.Background(), &packet.Message{Header: packet.Header{Route: 2}}); err != nil {
		t.Fatal(err)
	}
	uidWait(t, entered)
	current := fixture.connect(t)
	if current.session == old.session || fixture.app.server.Session(42) != current.session {
		t.Fatal("新连接未接管 Uid")
	}
	unblock()
	if ended := uidWait(t, fixture.ended); ended != old.session {
		t.Fatal("退出的不是旧连接")
	}
	locator := NewLocator(LocatorConfig{Redis: client})
	defer locator.Close()
	gateId, found, err := locator.Locate(context.Background(), 42)
	if err != nil || !found || gateId != fixture.app.InstanceId() {
		t.Fatalf("旧连接退出删除了新归属: gate=%s found=%v error=%v", gateId, found, err)
	}
	response, err := fixture.app.Push(context.Background(), &gatepb.PushRequest{Uid: 42, Route: 1, Body: []byte("new")})
	if err != nil || response.Code != gatepb.PushCode_PUSH_SENT {
		t.Fatalf("Uid 推送失败: response=%v error=%v", response, err)
	}
	if message := uidWait(t, current.pushes); string(message.Body) != "new" {
		t.Fatalf("推送内容=%q", message.Body)
	}
	multi, err := fixture.app.MultiPush(context.Background(), &gatepb.MultiPushRequest{Uids: []uint64{42, 42, 43}, Route: 1})
	if err != nil || len(multi.Results) != 2 || multi.Results[0].Uid != 42 || multi.Results[0].Code != gatepb.PushCode_PUSH_SENT || multi.Results[1].Code != gatepb.PushCode_PUSH_SESSION_NOT_HERE {
		t.Fatalf("批量 Uid 去重失败: response=%v error=%v", multi, err)
	}
	uidWait(t, current.pushes)
	mock, err := fixture.app.MockCall(context.Background(), &gatepb.MockCallRequest{Uid: 42, Route: 3})
	if err != nil || !mock.Replied || string(mock.Body) != "ok" {
		t.Fatalf("Uid MockCall 失败: response=%v error=%v", mock, err)
	}
	kick, err := fixture.app.Kick(context.Background(), &gatepb.KickRequest{Uid: 42})
	if err != nil || kick.Code != gatepb.KickCode_KICK_CLOSED {
		t.Fatalf("Uid Kick 失败: response=%v error=%v", kick, err)
	}
	uidWait(t, current.client.Done())
	uidWait(t, fixture.ended)
	if _, found, err := locator.Refresh(context.Background(), 42, gateId); err != nil || found {
		t.Fatalf("最后一个连接退出没有解绑: found=%v error=%v", found, err)
	}
}

func TestUidGateRestartProtectsNewInstance(t *testing.T) {
	client := uidRedis(t)
	old := newUidGate(t, client, nil)
	old.connect(t)
	current := newUidGate(t, client, nil)
	connection := current.connect(t)
	if old.app.InstanceId() == current.app.InstanceId() {
		t.Fatal("相同节点名前缀复用了实例 Id")
	}
	if registered := current.app.registry.(*uidRegistry).instance.Id; registered != current.app.InstanceId() {
		t.Fatalf("注册 Id 与运行 Id 不同: %s", registered)
	}
	if err := old.app.binder.Unbind(context.Background(), 42, old.app.InstanceId()); err != nil {
		t.Fatal(err)
	}
	lost, err := old.app.binder.renew(context.Background(), old.app.InstanceId(), []uint64{42})
	if _, ok := lost[42]; err != nil || !ok {
		t.Fatalf("旧进程续租没有检测到归属丢失: lost=%v error=%v", lost, err)
	}
	locator := NewLocator(LocatorConfig{Redis: client})
	defer locator.Close()
	gateId, found, err := locator.Locate(context.Background(), 42)
	if err != nil || !found || gateId != current.app.InstanceId() {
		t.Fatalf("旧进程改动了新归属: gate=%s found=%v error=%v", gateId, found, err)
	}
	conn, err := grpc.NewClient(current.app.rpcListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	rpcClient := gatepb.NewGateClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	wrong := metadata.NewOutgoingContext(ctx, metadata.Pairs("ngame-instance-id", old.app.InstanceId()))
	if _, err := rpcClient.SessionCount(wrong, &emptypb.Empty{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("RPC 接受了旧实例 Id: %v", err)
	}
	right := metadata.NewOutgoingContext(ctx, metadata.Pairs("ngame-instance-id", gateId))
	response, err := rpcClient.Push(right, &gatepb.PushRequest{Uid: 42, Route: 1, Body: []byte("current")})
	if err != nil || response.Code != gatepb.PushCode_PUSH_SENT {
		t.Fatalf("新实例 RPC 身份不匹配: response=%v error=%v", response, err)
	}
	if message := uidWait(t, connection.pushes); string(message.Body) != "current" {
		t.Fatalf("新进程未收到推送: %q", message.Body)
	}
}

type renewSnapshotHook struct {
	entered chan struct{}
	resume  chan struct{}
	checked chan struct{}
	pause   sync.Once
	check   sync.Once
}

func (*renewSnapshotHook) DialHook(next redis.DialHook) redis.DialHook          { return next }
func (*renewSnapshotHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook { return next }
func (h *renewSnapshotHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, commands []redis.Cmder) error {
		err := next(ctx, commands)
		if err != nil {
			return err
		}
		for _, command := range commands {
			if command.Name() != "eval" || command.Args()[1] != renewScript {
				continue
			}
			lost, err := command.(*redis.Cmd).StringSlice()
			if err != nil {
				return err
			}
			if len(lost) > 0 {
				h.pause.Do(func() { close(h.entered); <-h.resume })
			} else {
				select {
				case <-h.entered:
					h.check.Do(func() { close(h.checked) })
				default:
				}
			}
		}
		return nil
	}
}

func TestUidRenewSnapshotDoesNotCloseReplacement(t *testing.T) {
	client := uidRedis(t)
	hook := &renewSnapshotHook{entered: make(chan struct{}), resume: make(chan struct{}), checked: make(chan struct{})}
	client.AddHook(hook)
	unblock := sync.OnceFunc(func() { close(hook.resume) })
	defer unblock()
	fixture := newUidGate(t, client, nil)
	old := fixture.connect(t)
	key := fixture.app.binder.keys[42%sessionRouteBuckets]
	if err := client.HSet(context.Background(), key, "42", "other-gate").Err(); err != nil {
		t.Fatal(err)
	}
	uidWait(t, hook.entered)
	current := fixture.connect(t)
	unblock()
	uidWait(t, hook.checked) // 下一轮成功续租，说明旧快照已经处理完。
	if current.session.Context().Err() != nil || fixture.app.server.Session(42) != current.session {
		t.Fatal("旧续租结果关闭了新连接")
	}
	if old.session.Context().Err() == nil {
		t.Fatal("旧连接未关闭")
	}
	response, err := fixture.app.Push(context.Background(), &gatepb.PushRequest{Uid: 42, Route: 1})
	if err != nil || response.Code != gatepb.PushCode_PUSH_SENT {
		t.Fatalf("新连接不可推送: response=%v error=%v", response, err)
	}
	uidWait(t, current.pushes)
}

func TestUidRenewBatchesAndLargeUid(t *testing.T) {
	client := uidRedis(t)
	binder := NewBinder(BinderConfig{Redis: client})
	const count = sessionRouteBuckets * (sessionSyncBatch + 1)
	uids := make([]uint64, 0, count+1)
	pipeline := client.Pipeline()
	for uid := uint64(1); uid <= count; uid++ {
		uids = append(uids, uid)
		if uid != 1 && uid != count {
			pipeline.HSet(context.Background(), binder.keys[uid%sessionRouteBuckets], cast.ToString(uid), "gate-a")
		}
	}
	const largeUid = ^uint64(0)
	uids = append(uids, largeUid)
	pipeline.HSet(context.Background(), binder.keys[(count/2)%sessionRouteBuckets], cast.ToString(count/2), "gate-b")
	if _, err := pipeline.Exec(context.Background()); err != nil {
		t.Fatal(err)
	}
	lost, err := binder.renew(context.Background(), "gate-a", uids)
	if err != nil || len(lost) != 4 {
		t.Fatalf("续租分桶/分批结果错误: lost=%v error=%v", lost, err)
	}
	for _, uid := range []uint64{1, count, count / 2, largeUid} {
		if _, ok := lost[uid]; !ok {
			t.Errorf("遗漏失去归属的 Uid=%d", uid)
		}
	}
	if value := client.HGet(context.Background(), binder.keys[(count/2)%sessionRouteBuckets], cast.ToString(count/2)).Val(); value != "gate-b" {
		t.Fatalf("续租覆盖了其他网关归属: %s", value)
	}
}

type uidPushClient struct {
	gatepb.GateClient
	firstCode gatepb.PushCode
	firstErr  error
	calls     int
}

func (c *uidPushClient) Push(ctx context.Context, req *gatepb.PushRequest, _ ...grpc.CallOption) (*gatepb.PushResponse, error) {
	c.calls++
	if c.calls == 1 {
		if c.firstErr != nil {
			return nil, c.firstErr
		}
		return &gatepb.PushResponse{Code: c.firstCode}, nil
	}
	return &gatepb.PushResponse{Code: gatepb.PushCode_PUSH_SENT}, nil
}

func TestUidClientRetryBoundary(t *testing.T) {
	transportFailure := errors.New("连接中断")
	cases := []struct {
		name      string
		code      gatepb.PushCode
		firstErr  error
		wantErr   error
		wantCalls int
	}{
		{
			name:      "明确不在本节点才重试",
			code:      gatepb.PushCode_PUSH_SESSION_NOT_HERE,
			wantCalls: 2,
		},
		{
			name:      "投递未知不重发",
			code:      gatepb.PushCode_PUSH_DELIVERY_UNKNOWN,
			wantErr:   ErrDeliveryUnknown,
			wantCalls: 1,
		},
		{
			name:      "通信失败不重发",
			firstErr:  transportFailure,
			wantErr:   transportFailure,
			wantCalls: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			redisClient := uidRedis(t)
			binder := NewBinder(BinderConfig{Redis: redisClient})
			locator := NewLocator(LocatorConfig{Redis: redisClient})
			defer locator.Close()
			if err := binder.Bind(context.Background(), 42, "gate-old"); err != nil {
				t.Fatal(err)
			}
			if _, _, err := locator.Locate(context.Background(), 42); err != nil {
				t.Fatal(err)
			}
			if err := binder.Bind(context.Background(), 42, "gate-new"); err != nil {
				t.Fatal(err)
			}
			peer := &uidPushClient{firstCode: tc.code, firstErr: tc.firstErr}
			err := NewClient(peer, locator).Push(context.Background(), 42, &packet.Message{Header: packet.Header{Route: 1}})
			if !errors.Is(err, tc.wantErr) || peer.calls != tc.wantCalls {
				t.Fatalf("error=%v calls=%d", err, peer.calls)
			}
			gateId, found, err := locator.Locate(context.Background(), 42)
			if err != nil || !found || gateId != "gate-new" {
				t.Fatalf("归属未刷新: gate=%s found=%v error=%v", gateId, found, err)
			}
		})
	}
}
