package actor

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
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
	"github.com/spf13/cast"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type unloadActor struct {
	messageActor
	onStop func(StopContext)
}

func (a *unloadActor) OnStop(ctx StopContext) { a.onStop(ctx) }

func TestRefConcurrentUnloadPersistsBeforeReactivation(t *testing.T) {
	_, redisClient := loaderRedis(t)
	// 业务测试存储：由 Loader 读取、OnStop 保存，不属于 Actor System
	var storageMu sync.Mutex
	stored := 0
	var loads, active atomic.Int32
	var unloading atomic.Bool
	activationDuringUnload := make(chan struct{}, 1)
	observeActivation := grpc.ChainUnaryInterceptor(func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if unloading.Load() && info.FullMethod == actorpb.Actor_Activate_FullMethodName {
			select {
			case activationDuringUnload <- struct{}{}:
			default:
			}
		}
		return handler(ctx, request)
	})
	stopping, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	loader := func(context.Context, Key) (Actor, error) {
		generation := loads.Add(1)
		storageMu.Lock()
		count := stored
		storageMu.Unlock()
		value := &unloadActor{}
		value.onStart = func(StartContext) error {
			if running := active.Add(1); running != 1 {
				t.Errorf("同 Key 同时运行 %d 个实例", running)
			}
			return nil
		}
		value.onMessage = func(ctx MessageContext, message Message) ([]byte, error) {
			if message.Method == "increment" {
				count++
			}
			return []byte(cast.ToString(count)), nil
		}
		value.onStop = func(ctx StopContext) {
			if generation == 1 {
				close(stopping)
				<-resume
			}
			if err := ctx.Err(); err != nil {
				t.Errorf("正常卸载的保存上下文被取消: %v", err)
			}
			storageMu.Lock()
			stored = count
			storageMu.Unlock()
			active.Add(-1)
		}
		return value, nil
	}
	a := newActorRPCNode(t, redisClient, "game-a", "game", map[string]Loader{"room": loader}, observeActivation)
	b := newActorRPCNode(t, redisClient, "game-b", "game", map[string]Loader{"room": loader}, observeActivation)
	caller := actorClient(t, redisClient, actorDiscover(t, a, b), map[string]string{"room": "game"})
	key := Key{Type: "room", Id: "1"}
	ref := caller.Ref(key)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for index := range 2 {
		body, err := ref.Ask(ctx, Message{Method: "increment"}, ActivateIfMissing)
		if err != nil || string(body) != cast.ToString(index+1) {
			t.Fatalf("初始数据更新失败: body=%s error=%v", body, err)
		}
	}
	owner, _, err := caller.locator.Locate(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	source, other := a, b
	if owner.NodeId == b.instance.Id {
		source, other = b, a
	}
	stopResult := make(chan error, 1)
	unloading.Store(true)
	go func() { stopResult <- source.system.Stop(ctx, key) }()
	waitLoader(t, stopping)
	if _, err := ref.Ask(ctx, Message{Method: "read"}, ExistingOnly); !errors.Is(err, ErrNotFound) {
		t.Fatalf("卸载期间 ExistingOnly 不应激活: %v", err)
	}
	if _, err := other.system.Spawn(ctx, key); err == nil {
		t.Fatal("旧 OnStop 未保存完成，其他节点已激活同 Key")
	} else {
		var owned *OwnedError
		if !errors.As(err, &owned) || owned.Owner != owner {
			t.Fatalf("卸载期间没有继续持有原归属: %v", err)
		}
	}
	// 64 个调用与仍未结束的卸载重叠；另一半 Tell 在新实例中累加数据
	var wait sync.WaitGroup
	for index := range 64 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if index%2 == 0 {
				_, err := ref.Ask(ctx, Message{Method: "read"}, ActivateIfMissing)
				if err != nil {
					t.Error(err)
				}
			} else if err := ref.Tell(ctx, Message{Method: "increment"}, ActivateIfMissing); err != nil {
				t.Error(err)
			}
		}()
	}
	waitLoader(t, activationDuringUnload)
	if loads.Load() != 1 || active.Load() != 1 {
		t.Fatalf("OnStop 未完成就开始加载: loads=%d active=%d", loads.Load(), active.Load())
	}
	unblock()
	if err := <-stopResult; err != nil {
		t.Fatal(err)
	}
	wait.Wait()
	body, err := ref.Ask(ctx, Message{Method: "read"}, ExistingOnly)
	if err != nil || string(body) != "34" || loads.Load() != 2 || active.Load() != 1 {
		t.Fatalf("卸载/激活丢失或重复更新: body=%s loads=%d active=%d error=%v", body, loads.Load(), active.Load(), err)
	}
}

func TestOnStopCancelledWhenLeaseLost(t *testing.T) {
	redisServer, client := loaderRedis(t)
	entered := make(chan struct{})
	result := make(chan error, 1)
	system := loaderSystem(t, NewGuard("node-a", client, WithGuardTTL(300*time.Millisecond)), func(context.Context, Key) (Actor, error) {
		return &unloadActor{
			onStop: func(ctx StopContext) {
				if err := ctx.Err(); err != nil {
					t.Errorf("正常卸载尚未保存就取消: %v", err)
				}
				close(entered)
				<-ctx.Done()
				result <- ctx.Err()
			},
		}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	key := Key{Type: "player", Id: "1"}
	handle, err := system.Spawn(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	handle.Stop()
	waitLoader(t, entered)
	redisServer.FastForward(time.Second)
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("租约失效没有取消 OnStop: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("OnStop 未响应租约失效", ctx.Err())
	}
	if err := system.Stop(ctx, key); err != nil {
		t.Fatal(err)
	}
}

// 仅作为子进程运行，父测试使用 SIGKILL，确保没有执行 Shutdown/释放租约
func TestActorRecoveryNodeProcess(t *testing.T) {
	if os.Getenv("NGAME_ACTOR_RECOVERY_CHILD") != "1" {
		return
	}
	client := redis.NewClient(&redis.Options{Addr: os.Getenv("NGAME_ACTOR_RECOVERY_REDIS"), MaxRetries: -1})
	id := os.Getenv("NGAME_ACTOR_RECOVERY_NODE")
	system := New(NewGuard(id, client, WithGuardTTL(600*time.Millisecond)))
	system.Register("room", func(ctx context.Context, key Key) (Actor, error) {
		count, err := client.Get(ctx, "business:recovery:"+key.Id).Int()
		if err != nil && !errors.Is(err, redis.Nil) {
			return nil, err
		}
		return &messageActor{
			onMessage: func(ctx MessageContext, message Message) ([]byte, error) {
				if message.Method != "read" {
					count++
					if err := client.Set(ctx, "business:recovery:"+key.Id, count, 0).Err(); err != nil {
						return nil, err
					}
				}
				if message.Method == "crash-before-reply" {
					fmt.Println("NGAME_APPLIED")
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return []byte(cast.ToString(count)), nil
			},
		}, nil
	})
	if err := system.Start(); err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", os.Getenv("NGAME_ACTOR_RECOVERY_ADDRESS"))
	if err != nil {
		t.Fatal(err)
	}
	server := rpc.NewServer(id)
	system.RegisterRPC(server)
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Error(err)
		}
	}()
	fmt.Println("NGAME_READY " + listener.Addr().String())
	select {}
}

type recoveryProcess struct {
	address string
	applied <-chan struct{}
	stop    func()
}

func startRecoveryProcess(t *testing.T, redisAddress, id, address string) *recoveryProcess {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^TestActorRecoveryNodeProcess$", "-test.timeout=30s")
	command.Env = append(os.Environ(),
		"NGAME_ACTOR_RECOVERY_CHILD=1",
		"NGAME_ACTOR_RECOVERY_REDIS="+redisAddress,
		"NGAME_ACTOR_RECOVERY_NODE="+id,
		"NGAME_ACTOR_RECOVERY_ADDRESS="+address,
	)
	var stderr bytes.Buffer
	command.Stderr = &stderr
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	exited := make(chan struct{})
	go func() { _ = command.Wait(); close(exited) }()
	stop := sync.OnceFunc(func() {
		_ = command.Process.Kill()
		<-exited
		if strings.Contains(stderr.String(), "WARNING: DATA RACE") {
			t.Errorf("节点子进程发现数据竞争: %s", stderr.String())
		}
	})
	t.Cleanup(stop)
	ready, applied := make(chan string, 1), make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "NGAME_READY ") {
				ready <- strings.TrimPrefix(line, "NGAME_READY ")
			} else if line == "NGAME_APPLIED" {
				close(applied)
			}
		}
	}()
	select {
	case actual := <-ready:
		return &recoveryProcess{address: actual, applied: applied, stop: stop}
	case <-exited:
		t.Fatalf("节点启动前退出: %s", stderr.String())
	case <-time.After(5 * time.Second):
		stop()
		t.Fatalf("节点启动超时: %s", stderr.String())
	}
	return nil
}

func TestRefProcessCrashAndRestartAtSameAddress(t *testing.T) {
	redisServer, redisClient := loaderRedis(t)
	oldId, newId := "game-start-1", "game-start-2"
	first := startRecoveryProcess(t, redisClient.Options().Addr, oldId, "127.0.0.1:0")
	host, port, err := net.SplitHostPort(first.address)
	if err != nil {
		t.Fatal(err)
	}
	instance := endpoint.ServiceInst{Id: oldId, Name: "game", RpcHost: host, RpcPort: cast.ToInt(port), Weight: 1}
	updates := make(chan []endpoint.ServiceInst, 1)
	discover := registry.NewDiscover(func(ctx context.Context, publish func([]endpoint.ServiceInst)) error {
		publish([]endpoint.ServiceInst{instance})
		for {
			select {
			case instances := <-updates:
				publish(instances)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	t.Cleanup(discover.Close)
	caller := actorClient(t, redisClient, discover, map[string]string{"room": "game"})
	ref := caller.Ref(Key{Type: "room", Id: "1"})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	body, err := ref.Ask(ctx, Message{Method: "increment"}, ActivateIfMissing)
	if err != nil || string(body) != "1" {
		t.Fatalf("初次调用失败: body=%s error=%v", body, err)
	}
	oldOwner, _, err := caller.locator.Locate(ctx, ref.Key())
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := ref.Ask(ctx, Message{Method: "crash-before-reply"}, ActivateIfMissing)
		result <- err
	}()
	waitLoader(t, first.applied)
	first.stop()
	if err := <-result; status.Code(err) != codes.Unavailable {
		t.Fatalf("已执行但未回包的崩溃调用: %v", err)
	}
	if count, err := redisClient.Get(ctx, "business:recovery:1").Int(); err != nil || count != 2 {
		t.Fatalf("结果未知的请求被重放: count=%d error=%v", count, err)
	}
	// 崩溃不执行释放，必须由 Redis TTL 清理归属；无需真的等待 600ms
	redisServer.FastForward(2 * time.Second)
	if _, found, err := readOwner(ctx, redisClient, "ngame:actor:", ref.Key()); err != nil || found {
		t.Fatalf("崩溃节点的归属没有过期: found=%v error=%v", found, err)
	}
	second := startRecoveryProcess(t, redisClient.Options().Addr, newId, first.address)
	if second.address != first.address {
		t.Fatal("重启未复用同一监听地址")
	}
	// 即使旧连接碰巧连上相同地址的新进程，RPC 实例身份检查也必须拒绝
	connection, err := grpc.NewClient(second.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	staleCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs("ngame-instance-id", oldId))
	if _, err := actorpb.NewActorClient(connection).Activate(staleCtx, &actorpb.ActivateRequest{Type: "room", Id: "1"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("新进程接受了旧节点身份: %v", err)
	}
	updated := instance
	updated.Id = newId
	updates <- []endpoint.ServiceInst{updated}
	// 等待真实 Resolver/Picker 接收新 Id；Activate 可以安全重复确认，不执行业务消息
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		response, err := caller.clients["room"].Activate(rpc.WithInstance(ctx, newId), &actorpb.ActivateRequest{Type: "room", Id: "1"})
		if err == nil {
			if response.NodeId != newId || response.ActivationId == oldOwner.ActivationId {
				t.Fatalf("重启后归属无效: %v", response)
			}
			break
		}
		if status.Code(err) != codes.Unavailable {
			t.Fatal(err)
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("RPC 没有接收到重启后的节点", ctx.Err())
		}
	}
	// Ref 仍缓存旧 NodeId：这次失败只刷新缓存，不能把未知请求重放到新节点
	if _, err := ref.Ask(ctx, Message{Method: "read"}, ExistingOnly); status.Code(err) != codes.Unavailable {
		t.Fatalf("旧节点调用应该返回不可用并刷新路由: %v", err)
	}
	body, err = ref.Ask(ctx, Message{Method: "read"}, ExistingOnly)
	if err != nil || string(body) != "2" {
		t.Fatalf("同一个 Ref 无法读取重启前的数据: body=%s error=%v", body, err)
	}
	body, err = ref.Ask(ctx, Message{Method: "increment"}, ActivateIfMissing)
	if err != nil || string(body) != "3" {
		t.Fatalf("重启后更新失败: body=%s error=%v", body, err)
	}
}
