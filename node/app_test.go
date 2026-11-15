package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/2comjie/ngame/actor"
	"github.com/2comjie/ngame/app"
	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ngame/registry"
	"github.com/2comjie/ngame/rpc"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type metaRegistration struct {
	mutex   sync.Mutex
	meta    map[string]string
	updates chan map[string]string
	done    chan struct{}
	once    sync.Once
	calls   atomic.Int32
	fail    atomic.Bool
}

func (r *metaRegistration) Update(ctx context.Context, instance endpoint.ServiceInst) error {
	return r.UpdateMeta(ctx, instance.Meta)
}

func (r *metaRegistration) UpdateMeta(ctx context.Context, meta map[string]string) error {
	r.calls.Add(1)
	if r.fail.Load() {
		return errors.New("测试上报失败")
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	select {
	case <-r.done:
		return registry.ErrRegistrationClosed
	case <-ctx.Done():
		return ctx.Err()
	case r.updates <- maps.Clone(meta):
		r.meta = maps.Clone(meta)
		return nil
	}
}

func (r *metaRegistration) Unregister(context.Context) error {
	r.once.Do(func() { close(r.done) })
	return nil
}

func (r *metaRegistration) Done() <-chan struct{} { return r.done }

type metaRegistry struct {
	instance     endpoint.ServiceInst
	registration *metaRegistration
}

func (r *metaRegistry) Register(_ context.Context, instance endpoint.ServiceInst) (registry.Registration, error) {
	r.instance = instance
	return r.registration, nil
}

func newMetaRegistry() *metaRegistry {
	return &metaRegistry{registration: &metaRegistration{
		updates: make(chan map[string]string, 256),
		done:    make(chan struct{}),
	}}
}

func nodeApp(t *testing.T, center *metaRegistry) *App {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	n := New(Config{
		Instance: endpoint.ServiceInst{
			Id:      "node",
			Name:    "game",
			RpcHost: "127.0.0.1",
			RpcPort: listener.Addr().(*net.TCPAddr).Port,
			Weight:  1,
			Meta:    map[string]string{"zone": "one"},
		},
		Registry:    center,
		RPCServer:   rpc.NewServer("node"),
		RPCListener: listener,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := n.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	return n
}

func TestReportMetaBeforeAndAfterRegistration(t *testing.T) {
	center := newMetaRegistry()
	n := nodeApp(t, center)
	ctx := context.Background()
	values := map[string]string{"business": "ready"}
	if err := n.ReportMeta(ctx, values); err != nil {
		t.Fatal(err)
	}
	values["business"] = "changed"
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	if center.instance.Meta["zone"] != "one" || center.instance.Meta["business"] != "ready" {
		t.Fatal("注册前的数据未合并或未复制", center.instance.Meta)
	}
	if err := n.ReportMeta(ctx, map[string]string{"zone": "two"}); err != nil {
		t.Fatal(err)
	}
	meta := <-center.registration.updates
	if meta["zone"] != "two" || meta["business"] != "ready" {
		t.Fatal("增量上报覆盖了其他字段", meta)
	}
	if center.instance.Meta["zone"] != "one" {
		t.Fatal("更新修改了已发布的注册快照")
	}
}

func TestReportMetaConcurrent(t *testing.T) {
	center := newMetaRegistry()
	n := nodeApp(t, center)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	report := func(prefix string) {
		var wait sync.WaitGroup
		start := make(chan struct{})
		for index := range 64 {
			wait.Add(1)
			go func() {
				defer wait.Done()
				<-start
				if err := n.ReportMeta(ctx, map[string]string{fmt.Sprintf("%s-%d", prefix, index): "one"}); err != nil {
					t.Error(err)
				}
				snapshot := n.Instance()
				snapshot.Meta["zone"] = "outside"
			}()
		}
		close(start)
		wait.Wait()
	}
	report("before")
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	if len(center.instance.Meta) != 65 {
		t.Fatal("注册前并发上报丢失字段", len(center.instance.Meta))
	}
	report("after")
	if snapshot := n.Instance(); len(snapshot.Meta) != 129 || snapshot.Meta["zone"] != "one" {
		t.Fatal("读取或修改实例快照影响了节点元信息", snapshot.Meta)
	}
	center.registration.mutex.Lock()
	defer center.registration.mutex.Unlock()
	if len(center.registration.meta) != 129 {
		t.Fatal("注册后并发上报丢失字段", len(center.registration.meta))
	}
}

func TestReportMetaFailureAndStop(t *testing.T) {
	center := newMetaRegistry()
	n := nodeApp(t, center)
	ctx := context.Background()
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	center.registration.fail.Store(true)
	if err := n.ReportMeta(ctx, map[string]string{"failed": "value"}); err == nil {
		t.Fatal("上报失败未返回错误")
	}
	center.registration.fail.Store(false)
	if err := n.ReportMeta(ctx, map[string]string{"success": "value"}); err != nil {
		t.Fatal(err)
	}
	if meta := <-center.registration.updates; meta["failed"] != "" || meta["success"] != "value" {
		t.Fatal("失败的上报修改了本地元信息", meta)
	}
	n.RequestStop()
	if err := n.ReportMeta(ctx, map[string]string{"late": "value"}); !errors.Is(err, app.ErrClosed) {
		t.Fatal("停止后仍接受上报", err)
	}
}

type loadActor struct{}

func (*loadActor) OnStart(actor.StartContext) error                              { return nil }
func (*loadActor) OnUpdate(actor.UpdateContext) time.Duration                    { return -1 }
func (*loadActor) OnStop(actor.StopContext)                                      {}
func (*loadActor) OnMessage(actor.MessageContext, actor.Message) ([]byte, error) { return nil, nil }

func TestActorReportsLoadThroughNode(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = redisClient.Close() })
	center := newMetaRegistry()
	n := nodeApp(t, center)
	system := actor.New(actor.NewGuard("node", redisClient), actor.WithMaxActors(100))
	reporter := actor.NewReporter(system, n.ReportMeta,
		actor.WithResourceCapacity(1024, 4<<30),
		actor.WithLoadInterval(5*time.Millisecond),
	)
	system.Register("room", func(context.Context, actor.Key) (actor.Actor, error) { return &loadActor{}, nil })
	system.RegisterRPC(n.RPCServer())
	n.AddComponent(system)
	n.AddComponent(reporter)
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	var first actor.NodeLoad
	if err := json.Unmarshal([]byte(center.instance.Meta[actor.NodeLoadMetaKey]), &first); err != nil {
		t.Fatal(err)
	}
	if first.ActorCount != 0 || first.ActorLimit != 100 || first.SampledAt.IsZero() || first.MemoryUsage <= 0 {
		t.Fatal("首次注册没有 Actor 负载", first)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := system.Spawn(ctx, actor.Key{Type: "room", Id: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := n.ReportMeta(ctx, map[string]string{"zone": "two"}); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case meta := <-center.registration.updates:
			var load actor.NodeLoad
			if err := json.Unmarshal([]byte(meta[actor.NodeLoadMetaKey]), &load); err != nil {
				t.Fatal(err)
			}
			if load.ActorCount != 1 || meta["zone"] != "two" {
				continue
			}
			if !load.SampledAt.After(first.SampledAt) || load.CPUUsage < 0 || load.MemoryUsage <= 0 {
				t.Fatal("周期负载无效", load)
			}
		case <-ctx.Done():
			t.Fatal("Actor 没有通过 Node 上报新负载", ctx.Err())
		}
		break
	}
	if err := n.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	calls := center.registration.calls.Load()
	select {
	case <-time.After(20 * time.Millisecond):
		if center.registration.calls.Load() != calls {
			t.Fatal("Actor 停止后仍在上报")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestReporterStartupFailureIsHandledByNode(t *testing.T) {
	redisServer := miniredis.RunT(t)
	redisClient := redis.NewClient(&redis.Options{Addr: redisServer.Addr(), MaxRetries: -1})
	t.Cleanup(func() { _ = redisClient.Close() })
	center := newMetaRegistry()
	n := nodeApp(t, center)
	system := actor.New(actor.NewGuard("node", redisClient))
	failed := errors.New("初始负载上报失败")
	reporter := actor.NewReporter(system, func(context.Context, map[string]string) error { return failed }, actor.WithResourceCapacity(1024, 4<<30))
	n.AddComponent(system)
	n.AddComponent(reporter)
	if err := n.Start(); !errors.Is(err, failed) {
		t.Fatal(err)
	}
	if _, err := system.Spawn(context.Background(), actor.Key{Type: "player", Id: "1"}); !errors.Is(err, actor.ErrStopped) {
		t.Fatal("Node 启动失败后没有释放已启动的 System", err)
	}
	if center.instance.Id != "" {
		t.Fatal("组件启动失败后仍注册了节点")
	}
	if err := reporter.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
