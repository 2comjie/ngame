package actor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ngame/registry"
)

func placementInstance(t *testing.T, id string, load NodeLoad) endpoint.ServiceInst {
	t.Helper()
	value, err := json.Marshal(load)
	if err != nil {
		t.Fatal(err)
	}
	return endpoint.ServiceInst{
		Id:      id,
		Name:    "game",
		RpcHost: "127.0.0.1",
		RpcPort: 9001,
		Weight:  1,
		Meta:    map[string]string{NodeLoadMetaKey: string(value)},
	}
}

func placementDiscover(t *testing.T, instances ...endpoint.ServiceInst) registry.Discover {
	t.Helper()
	discover := registry.NewDiscover(func(ctx context.Context, publish func([]endpoint.ServiceInst)) error {
		publish(instances)
		<-ctx.Done()
		return ctx.Err()
	})
	t.Cleanup(discover.Close)
	return discover
}

func TestPlacementUsesActorCPUAndMemory(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    NodeLoad
		b    NodeLoad
	}{
		{"actor-count", NodeLoad{ActorCount: 80, ActorLimit: 100}, NodeLoad{ActorCount: 20, ActorLimit: 100}},
		{"actor-capacity", NodeLoad{ActorCount: 10, ActorLimit: 20}, NodeLoad{ActorCount: 20, ActorLimit: 100}},
		{"cpu", NodeLoad{ActorCount: 1, ActorLimit: 100, CPUUsage: 0.8}, NodeLoad{ActorCount: 40, ActorLimit: 100, CPUUsage: 0.2}},
		{"memory", NodeLoad{ActorCount: 1, ActorLimit: 100, MemoryUsage: 0.9}, NodeLoad{ActorCount: 40, ActorLimit: 100, MemoryUsage: 0.2}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tc.a.SampledAt = time.Now()
			tc.b.SampledAt = time.Now()
			_, redisClient := loaderRedis(t)
			a := placementInstance(t, "a", tc.a)
			b := placementInstance(t, "b", tc.b)
			caller := actorClient(t, redisClient, placementDiscover(t, a, b), map[string]string{"room": "game"})
			for range 32 {
				nodeId, err := caller.selectNode(context.Background(), "room", nil)
				if err != nil || nodeId != "b" {
					t.Fatalf("未选择压力较低的节点: id=%s error=%v", nodeId, err)
				}
			}
		})
	}
}

func TestPlacementExcludesUnavailableLoads(t *testing.T) {
	for _, mode := range []string{"missing", "corrupt", "stale", "invalid-limit", "negative-count", "negative-cpu", "negative-memory", "full-count", "full-cpu", "full-memory", "hung", "shutdown", "zero-weight", "rejected", "other-service"} {
		t.Run(mode, func(t *testing.T) {
			load := NodeLoad{ActorLimit: 100, SampledAt: time.Now()}
			switch mode {
			case "stale":
				load.SampledAt = time.Now().Add(-time.Minute)
			case "invalid-limit":
				load.ActorLimit = 0
			case "negative-count":
				load.ActorCount = -1
			case "negative-cpu":
				load.CPUUsage = -1
			case "negative-memory":
				load.MemoryUsage = -1
			case "full-count":
				load.ActorCount = 100
			case "full-cpu":
				load.CPUUsage = 1
			case "full-memory":
				load.MemoryUsage = 1
			}
			instance := placementInstance(t, "unavailable", load)
			switch mode {
			case "missing":
				instance.Meta = nil
			case "corrupt":
				instance.Meta[NodeLoadMetaKey] = "{"
			case "hung":
				instance.Status = endpoint.Hung
			case "shutdown":
				instance.Status = endpoint.Shutdown
			case "zero-weight":
				instance.Weight = 0
			case "other-service":
				instance.Name = "player"
			}
			_, redisClient := loaderRedis(t)
			caller := actorClient(t, redisClient, placementDiscover(t, instance), map[string]string{"room": "game"})
			var rejected map[string]bool
			if mode == "rejected" {
				rejected = map[string]bool{instance.Id: true}
			}
			if _, err := caller.selectNode(context.Background(), "room", rejected); !errors.Is(err, ErrNoAvailableNode) {
				t.Fatal("无可用负载未返回无候选节点", err)
			}
		})
	}
}

func TestPlacementConcurrentUsesDiscoveryCache(t *testing.T) {
	_, redisClient := loaderRedis(t)
	var reads atomic.Int32
	instances := []endpoint.ServiceInst{
		placementInstance(t, "a", NodeLoad{ActorLimit: 100, CPUUsage: 0.8, SampledAt: time.Now()}),
		placementInstance(t, "b", NodeLoad{ActorLimit: 100, CPUUsage: 0.2, SampledAt: time.Now()}),
	}
	discover := registry.NewDiscover(func(ctx context.Context, publish func([]endpoint.ServiceInst)) error {
		reads.Add(1)
		publish(instances)
		<-ctx.Done()
		return ctx.Err()
	})
	t.Cleanup(discover.Close)
	caller := actorClient(t, redisClient, discover, map[string]string{"room": "game"})
	var wait sync.WaitGroup
	for range 256 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if id, err := caller.selectNode(context.Background(), "room", nil); err != nil || id != "b" {
				t.Errorf("id=%s error=%v", id, err)
			}
		}()
	}
	wait.Wait()
	if reads.Load() != 1 {
		t.Fatalf("选点重复读取后端: reads=%d", reads.Load())
	}
}

func TestPlacementConcurrentActivationHonorsLimits(t *testing.T) {
	_, redisClient := loaderRedis(t)
	const requests = 128
	entered := make(chan int, requests)
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	var active [2]atomic.Int32
	nodes := make([]*actorRPCNode, 0, 2)
	for index, usage := range []float64{0.1, 0.6} {
		id := fmt.Sprintf("game-%d", index)
		system := New(NewGuard(id, redisClient), WithMaxActors(4), WithMaxStarting(2))
		system.Register("room", func(ctx context.Context, key Key) (Actor, error) {
			current := active[index].Add(1)
			defer active[index].Add(-1)
			if current > 2 || system.Stats().ActorCount > 4 {
				t.Errorf("节点突破上限 node=%s starting=%d", id, current)
			}
			entered <- index
			select {
			case <-resume:
				return &loadedActor{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})
		node := startActorRPCNode(t, system, "game")
		// 初始快照保持空闲，模拟并发请求到达时上报尚未更新
		node.instance.Meta = placementInstance(t, id, NodeLoad{ActorLimit: 4, CPUUsage: usage, SampledAt: time.Now()}).Meta
		nodes = append(nodes, node)
	}
	discover := actorDiscover(t, nodes...)
	callers := make([]*Client, 4)
	for index := range callers {
		callers[index] = actorClient(t, redisClient, discover, map[string]string{"room": "game"})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results := make(chan error, requests)
	start := make(chan struct{})
	for index := range requests {
		go func() {
			<-start
			_, err := callers[index%len(callers)].activate(ctx, Key{Type: "room", Id: fmt.Sprint(index)}, Owner{})
			results <- err
		}()
	}
	close(start)
	var admitted [2]int
	for range 4 {
		select {
		case index := <-entered:
			admitted[index]++
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	for range requests - 4 {
		select {
		case err := <-results:
			if !errors.Is(err, ErrCapacity) {
				t.Fatal("超额激活未返回容量不足", err)
			}
		case <-ctx.Done():
			t.Fatal("超额激活没有及时拒绝", ctx.Err())
		}
	}
	if admitted != [2]int{2, 2} {
		t.Fatal("未在两个节点执行启动限额", admitted)
	}
	unblock()
	for range 4 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	for index := range 4 {
		if _, err := callers[0].activate(ctx, Key{Type: "room", Id: fmt.Sprintf("next-%d", index)}, Owner{}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := callers[0].activate(ctx, Key{Type: "room", Id: "overflow"}, Owner{}); !errors.Is(err, ErrCapacity) {
		t.Fatal("总数满额后仍允许激活", err)
	}
	for _, node := range nodes {
		if load := node.system.Stats(); load.ActorCount != 4 {
			t.Fatal("节点总数未受限制", load)
		}
	}
}

func TestRefLoadPlacementKeepsExistingOwner(t *testing.T) {
	_, redisClient := loaderRedis(t)
	loader := func(id string) Loader {
		return func(context.Context, Key) (Actor, error) {
			return &messageActor{onMessage: func(MessageContext, Message) ([]byte, error) { return []byte(id), nil }}, nil
		}
	}
	a := newActorRPCNode(t, redisClient, "a", "game", map[string]Loader{"room": loader("a")})
	b := newActorRPCNode(t, redisClient, "b", "game", map[string]Loader{"room": loader("b")})
	first := placementInstance(t, "a", NodeLoad{ActorLimit: 100, CPUUsage: 0.1, SampledAt: time.Now()})
	second := placementInstance(t, "b", NodeLoad{ActorLimit: 100, CPUUsage: 0.8, SampledAt: time.Now()})
	first.RpcPort, second.RpcPort = a.instance.RpcPort, b.instance.RpcPort
	updates := make(chan []endpoint.ServiceInst, 1)
	discover := registry.NewDiscover(func(ctx context.Context, publish func([]endpoint.ServiceInst)) error {
		publish([]endpoint.ServiceInst{first, second})
		for {
			select {
			case values := <-updates:
				publish(values)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})
	t.Cleanup(discover.Close)
	caller := actorClient(t, redisClient, discover, map[string]string{"room": "game"})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ref := caller.Ref(Key{Type: "room", Id: "existing"})
	if body, err := ref.Ask(ctx, Message{}, ActivateIfMissing); err != nil || string(body) != "a" {
		t.Fatalf("初次激活未按负载选点: body=%s error=%v", body, err)
	}
	updatedA := placementInstance(t, "a", NodeLoad{ActorLimit: 100, CPUUsage: 1, SampledAt: time.Now()})
	updatedB := placementInstance(t, "b", NodeLoad{ActorLimit: 100, CPUUsage: 0.1, SampledAt: time.Now()})
	updatedA.RpcPort, updatedB.RpcPort = a.instance.RpcPort, b.instance.RpcPort
	updates <- []endpoint.ServiceInst{updatedA, updatedB}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		instance, _, err := discover.Get(ctx, "a")
		if err != nil {
			t.Fatal(err)
		}
		if instance.Meta[NodeLoadMetaKey] == updatedA.Meta[NodeLoadMetaKey] {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
		}
	}
	if body, err := ref.Ask(ctx, Message{}, ExistingOnly); err != nil || string(body) != "a" {
		t.Fatalf("负载变化干扰了已有归属: body=%s error=%v", body, err)
	}
	if body, err := caller.Ref(Key{Type: "room", Id: "new"}).Ask(ctx, Message{}, ActivateIfMissing); err != nil || string(body) != "b" {
		t.Fatalf("新激活未采用最新负载: body=%s error=%v", body, err)
	}
}
