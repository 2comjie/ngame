package rpc

import (
	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ntool/logx"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/connectivity"
)

const balancerName = "ngame"

func init() {
	balancer.Register(balancerBuilder{})
}

type balancerBuilder struct{}

func (balancerBuilder) Name() string { return balancerName }

func (balancerBuilder) Build(cc balancer.ClientConn, _ balancer.BuildOptions) balancer.Balancer {
	return &instanceBalancer{cc: cc, instances: make(map[string]*instanceConn)}
}

type instanceConn struct {
	info    instanceInfo
	address string
	conn    balancer.SubConn
	state   connectivity.State
}

// gRPC 将配置更新、StateListener 和 Close 串行调度；Pick 只读取单独生成的快照。
type instanceBalancer struct {
	cc        balancer.ClientConn
	instances map[string]*instanceConn
}

func (b *instanceBalancer) UpdateClientConnState(state balancer.ClientConnState) error {
	next := make(map[string]*instanceConn, len(state.ResolverState.Endpoints))
	var created []*instanceConn
	for _, ep := range state.ResolverState.Endpoints {
		info := ep.Attributes.Value(instanceKey{}).(instanceInfo)
		address := ep.Addresses[0].Addr
		entry := b.instances[info.Id]
		if entry == nil || entry.address != address {
			entry = &instanceConn{info: info, address: address, state: connectivity.Idle}
			conn, err := b.cc.NewSubConn(ep.Addresses, balancer.NewSubConnOptions{
				StateListener: func(update balancer.SubConnState) { b.updateConnection(entry, update) },
			})
			if err != nil {
				for _, entry := range created {
					entry.conn.Shutdown()
				}
				return err
			}
			entry.conn = conn
			created = append(created, entry)
		}
		next[info.Id] = entry
	}
	for _, ep := range state.ResolverState.Endpoints {
		info := ep.Attributes.Value(instanceKey{}).(instanceInfo)
		next[info.Id].info = info
	}
	previous := b.instances
	b.instances = next
	b.publish()
	for id, entry := range previous {
		if next[id] != entry {
			entry.conn.Shutdown()
		}
	}
	for _, entry := range created {
		entry.conn.Connect()
	}
	return nil
}

func (b *instanceBalancer) updateConnection(entry *instanceConn, update balancer.SubConnState) {
	// 已被移除或替换的连接仍可能产生关闭回调，不能覆盖新连接的状态。
	if b.instances[entry.info.Id] != entry {
		return
	}
	entry.state = update.ConnectivityState
	if entry.state == connectivity.Idle {
		entry.conn.Connect()
	}
	if entry.state == connectivity.TransientFailure {
		logx.Debugf("rpc: 实例连接失败 id=%s error=%v", entry.info.Id, update.ConnectionError)
	}
	b.publish()
}

func (b *instanceBalancer) ResolverError(err error) {
	logx.Warnf("rpc: 服务发现失败 error=%v", err)
	// 暂时保留已有连接，Resolver 会重建订阅；不缓存或跨层保存错误。
	b.publish()
}

// 使用 StateListener 接收连接状态，不使用已弃用的回调入口。
func (*instanceBalancer) UpdateSubConnState(balancer.SubConn, balancer.SubConnState) {}

func (b *instanceBalancer) ExitIdle() {
	for _, entry := range b.instances {
		if entry.state == connectivity.Idle {
			entry.conn.Connect()
		}
	}
}

func (b *instanceBalancer) Close() {
	previous := b.instances
	b.instances = nil
	for _, entry := range previous {
		entry.conn.Shutdown()
	}
}

func (b *instanceBalancer) publish() {
	picker := &routingPicker{instances: make(map[string]instanceTarget, len(b.instances))}
	state := connectivity.TransientFailure
	for id, entry := range b.instances {
		info := entry.info
		picker.instances[id] = instanceTarget{info: info, conn: entry.conn, state: entry.state}
		if entry.state == connectivity.Ready {
			state = connectivity.Ready
		} else if state != connectivity.Ready && (entry.state == connectivity.Idle || entry.state == connectivity.Connecting) {
			state = connectivity.Connecting
		}
		if info.Status != endpoint.Working || info.Weight == 0 {
			continue
		}
		switch entry.state {
		case connectivity.Ready:
			weight := float64(info.Weight)
			picker.ready = append(picker.ready, weightedTarget{id: id, conn: entry.conn, weight: weight})
			picker.totalWeight += weight
		case connectivity.Idle, connectivity.Connecting:
			picker.connecting = true
		}
	}
	b.cc.UpdateState(balancer.State{ConnectivityState: state, Picker: picker})
}
