package rpc

import (
	"github.com/2comjie/ngame/endpoint"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/endpointsharding"
	"google.golang.org/grpc/balancer/pickfirst"
	"google.golang.org/grpc/connectivity"
)

const balancerName = "ngame"

func init() {
	balancer.Register(balancerBuilder{})
}

type balancerBuilder struct{}

func (balancerBuilder) Name() string { return balancerName }

func (balancerBuilder) Build(cc balancer.ClientConn, opts balancer.BuildOptions) balancer.Balancer {
	// gRPC 负责实例连接的建立、重连、地址更新和优雅关闭。
	return endpointsharding.NewBalancer(
		&routingClientConn{ClientConn: cc}, opts,
		balancer.Get(pickfirst.Name).Build, endpointsharding.Options{},
	)
}

type routingClientConn struct {
	balancer.ClientConn
}

func (c *routingClientConn) UpdateState(state balancer.State) {
	children := endpointsharding.ChildStatesFromPicker(state.Picker)
	picker := &routingPicker{instances: make(map[string]instanceTarget, len(children))}
	for _, child := range children {
		info := child.Endpoint.Attributes.Value(instanceKey{}).(instanceInfo)
		picker.instances[info.Id] = instanceTarget{info: info, picker: child.State.Picker}
		if info.Status != endpoint.Working || info.Weight == 0 {
			continue
		}
		switch child.State.ConnectivityState {
		case connectivity.Ready:
			weight := float64(info.Weight)
			picker.ready = append(picker.ready, weightedTarget{picker: child.State.Picker, weight: weight})
			picker.totalWeight += weight
		case connectivity.Idle, connectivity.Connecting:
			picker.connecting = true
		}
	}
	state.Picker = picker
	c.ClientConn.UpdateState(state)
}
