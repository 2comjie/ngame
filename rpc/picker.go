package rpc

import (
	"math/rand/v2"

	"github.com/2comjie/ngame/endpoint"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type instanceTarget struct {
	info   instanceInfo
	picker balancer.Picker
}

type weightedTarget struct {
	picker balancer.Picker
	weight float64
}

type routingPicker struct {
	instances   map[string]instanceTarget
	ready       []weightedTarget
	totalWeight float64
	connecting  bool
}

func (p *routingPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if instanceId, ok := info.Ctx.Value(instanceContextKey{}).(string); ok {
		target, exists := p.instances[instanceId]
		if !exists {
			return balancer.PickResult{}, status.Errorf(codes.Unavailable, "rpc: 目标实例 %q 不存在", instanceId)
		}
		if target.info.Status == endpoint.Shutdown {
			return balancer.PickResult{}, status.Errorf(codes.Unavailable, "rpc: 目标实例 %q 正在退出", instanceId)
		}
		// 显式定向允许 Hung 和零权重实例，连接就绪及失败语义交给其子 Picker。
		return target.picker.Pick(info)
	}
	if len(p.ready) == 0 {
		if p.connecting {
			return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
		}
		return balancer.PickResult{}, status.Error(codes.Unavailable, "rpc: 没有可用的负载均衡实例")
	}
	// 按权重随机选择；最后一段承接剩余区间，避免维护共享轮询游标。
	choice := rand.Float64() * p.totalWeight
	for _, target := range p.ready[:len(p.ready)-1] {
		if choice < target.weight {
			return target.picker.Pick(info)
		}
		choice -= target.weight
	}
	return p.ready[len(p.ready)-1].picker.Pick(info)
}
