package rpc

import (
	"context"
	"errors"
	"time"

	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ngame/registry"
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
)

// NewResolver 将服务发现接入 gRPC，通过 grpc.WithResolvers 使用。
// 目标格式为 ngame:///服务名；不修改 gRPC 的全局 resolver 注册表。
func NewResolver(discover registry.Discover) resolver.Builder {
	if discover == nil {
		panic("rpc: 必须提供 Discover")
	}
	return &resolverBuilder{discover: discover}
}

type resolverBuilder struct {
	discover registry.Discover
}

func (*resolverBuilder) Scheme() string { return "ngame" }

func (b *resolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	name := target.Endpoint()
	if name == "" {
		return nil, errors.New("rpc: 解析目标必须包含服务名")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &discoveryResolver{cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(r.done)
		for {
			err := b.discover.Watch(ctx, name, func(instances []endpoint.ServiceInst) {
				if ctx.Err() != nil {
					return
				}
				endpoints := make([]resolver.Endpoint, 0, len(instances))
				for _, instance := range instances {
					endpoints = append(endpoints, resolver.Endpoint{
						Addresses: []resolver.Address{{Addr: instance.RpcTarget()}},
						Attributes: attributes.New(instanceKey{}, instanceInfo{
							Id: instance.Id, Status: instance.Status, Weight: instance.Weight,
						}),
					})
				}
				// Watch 会在快照变化时再次更新，无需因返回错误立即重读同一份列表。
				// 空列表也必须提交，使 gRPC 感知实例已全部下线。
				_ = cc.UpdateState(resolver.State{Endpoints: endpoints})
			})
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				cc.ReportError(err)
			}
			// Discover.Watch 将无法恢复的错误直接返回；由解析器延迟重建订阅。
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return r, nil
}

type discoveryResolver struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// Watch 主动推送地址变化，忽略额外的刷新提示，避免并发创建重复订阅。
func (*discoveryResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *discoveryResolver) Close() {
	r.cancel()
	<-r.done
}

type instanceKey struct{}

// 仅保存选节点所需的可比较字段，满足 gRPC Attributes 的相等比较约定。
type instanceInfo struct {
	Id     string
	Status endpoint.Status
	Weight int
}
