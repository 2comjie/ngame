package rpc

import (
	"net/url"

	"github.com/2comjie/ngame/registry"
	"google.golang.org/grpc"
)

func NewClient(discover registry.Discover, service string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if service == "" {
		panic("rpc: 必须提供服务名")
	}
	dialOpts := []grpc.DialOption{
		grpc.WithResolvers(NewResolver(discover)),
	}
	dialOpts = append(dialOpts, opts...)
	// gRPC 使用第一个匹配的 Resolver，默认服务配置则以最后一个选项为准。
	dialOpts = append(dialOpts, grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"ngame":{}}]}`))
	return grpc.NewClient("ngame:///"+url.PathEscape(service), dialOpts...)
}
