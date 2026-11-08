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
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"ngame":{}}]}`),
	}
	dialOpts = append(dialOpts, opts...)
	return grpc.NewClient("ngame:///"+url.PathEscape(service), dialOpts...)
}
