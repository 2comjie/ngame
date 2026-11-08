package rpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const instanceHeader = "ngame-instance-id"

func NewServer(instanceId string, opts ...grpc.ServerOption) *grpc.Server {
	if instanceId == "" {
		panic("rpc: 必须提供服务实例 Id")
	}
	serverOpts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := verifyInstance(ctx, instanceId); err != nil {
				return nil, err
			}
			return handler(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := verifyInstance(stream.Context(), instanceId); err != nil {
				return err
			}
			return handler(srv, stream)
		}),
	}
	serverOpts = append(serverOpts, opts...)
	return grpc.NewServer(serverOpts...)
}

func verifyInstance(ctx context.Context, instanceId string) error {
	values := metadata.ValueFromIncomingContext(ctx, instanceHeader)
	if len(values) == 0 {
		return nil
	}
	if len(values) != 1 || values[0] != instanceId {
		return status.Error(codes.Unavailable, "rpc: 目标实例已变化，请重新确认节点归属")
	}
	return nil
}
