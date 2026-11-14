package actor

import (
	"context"
	"errors"

	actorpb "github.com/2comjie/ngame/actor/internal/protocol"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type rpcServer struct {
	actorpb.UnimplementedActorServer
	system *System
}

// RegisterRPC 在启动前注册，可与 Node 共用 Server
// Guard、服务注册和 RPC Server 使用同一实例 Id，重启须更换 Id
func (s *System) RegisterRPC(server grpc.ServiceRegistrar) {
	actorpb.RegisterActorServer(server, &rpcServer{system: s})
}

func (server *rpcServer) Activate(ctx context.Context, request *actorpb.ActivateRequest) (*actorpb.ActivateResponse, error) {
	key := Key{Type: request.Type, Id: request.Id}
	for {
		r, err := server.system.spawn(ctx, key)
		if owned, ok := errors.AsType[*OwnedError](err); ok {
			return &actorpb.ActivateResponse{
				Code:         actorpb.ActivationCode_ACTIVATION_OWNED,
				NodeId:       owned.Owner.NodeId,
				ActivationId: owned.Owner.ActivationId,
			}, nil
		}
		if errors.Is(err, ErrStopping) {
			// 等待 OnStop 完成并释放租约后再激活
			select {
			case <-r.done:
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			continue
		}
		if err != nil {
			return nil, actorRPCError(err)
		}
		return &actorpb.ActivateResponse{
			Code:         actorpb.ActivationCode_ACTIVATION_READY,
			NodeId:       server.system.guard.nodeId,
			ActivationId: r.activationId,
		}, nil
	}
}

func (server *rpcServer) Ask(ctx context.Context, request *actorpb.CallRequest) (*actorpb.CallResponse, error) {
	return server.call(ctx, request, true)
}

func (server *rpcServer) Tell(ctx context.Context, request *actorpb.CallRequest) (*actorpb.CallResponse, error) {
	return server.call(ctx, request, false)
}

func (server *rpcServer) call(ctx context.Context, request *actorpb.CallRequest, ask bool) (*actorpb.CallResponse, error) {
	if request.ActivationId == "" {
		return nil, status.Error(codes.InvalidArgument, "actor: 必须提供目标 ActivationId")
	}
	var reply chan messageResult
	if ask {
		reply = make(chan messageResult, 1)
	}
	r, err := server.system.enqueue(ctx, Key{Type: request.Type, Id: request.Id}, Message{
		Method: request.Method,
		Body:   request.Body,
	}, reply, request.ActivationId)
	if errors.Is(err, errActorNotHere) {
		return &actorpb.CallResponse{Code: actorpb.CallCode_CALL_NOT_HERE}, nil
	}
	if err != nil {
		return nil, actorRPCError(err)
	}
	var body []byte
	if ask {
		body, err = r.waitResult(ctx, reply)
		if err != nil {
			// 已入队的错误不能转换成允许重试的 NOT_HERE
			return nil, actorRPCError(err)
		}
	}
	return &actorpb.CallResponse{Body: body}, nil
}

func actorRPCError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, ErrInvalidKey):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrEntityNotFound), errors.Is(err, ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, ErrTypeNotRegistered):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ErrMailboxFull):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, ErrGuardLost), errors.Is(err, ErrStopping), errors.Is(err, ErrStopped), errors.Is(err, ErrNotStarted):
		return status.Error(codes.Unavailable, err.Error())
	default:
		return status.Convert(err).Err()
	}
}
