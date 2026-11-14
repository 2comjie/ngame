package gate

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	gatepb "github.com/2comjie/ngame/gate/protocol"
	"github.com/2comjie/ngame/network"
	"github.com/2comjie/ngame/packet"
	"github.com/2comjie/ntool/logx"
	"github.com/2comjie/ntool/util/gopool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (g *App) Push(ctx context.Context, request *gatepb.PushRequest) (*gatepb.PushResponse, error) {
	if g.ctx.Err() != nil {
		return nil, status.Error(codes.Unavailable, "gate: 网关正在停止")
	}
	if request.Uid == 0 {
		return nil, status.Error(codes.InvalidArgument, "gate: 必须提供 Uid")
	}
	err := g.server.PushUid(ctx, request.Uid, &packet.Message{
		Header: packet.Header{
			Route:  request.Route,
			Values: request.Values,
		},
		Body: request.Body,
	})
	if err == nil {
		return &gatepb.PushResponse{
			Code: gatepb.PushCode_PUSH_SENT,
		}, nil
	}
	if errors.Is(err, network.ErrNotBound) {
		return &gatepb.PushResponse{
			Code: gatepb.PushCode_PUSH_SESSION_NOT_HERE,
		}, nil
	}
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if errors.Is(err, network.ErrBodyTooLarge) || errors.Is(err, packet.ErrRoute) || errors.Is(err, packet.ErrHeader) || errors.Is(err, packet.ErrFrameLength) {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	logx.Warnf("gate: 推送结果未知 uid=%d error=%v", request.Uid, err)
	return &gatepb.PushResponse{
		Code: gatepb.PushCode_PUSH_DELIVERY_UNKNOWN,
	}, nil
}

func (g *App) MultiPush(ctx context.Context, request *gatepb.MultiPushRequest) (*gatepb.MultiPushResponse, error) {
	if g.ctx.Err() != nil {
		return nil, status.Error(codes.Unavailable, "gate: 网关正在停止")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := g.server.ValidatePush(&packet.Message{
		Header: packet.Header{
			Route:  request.Route,
			Values: request.Values,
		},
		Body: request.Body,
	}); err != nil {
		return nil, rpcError(err)
	}
	results := make([]*gatepb.PushResult, 0, len(request.Uids))
	seen := make(map[uint64]bool, len(request.Uids))
	for _, uid := range request.Uids {
		if uid == 0 {
			return nil, status.Error(codes.InvalidArgument, "gate: 必须提供有效 Uid")
		}
		if seen[uid] {
			continue
		}
		seen[uid] = true
		results = append(results, &gatepb.PushResult{
			Uid:  uid,
			Code: gatepb.PushCode_PUSH_DELIVERY_UNKNOWN,
		})
	}
	var next atomic.Int64
	var wait sync.WaitGroup
	for range min(16, len(results)) {
		wait.Add(1)
		gopool.CtxGo(ctx, func() {
			defer wait.Done()
			for ctx.Err() == nil {
				index := int(next.Add(1) - 1)
				if index >= len(results) {
					return
				}
				result := results[index]
				response, err := g.Push(ctx, &gatepb.PushRequest{
					Uid:    result.Uid,
					Route:  request.Route,
					Values: request.Values,
					Body:   request.Body,
				})
				if err != nil {
					if ctx.Err() == nil {
						logx.Warnf("gate: 批量推送结果未知 uid=%d error=%v", result.Uid, err)
					}
					continue
				}
				result.Code = response.Code
			}
		})
	}
	wait.Wait()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &gatepb.MultiPushResponse{
		Results: results,
	}, nil
}

func (g *App) Broadcast(ctx context.Context, request *gatepb.BroadcastRequest) (*gatepb.BroadcastResponse, error) {
	if g.ctx.Err() != nil {
		return nil, status.Error(codes.Unavailable, "gate: 网关正在停止")
	}
	message := &packet.Message{
		Header: packet.Header{
			Route:  request.Route,
			Values: request.Values,
		},
		Body: request.Body,
	}
	if err := g.server.ValidatePush(message); err != nil {
		return nil, rpcError(err)
	}
	targets, sent, err := g.server.BroadcastStats(ctx, message)
	if err != nil && !errors.Is(err, network.ErrPushFailed) {
		return nil, rpcError(err)
	}
	return &gatepb.BroadcastResponse{
		TargetCount: targets,
		SentCount:   sent,
		FailedCount: targets - sent,
	}, nil
}

func (g *App) Kick(ctx context.Context, request *gatepb.KickRequest) (*gatepb.KickResponse, error) {
	if g.ctx.Err() != nil {
		return nil, status.Error(codes.Unavailable, "gate: 网关正在停止")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request.Uid == 0 {
		return nil, status.Error(codes.InvalidArgument, "gate: 必须提供 Uid")
	}
	closed, err := g.server.KickUid(request.Uid)
	if err != nil {
		return nil, rpcError(err)
	}
	if !closed {
		return &gatepb.KickResponse{
			Code: gatepb.KickCode_KICK_SESSION_NOT_HERE,
		}, nil
	}
	return &gatepb.KickResponse{
		Code: gatepb.KickCode_KICK_CLOSED,
	}, nil
}

func (g *App) MockCall(ctx context.Context, request *gatepb.MockCallRequest) (*gatepb.MockCallResponse, error) {
	if g.ctx.Err() != nil {
		return nil, status.Error(codes.Unavailable, "gate: 网关正在停止")
	}
	if request.Uid == 0 {
		return nil, status.Error(codes.InvalidArgument, "gate: 必须提供 Uid")
	}
	var response *Response
	err := g.server.ExecuteSession(ctx, request.Uid, &packet.Message{
		Header: packet.Header{
			Type:   packet.Req,
			Route:  request.Route,
			Seq:    1,
			Values: request.Values,
		},
		Body: request.Body,
	}, func(callCtx context.Context, session *network.Session, message *packet.Message) error {
		var err error
		response, err = g.forward(callCtx, &Request{
			Uid:       session.Uid(),
			GateId:    g.instance.Id,
			NeedReply: true,
			Route:     message.Header.Route,
			Values:    message.Header.Values,
			Body:      message.Body,
		})
		if err != nil {
			// Forward 已经开始，不能把它返回的 ErrNotBound 当成允许重放的会话错误。
			return rpcError(err)
		}
		return nil
	})
	if errors.Is(err, network.ErrNotBound) {
		return &gatepb.MockCallResponse{
			Code: gatepb.MockCallCode_MOCK_CALL_SESSION_NOT_HERE,
		}, nil
	}
	if err != nil {
		return nil, rpcError(err)
	}
	return &gatepb.MockCallResponse{
		Code:    gatepb.MockCallCode_MOCK_CALL_OK,
		Replied: response.Replied,
		Values:  response.Values,
		Body:    response.Body,
	}, nil
}

func (g *App) SessionCount(ctx context.Context, _ *emptypb.Empty) (*gatepb.SessionCountResponse, error) {
	if g.ctx.Err() != nil {
		return nil, status.Error(codes.Unavailable, "gate: 网关正在停止")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &gatepb.SessionCountResponse{
		Count: g.server.SessionCount(),
	}, nil
}

func rpcError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, network.ErrClosed):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, network.ErrRequestQueueFull):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, network.ErrBodyTooLarge), errors.Is(err, packet.ErrRoute), errors.Is(err, packet.ErrHeader), errors.Is(err, packet.ErrType), errors.Is(err, packet.ErrFrameLength):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Convert(err).Err()
	}
}
