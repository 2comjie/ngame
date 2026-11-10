package node

import (
	"context"
	"errors"
	"strings"

	nodepb "github.com/2comjie/ngame/node/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandlerFunc 在当前 RPC 中同步处理请求；并发 RPC 会并发调用 Handler。
// 业务自行决定分发、串行化及进一步转发，请求只在回调期间有效。
// 成功必须返回 CallResponse，Replied=false 表示无需客户端业务回包。
type HandlerFunc func(context.Context, *nodepb.CallRequest) (*nodepb.CallResponse, error)

func (n *App) Call(ctx context.Context, request *nodepb.CallRequest) (*nodepb.CallResponse, error) {
	if n.ctx.Err() != nil {
		return nil, status.Error(codes.Unavailable, "node: 节点正在停止")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request.Uid == 0 || request.GateId == "" || request.SessionId == 0 || request.Route == 0 {
		return nil, status.Error(codes.InvalidArgument, "node: 必须提供 Uid、GateId、SessionId 和 Route")
	}
	for key := range request.Values {
		if key == "" || key != strings.ToLower(key) {
			return nil, status.Error(codes.InvalidArgument, "node: Header 键必须非空且为小写")
		}
	}
	callCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(n.ctx, cancel)
	defer stop()
	defer cancel()
	if err := callCtx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	response, err := n.handler(callCtx, request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, err
	}
	return response, nil
}
