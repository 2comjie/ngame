package node

import (
	"context"
	"errors"
	"strings"

	"github.com/2comjie/ngame/app"
	nodepb "github.com/2comjie/ngame/node/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HandlerFunc 在当前 RPC 中同步处理请求；并发 RPC 会并发调用 Handler。
// 业务自行决定分发、串行化及进一步转发，请求只在回调期间有效。
// 成功必须返回 CallResponse，Replied=false 表示无需客户端业务回包。
type HandlerFunc func(context.Context, *nodepb.CallRequest) (*nodepb.CallResponse, error)

// RequestComponent 按需接入网关转发请求
type RequestComponent struct {
	nodepb.UnimplementedNodeServer
	node    *App
	handler HandlerFunc
}

var _ app.Component = (*RequestComponent)(nil)

// NewRequestComponent 在 App 构造后添加，随组件启动注册 RPC
func NewRequestComponent(node *App, handler HandlerFunc) *RequestComponent {
	if node == nil || handler == nil {
		panic("node: 请求组件必须提供 App 和 Handler")
	}
	return &RequestComponent{
		node:    node,
		handler: handler,
	}
}

func (r *RequestComponent) Start() error {
	nodepb.RegisterNodeServer(r.node.RPCServer(), r)
	return nil
}

func (*RequestComponent) RequestStop()                   {}
func (*RequestComponent) Shutdown(context.Context) error { return nil }

func (r *RequestComponent) Call(ctx context.Context, request *nodepb.CallRequest) (*nodepb.CallResponse, error) {
	nodeCtx := r.node.Context()
	if nodeCtx.Err() != nil {
		return nil, status.Error(codes.Unavailable, "node: 节点正在停止")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request.Uid == 0 || request.GateId == "" || request.Route == 0 {
		return nil, status.Error(codes.InvalidArgument, "node: 必须提供 Uid、GateId 和 Route")
	}
	for key := range request.Values {
		if key == "" || key != strings.ToLower(key) {
			return nil, status.Error(codes.InvalidArgument, "node: Header 键必须非空且为小写")
		}
	}
	callCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(nodeCtx, cancel)
	defer stop()
	defer cancel()
	if err := callCtx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	response, err := r.handler(callCtx, request)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		return nil, err
	}
	return response, nil
}
