package gate

import (
	"context"

	"github.com/2comjie/ngame/network"
)

// Request 仅在 Forward 调用期间有效；异步保留时复制 Values、Body。
type Request struct {
	Uid       uint64
	GateId    string
	SessionId uint64
	NeedReply bool
	Route     uint32
	Values    map[string]string
	Body      []byte
}

// Response 成功返回后交由 App 消费，上层不得继续修改。
type Response struct {
	Replied bool
	Values  map[string]string
	Body    []byte
}

// ForwardFunc 成功必须返回 Response；Replied=false 表示无业务回包。
type ForwardFunc func(context.Context, *Request) (*Response, error)

// ErrorHandler 由上层决定客户端错误格式；写入失败时应关闭对应连接。
type ErrorHandler func(*network.ReqContext, error)
