package network

import (
	"context"
	"maps"
	"sync/atomic"

	"github.com/2comjie/ngame/packet"
)

// ReqContext 和 Request 在 OnReq 回调期间有效，异步处理时应自行复制消息。
type ReqContext struct {
	context.Context
	Session *Session
	Request *packet.Message

	options options
	written atomic.Bool
}

func (c *ReqContext) NeedReply() bool { return c.Request.Header.Seq != 0 }

// Write 使用请求的 Route、Seq 构建响应，response 只需提供 Header.Values 和 Body。
func (c *ReqContext) Write(response *packet.Message) error {
	if !c.NeedReply() {
		return nil
	}
	message := &packet.Message{Header: packet.Header{
		Type: packet.Rsp, Route: c.Request.Header.Route, Seq: c.Request.Header.Seq, Values: maps.Clone(response.Header.Values),
	}, Body: response.Body}
	if err := encodeBody(c.options, message); err != nil {
		return err
	}
	if !c.written.CompareAndSwap(false, true) {
		return ErrResponseWritten
	}
	// 允许业务在处理超时后返回错误响应，写入仍受连接生命周期和传输层写超时限制。
	if err := c.Session.Conn.WriteContext(c.Session.ctx, message); err != nil {
		_ = c.Session.Conn.Close()
		return err
	}
	return nil
}
