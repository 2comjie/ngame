package packet

import "github.com/2comjie/ntool/util/buffer"

// Type 表示客户端与服务器之间的消息类型。
type Type uint16

const (
	Req     Type = 1
	Rsp     Type = 2
	Push    Type = 3
	Ping    Type = 4
	Pong    Type = 5
	BindReq Type = 6
	BindRsp Type = 7
)

// Header 包含固定协议字段和可扩展的键值字段。Values 的键统一使用小写。
type Header struct {
	Type   Type
	Route  uint32
	Seq    uint64
	Values map[string]string
}

// Message 是业务层消息。Read 返回的消息使用完后必须调用 Release。
// Body 引用池化内存，需要跨消息生命周期保存时应先复制。
type Message struct {
	Header Header
	Body   []byte

	buf *buffer.LinkBuffer
}

// Release 释放 Read 分配的整个 buffer。调用后不能再使用原来的 Body。
// Message 不得按值复制后分别释放，也不得在使用 Body 时并发释放。
func (m *Message) Release() {
	if m.buf == nil {
		return
	}
	_ = m.buf.Close()
	m.buf = nil
	m.Body = nil
}
