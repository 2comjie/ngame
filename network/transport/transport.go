package transport

import (
	"context"
	"errors"
	"net"

	"github.com/2comjie/ngame/packet"
)

type Type string

const (
	TypeTCP Type = "tcp"
	TypeKCP Type = "kcp"
	TypeWS  Type = "ws"
)

var (
	ErrClosed         = errors.New("network/transport: 连接已关闭")
	ErrStarted        = errors.New("network/transport: 连接已经启动")
	ErrWriteQueueFull = errors.New("network/transport: 写队列已满")
	ErrInsecure       = errors.New("network/transport: 连接未完成安全握手")
)

type Handler interface {
	// HandleMessage 在同一连接上串行调用，返回后由传输层 Release 消息。
	// 需要异步处理或保存消息时，调用方必须复制 Body 和 Header.Values。
	HandleMessage(Conn, *packet.Message)

	// HandleClose 在连接关闭后通知一次，可能与正在执行的 HandleMessage 并发。
	HandleClose(Conn)
}

// Conn 是消息连接。同一连接允许并发写入，完整消息之间不得交错。
type Conn interface {
	// Start 安装处理器并启动收发，每个连接只能调用一次。
	// handler 是必需依赖；重复调用返回 ErrStarted，连接已关闭返回 ErrClosed。
	Start(handler Handler) error

	// Write 使用连接默认的写超时，其他约定与 WriteContext 相同。
	Write(message *packet.Message) error

	// WriteContext 返回 nil 表示整包已写入底层连接，不表示远端已处理。
	// 调用期间不得修改消息；返回后传输层不再引用原消息及其 Header、Body。
	// 排队期间取消可以只取消本次写入；写入中取消或失败必须关闭连接，避免残留半包。
	WriteContext(ctx context.Context, message *packet.Message) error

	// Close 关闭连接并释放排队数据，允许重复调用。
	Close() error

	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	Type() Type

	// Secure 表示连接实际完成了TLS握手；本机明文连接返回false。
	// 代理终止TLS后，后端连接不会根据HTTP转发头被标记为安全连接。
	Secure() bool
}

type Listener interface {
	// Accept 返回已完成TLS握手的连接，或显式配置的本机明文连接。
	// 此时尚未进行玩家凭证认证，握手失败的底层连接由实现关闭。
	Accept() (Conn, error)
	Close() error
	Addr() net.Addr
}

type Dialer interface {
	// DialContext 在ctx限制内完成连接；TLS模式下还必须完成安全握手。
	// 失败时关闭已建立的底层连接；成功返回仍需由 network 执行凭证认证。
	DialContext(ctx context.Context) (Conn, error)
}

// IsLoopback 判断传输地址是否为本机回环地址，不解析域名或信任HTTP转发头。
func IsLoopback(addr net.Addr) bool {
	switch addr := addr.(type) {
	case *net.TCPAddr:
		return addr.IP.IsLoopback()
	case *net.UDPAddr:
		return addr.IP.IsLoopback()
	default:
		return false
	}
}
