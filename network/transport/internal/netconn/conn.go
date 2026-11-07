package netconn

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/2comjie/ngame/network/transport"
	"github.com/2comjie/ngame/packet"
)

type Conn struct {
	raw       *tls.Conn
	codec     *packet.Codec
	kind      transport.Type
	writeWait time.Duration
	writeGate chan struct{}
	done      chan struct{}

	mutex   sync.Mutex
	started bool
	closed  bool
	handler transport.Handler
}

func New(raw *tls.Conn, codec *packet.Codec, kind transport.Type, writeWait time.Duration) *Conn {
	if !raw.ConnectionState().HandshakeComplete {
		panic(transport.ErrInsecure)
	}
	return &Conn{
		raw: raw, codec: codec, kind: kind, writeWait: writeWait,
		writeGate: make(chan struct{}, 1), done: make(chan struct{}),
	}
}

func (c *Conn) Start(handler transport.Handler) error {
	if handler == nil {
		panic("network/transport: 必须提供Handler")
	}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.closed {
		return transport.ErrClosed
	}
	if c.started {
		return transport.ErrStarted
	}
	c.started = true
	c.handler = handler
	go c.readLoop()
	return nil
}

func (c *Conn) readLoop() {
	defer c.Close()
	for {
		message, err := c.codec.Read(c.raw)
		if err != nil {
			return
		}
		c.handler.HandleMessage(c, message)
		message.Release()
		select {
		case <-c.done:
			return
		default:
		}
	}
}

func (c *Conn) Write(message *packet.Message) error {
	return c.WriteContext(context.Background(), message)
}

func (c *Conn) WriteContext(ctx context.Context, message *packet.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return transport.ErrClosed
	case c.writeGate <- struct{}{}:
	}
	closeOnReturn := false
	defer func() {
		<-c.writeGate
		if closeOnReturn {
			_ = c.Close()
		}
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-c.done:
		return transport.ErrClosed
	default:
	}
	frame, err := c.codec.Encode(message)
	if err != nil {
		return err
	}
	defer frame.Close()
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline := time.Now().Add(c.writeWait)
	if value, ok := ctx.Deadline(); ok && value.Before(deadline) {
		deadline = value
	}
	closeOnReturn = true
	if err := c.raw.SetWriteDeadline(deadline); err != nil {
		return err
	}
	interrupted := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		// 先关闭底层流打断写入；等写锁释放后再通知 HandleClose。
		_ = c.raw.NetConn().Close()
		close(interrupted)
	})
	data := frame.Bytes()
	n, err := c.raw.Write(data)
	if !stop() {
		<-interrupted
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if err != nil {
		if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	closeOnReturn = false
	return nil
}

func (c *Conn) Close() error {
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	handler := c.handler
	c.mutex.Unlock()
	// 直接关闭底层连接，不等待 TLS close_notify，及时打断阻塞的读写。
	err := c.raw.NetConn().Close()
	if handler != nil {
		handler.HandleClose(c)
	}
	if errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
		return nil
	}
	return err
}

func (c *Conn) LocalAddr() net.Addr  { return c.raw.LocalAddr() }
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }
func (c *Conn) Type() transport.Type { return c.kind }
func (c *Conn) Secure() bool         { return c.raw.ConnectionState().HandshakeComplete }
