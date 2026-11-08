package netWs

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
	"github.com/gorilla/websocket"
)

// 每条WebSocket二进制消息恰好承载一个完整packet，允许WebSocket层自行分片。
type conn struct {
	raw       *websocket.Conn
	socket    net.Conn
	secure    bool
	codec     *packet.Codec
	writeWait time.Duration
	writeGate chan struct{}
	done      chan struct{}

	mutex   sync.Mutex
	started bool
	closed  bool
	handler transport.Handler
}

func newConn(raw *websocket.Conn, o options) *conn {
	socket := raw.NetConn()
	secure := false
	if conn, ok := socket.(*tls.Conn); ok {
		if !conn.ConnectionState().HandshakeComplete {
			panic(transport.ErrInsecure)
		}
		socket, secure = conn.NetConn(), true
	}
	raw.SetReadLimit(int64(o.codec.MaxFrameSize()))
	return &conn{raw: raw, socket: socket, secure: secure, codec: o.codec, writeWait: o.writeTimeout,
		writeGate: make(chan struct{}, 1), done: make(chan struct{})}
}

func (c *conn) Start(handler transport.Handler) error {
	if handler == nil {
		panic("network/ws: 必须提供Handler")
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

func (c *conn) readLoop() {
	defer c.Close()
	for {
		kind, reader, err := c.raw.NextReader()
		if err != nil || kind != websocket.BinaryMessage {
			return
		}
		message, err := c.codec.Read(reader)
		if err != nil {
			return
		}
		var trailing [1]byte
		if count, err := reader.Read(trailing[:]); count != 0 || !errors.Is(err, io.EOF) {
			message.Release()
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

func (c *conn) Write(message *packet.Message) error {
	return c.WriteContext(context.Background(), message)
}

func (c *conn) WriteContext(ctx context.Context, message *packet.Message) error {
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
		_ = c.socket.Close()
		close(interrupted)
	})
	err = c.raw.WriteMessage(websocket.BinaryMessage, frame.Bytes())
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
	closeOnReturn = false
	return nil
}

func (c *conn) Close() error {
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	handler := c.handler
	c.mutex.Unlock()
	err := c.socket.Close()
	if handler != nil {
		handler.HandleClose(c)
	}
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func (c *conn) LocalAddr() net.Addr  { return c.raw.LocalAddr() }
func (c *conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }
func (c *conn) Type() transport.Type { return transport.TypeWS }
func (c *conn) Secure() bool         { return c.secure }
