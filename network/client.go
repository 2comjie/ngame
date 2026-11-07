package network

import (
	"bytes"
	"context"
	"encoding/binary"
	"maps"
	"sync"
	"time"

	"github.com/2comjie/ngame/network/protocol"
	"github.com/2comjie/ngame/network/transport"
	"github.com/2comjie/ngame/packet"
	"google.golang.org/protobuf/proto"
)

type PushHandler func(context.Context, *packet.Message)

type pendingCall struct {
	route uint32
	reply chan *packet.Message
}

// Client 只执行一次连接尝试。失败或关闭后，重连应创建新的 Client 并申请新的凭证。
type Client struct {
	options options
	ctx     context.Context
	cancel  context.CancelFunc

	mutex       sync.Mutex
	conn        transport.Conn
	started     bool
	closed      bool
	bound       bool
	bindWait    chan *protocol.BindResponse
	pending     map[uint64]*pendingCall
	pushHandler map[uint32]PushHandler
	sequence    uint64
	pingNonce   uint64
	pingAt      time.Time
	pong        chan uint64
}

func NewClient(opts ...Option) *Client {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if o.dialer == nil {
		panic(ErrDialerMissing)
	}
	if o.bindTimeout <= 0 || o.heartbeatTimeout <= 0 || o.maxBody <= 0 || o.maxPending <= 0 {
		panic("network: 连接、心跳超时及Body、请求数上限必须大于0")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Client{options: o, ctx: ctx, cancel: cancel, pending: make(map[uint64]*pendingCall),
		pushHandler: make(map[uint32]PushHandler), pong: make(chan uint64, 1)}
}

// Connect 在 ctx 和 WithBindTimeout 的限制内完成安全连接和凭证认证。
// ticket 通过首包 Header.Values["authorization"] 发送，认证成功后才返回 nil。
func (c *Client) Connect(ctx context.Context, ticket []byte) error {
	if len(ticket) == 0 || len(ticket) > maxTokenSize {
		return ErrUnauthorized
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		return ErrClosed
	}
	if c.started {
		c.mutex.Unlock()
		return ErrStarted
	}
	c.started = true
	c.mutex.Unlock()
	success := false
	defer func() {
		if !success {
			_ = c.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, c.options.bindTimeout)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	conn, err := c.options.dialer.DialContext(ctx)
	if err != nil {
		if c.ctx.Err() != nil {
			return ErrClosed
		}
		return err
	}
	if !conn.Secure() {
		_ = conn.Close()
		return transport.ErrInsecure
	}
	wait := make(chan *protocol.BindResponse, 1)
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		_ = conn.Close()
		return ErrClosed
	}
	c.conn, c.bindWait = conn, wait
	c.mutex.Unlock()
	if err := conn.Start(c); err != nil {
		return err
	}
	if err := conn.WriteContext(ctx, &packet.Message{Header: packet.Header{
		Type: packet.BindReq, Values: map[string]string{"authorization": "Bearer " + string(ticket)},
	}}); err != nil {
		select {
		case response := <-wait:
			if response.Code != protocol.BindCode_BIND_OK {
				return ErrUnauthorized
			}
		default:
		}
		return err
	}
	select {
	case response := <-wait:
		if response.Code != protocol.BindCode_BIND_OK {
			return ErrUnauthorized
		}
		if response.HeartbeatIntervalMilli == 0 {
			return ErrBindResponse
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		c.mutex.Lock()
		success = !c.closed && c.bound
		c.mutex.Unlock()
		if !success {
			return ErrClosed
		}
		return nil
	case <-ctx.Done():
		select {
		case response := <-wait:
			if response.Code != protocol.BindCode_BIND_OK {
				return ErrUnauthorized
			}
		default:
		}
		if c.ctx.Err() != nil {
			return ErrClosed
		}
		return ctx.Err()
	case <-c.ctx.Done():
		// 认证拒绝后服务器可能立即关闭，优先保留已经收到的拒绝结果。
		select {
		case response := <-wait:
			if response.Code != protocol.BindCode_BIND_OK {
				return ErrUnauthorized
			}
		default:
		}
		return ErrClosed
	}
}

// Call 自动设置 Req 类型和请求序号，返回的响应独立持有 Header.Values 和 Body，无需 Release。
func (c *Client) Call(ctx context.Context, request *packet.Message) (*packet.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		return nil, ErrClosed
	}
	if !c.bound {
		c.mutex.Unlock()
		return nil, ErrNotBound
	}
	if len(c.pending) >= c.options.maxPending {
		c.mutex.Unlock()
		return nil, ErrPendingFull
	}
	c.sequence++
	seq := c.sequence
	call := &pendingCall{route: request.Header.Route, reply: make(chan *packet.Message, 1)}
	c.pending[seq] = call
	conn := c.conn
	c.mutex.Unlock()
	defer func() {
		c.mutex.Lock()
		delete(c.pending, seq)
		c.mutex.Unlock()
	}()
	message := &packet.Message{Header: packet.Header{
		Type: packet.Req, Route: request.Header.Route, Seq: seq, Values: maps.Clone(request.Header.Values),
	}, Body: request.Body}
	if err := encodeBody(c.options, message); err != nil {
		return nil, err
	}
	if err := conn.WriteContext(ctx, message); err != nil {
		return nil, err
	}
	select {
	case response := <-call.reply:
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, ErrClosed
	}
}

// Tell 发送无需响应的 Req，自动将 Seq 设为0。
func (c *Client) Tell(ctx context.Context, request *packet.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		return ErrClosed
	}
	if !c.bound {
		c.mutex.Unlock()
		return ErrNotBound
	}
	conn := c.conn
	c.mutex.Unlock()
	message := &packet.Message{Header: packet.Header{
		Type: packet.Req, Route: request.Header.Route, Values: maps.Clone(request.Header.Values),
	}, Body: request.Body}
	if err := encodeBody(c.options, message); err != nil {
		return err
	}
	return conn.WriteContext(ctx, message)
}

// OnPush 注册路由对应的推送回调；handler为nil时取消注册。
func (c *Client) OnPush(route uint32, handler PushHandler) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if handler == nil {
		delete(c.pushHandler, route)
	} else {
		c.pushHandler[route] = handler
	}
}

func (c *Client) HandleMessage(conn transport.Conn, message *packet.Message) {
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		return
	}
	bound := c.bound
	c.mutex.Unlock()
	if !bound && message.Header.Type != packet.BindRsp {
		_ = c.Close()
		return
	}
	switch message.Header.Type {
	case packet.BindRsp:
		var response protocol.BindResponse
		if err := proto.Unmarshal(message.Body, &response); err != nil {
			_ = c.Close()
			return
		}
		c.mutex.Lock()
		wait := c.bindWait
		if c.closed || c.bound || wait == nil {
			c.mutex.Unlock()
			_ = c.Close()
			return
		}
		c.bindWait = nil
		accepted := response.Code == protocol.BindCode_BIND_OK && response.HeartbeatIntervalMilli > 0
		if accepted {
			c.bound = true
		}
		c.mutex.Unlock()
		wait <- &response
		if accepted {
			go c.heartbeatLoop(time.Duration(response.HeartbeatIntervalMilli) * time.Millisecond)
		}
	case packet.Rsp:
		c.mutex.Lock()
		call := c.pending[message.Header.Seq]
		c.mutex.Unlock()
		if call == nil {
			return // 已超时请求的迟到响应。
		}
		if call.route != message.Header.Route {
			_ = c.Close()
			return
		}
		if err := decodeBody(c.options, message); err != nil {
			_ = c.Close()
			return
		}
		response := &packet.Message{Header: packet.Header{
			Type: message.Header.Type, Route: message.Header.Route, Seq: message.Header.Seq, Values: maps.Clone(message.Header.Values),
		}, Body: bytes.Clone(message.Body)}
		c.mutex.Lock()
		if c.pending[message.Header.Seq] != call {
			c.mutex.Unlock()
			return
		}
		delete(c.pending, message.Header.Seq)
		c.mutex.Unlock()
		call.reply <- response
	case packet.Push:
		if err := decodeBody(c.options, message); err != nil {
			_ = c.Close()
			return
		}
		c.mutex.Lock()
		handler := c.pushHandler[message.Header.Route]
		c.mutex.Unlock()
		if handler != nil {
			handler(c.ctx, message)
		}
	case packet.Ping:
		if err := conn.WriteContext(c.ctx, &packet.Message{Header: packet.Header{Type: packet.Pong}, Body: message.Body}); err != nil {
			_ = c.Close()
		}
	case packet.Pong:
		nonce := binary.BigEndian.Uint64(message.Body)
		c.mutex.Lock()
		matched := !c.pingAt.IsZero() && nonce == c.pingNonce
		if matched {
			c.pingAt = time.Time{}
		}
		c.mutex.Unlock()
		if matched {
			select {
			case c.pong <- nonce:
			default:
			}
		}
	default:
		_ = c.Close()
	}
}

func (c *Client) HandleClose(transport.Conn) { _ = c.Close() }

func (c *Client) heartbeatLoop(interval time.Duration) {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	waiting := false
	for {
		select {
		case <-c.ctx.Done():
			return
		case nonce := <-c.pong:
			c.mutex.Lock()
			matched := nonce == c.pingNonce && c.pingAt.IsZero()
			c.mutex.Unlock()
			if matched {
				waiting = false
				timer.Reset(interval)
			}
		case <-timer.C:
			c.mutex.Lock()
			if c.closed {
				c.mutex.Unlock()
				return
			}
			if waiting {
				unanswered := !c.pingAt.IsZero()
				c.mutex.Unlock()
				if unanswered {
					_ = c.Close()
					return
				}
				// Pong已收到但通知尚未处理，不能因此误判超时。
				waiting = false
				timer.Reset(interval)
				continue
			}
			c.pingNonce++
			var nonce [packet.PingBodySize]byte
			binary.BigEndian.PutUint64(nonce[:], c.pingNonce)
			c.pingAt = time.Now()
			conn := c.conn
			c.mutex.Unlock()
			if err := conn.WriteContext(c.ctx, &packet.Message{Header: packet.Header{Type: packet.Ping}, Body: nonce[:]}); err != nil {
				_ = c.Close()
				return
			}
			waiting = true
			timer.Reset(c.options.heartbeatTimeout)
		}
	}
}

func (c *Client) Close() error {
	c.mutex.Lock()
	if c.closed {
		c.mutex.Unlock()
		return nil
	}
	c.closed, c.bound = true, false
	conn := c.conn
	c.conn, c.bindWait = nil, nil
	clear(c.pending)
	c.cancel()
	c.mutex.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (c *Client) Done() <-chan struct{} { return c.ctx.Done() }
