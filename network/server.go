package network

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"math"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/2comjie/ngame/network/protocol"
	"github.com/2comjie/ngame/network/transport"
	"github.com/2comjie/ngame/packet"
	"github.com/2comjie/ntool/logx"
	"github.com/2comjie/ntool/util/gopool"
	"google.golang.org/protobuf/proto"
)

const maxQueuedRequestBytes = 4 << 20
const maxTokenSize = 8 << 10

type Server struct {
	options  options
	mutex    sync.RWMutex
	byConn   map[transport.Conn]*Session
	byUid    map[uint64]*Session
	unbound  int
	sequence uint64
	started  bool
	closed   bool
	ctx      context.Context
	cancel   context.CancelFunc
	wait     sync.WaitGroup
	done     chan struct{}
}

func NewServer(opts ...Option) *Server {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if o.auther == nil {
		panic(ErrAutherRequired)
	}
	if len(o.listeners) == 0 {
		panic(ErrListenerMissing)
	}
	for _, listener := range o.listeners {
		if listener == nil {
			panic(ErrListenerMissing)
		}
	}
	if o.bindTimeout <= 0 || o.heartbeat < time.Millisecond || o.heartbeat.Milliseconds() > math.MaxUint32 || o.heartbeatTimeout <= o.heartbeat || o.requestTimeout < 0 {
		panic("network: 认证、心跳或请求超时配置错误")
	}
	if o.requestQueue <= 0 || o.maxUnbound <= 0 || o.acceptWorkers <= 0 || o.maxBody <= 0 {
		panic("network: 队列、连接数和Body上限必须大于0")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{options: o, byConn: make(map[transport.Conn]*Session), byUid: make(map[uint64]*Session),
		ctx: ctx, cancel: cancel, done: make(chan struct{})}
}

func (s *Server) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.closed {
		return ErrClosed
	}
	if s.started {
		return ErrStarted
	}
	s.started = true
	s.wait.Go(func() {
		ticker := time.NewTicker(max(time.Millisecond, min(time.Second, s.options.bindTimeout/2, s.options.heartbeatTimeout/2)))
		defer ticker.Stop()
		for {
			select {
			case <-s.ctx.Done():
				return
			case now := <-ticker.C:
				var expired []*Session
				s.mutex.Lock()
				for _, session := range s.byConn {
					if (!session.IsBound() && now.Sub(session.acceptedAt) >= s.options.bindTimeout) ||
						(session.IsBound() && now.Sub(time.Unix(0, session.heartbeatAt.Load())) >= s.options.heartbeatTimeout) {
						session.cancel()
						expired = append(expired, session)
					}
				}
				s.mutex.Unlock()
				for _, session := range expired {
					_ = session.Conn.Close()
				}
			}
		}
	})
	for _, listener := range s.options.listeners {
		for range s.options.acceptWorkers {
			s.wait.Add(1)
			go s.acceptLoop(listener)
		}
	}
	return nil
}

func (s *Server) acceptLoop(listener transport.Listener) {
	defer s.wait.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil || errors.Is(err, net.ErrClosed) || errors.Is(err, io.ErrClosedPipe) {
				return
			}
			// 单个连接握手失败不应停止整个监听器。
			logx.Debugf("network: 接入失败 error=%v", err)
			continue
		}
		if !conn.Secure() && (!transport.IsLoopback(conn.LocalAddr()) || !transport.IsLoopback(conn.RemoteAddr())) {
			_ = conn.Close()
			continue
		}
		s.mutex.Lock()
		if s.closed || s.unbound >= s.options.maxUnbound {
			s.mutex.Unlock()
			_ = conn.Close()
			continue
		}
		ctx, cancel := context.WithCancel(s.ctx)
		s.sequence++
		session := &Session{Id: s.sequence, Conn: conn, ctx: ctx, cancel: cancel, acceptedAt: time.Now(),
			queue: make(chan queuedMessage, s.options.requestQueue)}
		s.byConn[conn] = session
		s.unbound++
		s.wait.Add(1)
		s.mutex.Unlock()
		if err := conn.Start(s); err != nil {
			_ = conn.Close()
			s.HandleClose(conn)
		}
		go s.serveSession(session)
	}
}

func (s *Server) HandleMessage(conn transport.Conn, message *packet.Message) {
	s.mutex.RLock()
	session := s.byConn[conn]
	s.mutex.RUnlock()
	if session == nil || session.ctx.Err() != nil {
		_ = conn.Close()
		return
	}
	if !session.IsBound() && !session.verified.Load() && message.Header.Type != packet.BindReq {
		_ = conn.Close()
		return
	}
	switch message.Header.Type {
	case packet.BindReq, packet.Req:
		size := int64(packet.HeaderSize + len(message.Body))
		for key, value := range message.Header.Values {
			size += int64(len(key) + len(value))
		}
		if session.queuedBytes.Add(size) > maxQueuedRequestBytes {
			session.queuedBytes.Add(-size)
			_ = conn.Close()
			return
		}
		request := &packet.Message{Header: packet.Header{
			Type: message.Header.Type, Route: message.Header.Route, Seq: message.Header.Seq, Values: maps.Clone(message.Header.Values),
		}, Body: bytes.Clone(message.Body)}
		select {
		case session.queue <- queuedMessage{message: request, size: size}:
		default:
			session.queuedBytes.Add(-size)
			_ = conn.Close()
		}
	case packet.Ping, packet.Pong:
		session.heartbeatAt.Store(time.Now().UnixNano())
		if message.Header.Type == packet.Ping {
			if err := conn.WriteContext(session.ctx, &packet.Message{Header: packet.Header{Type: packet.Pong}, Body: message.Body}); err != nil {
				_ = conn.Close()
			}
		}
	default:
		_ = conn.Close()
	}
}

func (s *Server) HandleClose(conn transport.Conn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if session := s.byConn[conn]; session != nil {
		session.cancel()
		delete(s.byConn, conn)
		if !session.IsBound() {
			s.unbound--
		}
		if s.byUid[session.Uid()] == session {
			delete(s.byUid, session.Uid())
		}
	}
}

func (s *Server) serveSession(session *Session) {
	defer s.wait.Done()
	defer func() {
		session.cancel()
		_ = session.Conn.Close()
		s.HandleClose(session.Conn)
		if session.IsBound() && s.options.onSessionEnd != nil {
			s.options.onSessionEnd(session)
		}
	}()
	for {
		select {
		case <-session.ctx.Done():
			return
		case queued := <-session.queue:
			session.queuedBytes.Add(-queued.size)
			if session.ctx.Err() != nil {
				return
			}
			if queued.call != nil {
				if err := queued.call.ctx.Err(); err != nil {
					queued.call.done <- err
					continue
				}
				s.mutex.RLock()
				current := s.byUid[session.Uid()] == session
				s.mutex.RUnlock()
				if !current {
					queued.call.done <- ErrNotBound
					continue
				}
			}
			if queued.message.Header.Type == packet.BindReq {
				s.handleBind(session, queued.message)
				continue
			}
			if queued.call == nil {
				if err := decodeBody(s.options, queued.message); err != nil {
					return
				}
			}
			ctx := session.ctx
			if queued.call != nil {
				ctx = queued.call.ctx
			}
			cancel := func() {}
			if s.options.requestTimeout > 0 {
				ctx, cancel = context.WithTimeout(ctx, s.options.requestTimeout)
			} else if queued.call != nil {
				ctx, cancel = context.WithCancel(ctx)
			}
			if queued.call != nil {
				stop := context.AfterFunc(session.ctx, cancel)
				if err := ctx.Err(); err != nil {
					queued.call.done <- err
				} else {
					queued.call.done <- queued.call.execute(ctx, session, queued.message)
				}
				stop()
			} else if s.options.onReq != nil {
				s.options.onReq(&ReqContext{Context: ctx, Session: session, Request: queued.message, options: s.options})
			}
			cancel()
		}
	}
}

func (s *Server) handleBind(session *Session, request *packet.Message) {
	if session.IsBound() {
		_ = session.Conn.Close()
		return
	}
	value := request.Header.Values["authorization"]
	if !strings.HasPrefix(value, "Bearer ") || len(value) <= len("Bearer ") || len(value)-len("Bearer ") > maxTokenSize {
		_ = s.writeBindResponse(session, protocol.BindCode_BIND_UNAUTHORIZED)
		_ = session.Conn.Close()
		return
	}
	ctx, cancel := context.WithDeadline(session.ctx, session.acceptedAt.Add(s.options.bindTimeout))
	defer cancel()
	uid, err := s.options.auther.Auth(ctx, []byte(value[len("Bearer "):]))
	if err != nil || uid == 0 || ctx.Err() != nil {
		_ = s.writeBindResponse(session, protocol.BindCode_BIND_UNAUTHORIZED)
		_ = session.Conn.Close()
		return
	}
	// 允许刚收到 BindRsp 的客户端立即发送请求；请求仍排队到绑定完成后处理。
	session.verified.Store(true)
	// 先写入 BindRsp，再将 Session 发布到 byUid，避免推送抢在认证响应前面。
	if err := s.writeBindResponse(session, protocol.BindCode_BIND_OK); err != nil {
		_ = session.Conn.Close()
		return
	}
	s.mutex.Lock()
	old := s.byUid[uid]
	if s.closed || ctx.Err() != nil || session.ctx.Err() != nil || (old != nil && old.Id > session.Id) {
		s.mutex.Unlock()
		_ = session.Conn.Close()
		return
	}
	session.uid.Store(uid)
	now := time.Now().UnixNano()
	session.heartbeatAt.Store(now)
	session.boundAt.Store(now)
	s.unbound--
	s.byUid[uid] = session
	s.mutex.Unlock()
	if old != nil {
		_ = old.Conn.Close()
	}
	if s.options.onSessionBind != nil {
		s.options.onSessionBind(session)
	}
}

func (s *Server) writeBindResponse(session *Session, code protocol.BindCode) error {
	response := &protocol.BindResponse{Code: code}
	if code == protocol.BindCode_BIND_OK {
		response.HeartbeatIntervalMilli = uint32(s.options.heartbeat.Milliseconds())
	}
	body, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	return session.Conn.WriteContext(session.ctx, &packet.Message{Header: packet.Header{Type: packet.BindRsp}, Body: body})
}

// PushUid 只向已认证的 Session 推送，自动设置消息类型和 Seq。
func (s *Server) PushUid(ctx context.Context, uid uint64, push *packet.Message) error {
	s.mutex.RLock()
	session := s.byUid[uid]
	s.mutex.RUnlock()
	if session == nil || session.ctx.Err() != nil {
		return ErrNotBound
	}
	message := &packet.Message{Header: packet.Header{Type: packet.Push, Route: push.Header.Route, Values: maps.Clone(push.Header.Values)}, Body: push.Body}
	if err := encodeBody(s.options, message); err != nil {
		return err
	}
	return session.Conn.WriteContext(ctx, message)
}

// Broadcast 向调用时所有已认证的Session推送，返回成功写入的连接数。
// 单个连接失败不影响其他连接，部分失败返回ErrPushFailed；ctx取消时返回ctx.Err()。
func (s *Server) Broadcast(ctx context.Context, push *packet.Message) (uint32, error) {
	_, sent, err := s.BroadcastStats(ctx, push)
	return uint32(sent), err
}

// BroadcastStats 返回同一次会话快照的目标数及成功写入数。
func (s *Server) BroadcastStats(ctx context.Context, push *packet.Message) (uint64, uint64, error) {
	s.mutex.RLock()
	sessions := make([]*Session, 0, len(s.byUid))
	for _, session := range s.byUid {
		sessions = append(sessions, session)
	}
	s.mutex.RUnlock()
	sent, err := s.pushSessions(ctx, sessions, push)
	return uint64(len(sessions)), uint64(sent), err
}

// MultiPush 向指定Uid列表推送，重复Uid只发送一次，离线用户跳过。
// 发送期间连接的替换不会将本次消息转发给新Session，返回规则与Broadcast相同。
func (s *Server) MultiPush(ctx context.Context, uids []uint64, push *packet.Message) (uint32, error) {
	s.mutex.RLock()
	sessions := make([]*Session, 0, len(uids))
	seen := make(map[uint64]bool, len(uids))
	for _, uid := range uids {
		if session := s.byUid[uid]; session != nil && !seen[uid] {
			seen[uid] = true
			sessions = append(sessions, session)
		}
	}
	s.mutex.RUnlock()
	return s.pushSessions(ctx, sessions, push)
}

func (s *Server) pushSessions(ctx context.Context, sessions []*Session, push *packet.Message) (uint32, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if len(sessions) == 0 {
		return 0, nil
	}
	message := &packet.Message{Header: packet.Header{Type: packet.Push, Route: push.Header.Route, Values: maps.Clone(push.Header.Values)}, Body: bytes.Clone(push.Body)}
	if err := packet.NewCodec(0).Validate(message); err != nil {
		return 0, err
	}
	if err := encodeBody(s.options, message); err != nil {
		return 0, err
	}
	var next atomic.Int64
	var success atomic.Uint32
	var failed atomic.Bool
	var wait sync.WaitGroup
	for range min(16, len(sessions)) {
		wait.Add(1)
		gopool.CtxGo(ctx, func() {
			defer wait.Done()
			for ctx.Err() == nil {
				index := int(next.Add(1) - 1)
				if index >= len(sessions) {
					return
				}
				session := sessions[index]
				if session.ctx.Err() != nil {
					continue
				}
				if err := session.Conn.WriteContext(ctx, message); err != nil {
					failed.Store(true)
					if ctx.Err() == nil && session.ctx.Err() == nil {
						logx.Warnf("network: 批量推送失败 uid=%d error=%v", session.Uid(), err)
					}
					continue
				}
				success.Add(1)
			}
		})
	}
	wait.Wait()
	if err := ctx.Err(); err != nil {
		return success.Load(), err
	}
	if failed.Load() {
		return success.Load(), ErrPushFailed
	}
	return success.Load(), nil
}

func (s *Server) KickUid(uid uint64) bool {
	s.mutex.RLock()
	session := s.byUid[uid]
	s.mutex.RUnlock()
	if session == nil {
		return false
	}
	_ = session.Conn.Close()
	return true
}

// RequestStop 开始关闭监听和连接，不等待 Session 回调退出。
func (s *Server) RequestStop() {
	s.mutex.Lock()
	first := !s.closed
	s.closed = true
	var sessions []*Session
	if first {
		s.cancel()
		for _, session := range s.byConn {
			sessions = append(sessions, session)
		}
	}
	s.mutex.Unlock()
	if first {
		for _, listener := range s.options.listeners {
			_ = listener.Close()
		}
		for _, session := range sessions {
			_ = session.Conn.Close()
		}
		go func() { s.wait.Wait(); close(s.done) }()
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.RequestStop()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
