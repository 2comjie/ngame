package network

import (
	"bytes"
	"context"
	"maps"

	"github.com/2comjie/ngame/packet"
)

// ValidatePush 校验推送的业务明文及 Header；Type、Seq 由发送端设置。
func (s *Server) ValidatePush(push *packet.Message) error {
	if len(push.Body) > s.options.maxBody {
		return ErrBodyTooLarge
	}
	return packet.NewCodec(0).Validate(&packet.Message{
		Header: packet.Header{
			Type:   packet.Push,
			Route:  push.Header.Route,
			Values: push.Header.Values,
		},
	})
}

// PushUidSession 捕获匹配的 Session，发送期间重连也不转投新会话。
func (s *Server) PushUidSession(ctx context.Context, uid, sessionId uint64, push *packet.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.ValidatePush(push); err != nil {
		return err
	}
	s.mutex.RLock()
	session := s.byUid[uid]
	s.mutex.RUnlock()
	if session == nil || session.Id != sessionId || session.ctx.Err() != nil {
		return ErrNotBound
	}
	message := &packet.Message{
		Header: packet.Header{
			Type:   packet.Push,
			Route:  push.Header.Route,
			Values: maps.Clone(push.Header.Values),
		},
		Body: bytes.Clone(push.Body),
	}
	if err := encodeBody(s.options, message); err != nil {
		return err
	}
	return session.Conn.WriteContext(ctx, message)
}

func (s *Server) KickUidSession(uid, sessionId uint64) (bool, error) {
	s.mutex.RLock()
	session := s.byUid[uid]
	s.mutex.RUnlock()
	if session == nil || session.Id != sessionId || session.ctx.Err() != nil {
		return false, nil
	}
	session.cancel()
	err := session.Conn.Close()
	s.HandleClose(session.Conn)
	return true, err
}

func (s *Server) SessionCount() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return uint64(len(s.byUid))
}

func (s *Server) Sessions() []*Session {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	sessions := make([]*Session, 0, len(s.byUid))
	for _, session := range s.byUid {
		if !session.bindReady.Load() {
			continue
		}
		select {
		case <-session.Context().Done():
			continue
		default:
		}
		sessions = append(sessions, session)
	}
	return sessions
}

// ExecuteSession 将业务明文与客户端请求排入同一个有界队列。
// execute 在 Session 处理协程内运行，必须响应 ctx；不得在该协程中重入本方法。
// 只有执行前确认目标会话不存在才返回 ErrNotBound，队列及取消错误不表示可以重放。
func (s *Server) ExecuteSession(ctx context.Context, uid, sessionId uint64, request *packet.Message,
	execute func(context.Context, *Session, *packet.Message) error) error {
	if execute == nil {
		panic("network: 必须提供 Session 执行函数")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if request.Header.Type != packet.Req {
		return packet.ErrType
	}
	if err := packet.NewCodec(0).Validate(request); err != nil {
		return err
	}
	if len(request.Body) > s.options.maxBody {
		return ErrBodyTooLarge
	}
	s.mutex.RLock()
	session := s.byUid[uid]
	closed := s.closed
	s.mutex.RUnlock()
	if closed {
		return ErrClosed
	}
	if session == nil || session.Id != sessionId || session.ctx.Err() != nil {
		return ErrNotBound
	}
	size := int64(packet.HeaderSize + len(request.Body))
	for key, value := range request.Header.Values {
		size += int64(len(key) + len(value))
	}
	if session.queuedBytes.Add(size) > maxQueuedRequestBytes {
		session.queuedBytes.Add(-size)
		return ErrRequestQueueFull
	}
	call := &sessionCall{
		ctx:     ctx,
		execute: execute,
		done:    make(chan error, 1),
	}
	queued := queuedMessage{
		message: &packet.Message{
			Header: packet.Header{
				Type:   packet.Req,
				Route:  request.Header.Route,
				Seq:    request.Header.Seq,
				Values: maps.Clone(request.Header.Values),
			},
			Body: bytes.Clone(request.Body),
		},
		size: size,
		call: call,
	}
	select {
	case session.queue <- queued:
	default:
		session.queuedBytes.Add(-size)
		return ErrRequestQueueFull
	}
	select {
	case err := <-call.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-session.ctx.Done():
		return session.ctx.Err()
	}
}
