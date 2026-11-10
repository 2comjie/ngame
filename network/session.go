package network

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/2comjie/ngame/network/transport"
	"github.com/2comjie/ngame/packet"
)

type queuedMessage struct {
	message *packet.Message
	size    int64
	call    *sessionCall
}

type sessionCall struct {
	ctx     context.Context
	execute func(context.Context, *Session, *packet.Message) error
	done    chan error
}

type Session struct {
	Id   uint64
	Conn transport.Conn

	ctx        context.Context
	cancel     context.CancelFunc
	queue      chan queuedMessage
	acceptedAt time.Time
	uid        atomic.Uint64
	verified   atomic.Bool
	boundAt    atomic.Int64

	bindReady   atomic.Bool
	heartbeatAt atomic.Int64
	queuedBytes atomic.Int64
}

func (s *Session) Uid() uint64              { return s.uid.Load() }
func (s *Session) IsBound() bool            { return s.boundAt.Load() != 0 }
func (s *Session) Context() context.Context { return s.ctx }
func (s *Session) BoundAt() time.Time {
	if value := s.boundAt.Load(); value != 0 {
		return time.Unix(0, value)
	}
	return time.Time{}
}
