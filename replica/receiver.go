package replica

import (
	"errors"

	"github.com/2comjie/ngame/diff"
	"github.com/2comjie/ngame/replica/protocol"
)

var (
	ErrInvalidFrame = errors.New("replica: invalid frame")
	ErrStaleFrame   = errors.New("replica: frame belongs to another activation or session")
)

// Handler 的回调串行执行。Snapshot 创建或替换对象；Remove 对不存在的对象也应成功。
// Delta 必须按 updates 顺序应用；应用失败后 Receiver 会要求全量恢复。
type Handler struct {
	Snapshot func(EntityKey, diff.Scope, []byte) error
	Delta    func(EntityKey, diff.Scope, [][]byte) error
	Remove   func(EntityKey) error
}

type Feedback struct {
	Ack   *protocol.Ack
	Reset *protocol.ResetRequest
}

type received struct {
	subscription uint64
	sequence     uint64
	version      uint64
	scope        diff.Scope
	initialized  bool
	removed      bool
	needsFull    bool
}

// Receiver 是客户端应用协议的 Go 实现。Owner 和会话标识由已认证的订阅握手确定。
// Actor 重新激活或客户端重连时创建新 Receiver，不能根据收到的任意 Frame 切换 Owner。
// 可在 Go 客户端使用，也可作为 C#/其他客户端实现的参考。
type Receiver struct {
	owner    Owner
	observer string
	handler  Handler
	entities map[EntityKey]*received
}

func NewReceiver(owner Owner, observer string, handler Handler) *Receiver {
	if owner.Actor.Type == "" || owner.Actor.Id == "" || owner.ActivationId == "" || observer == "" {
		panic("replica: Actor identity, ActivationId and observer are required")
	}
	if handler.Snapshot == nil || handler.Delta == nil || handler.Remove == nil {
		panic("replica: Snapshot, Delta and Remove handlers are required")
	}
	return &Receiver{
		owner:    owner,
		observer: observer,
		handler:  handler,
		entities: make(map[EntityKey]*received),
	}
}

// Receive 仅在成功应用消息后返回 Ack；发现基线/序号断层时返回 Reset 请求。
// 回调失败时同时返回错误和 Reset，调用方需要把 Reset 发回所属 Actor。
// 删除后保留订阅序号，直到 Receiver 随会话释放，避免迟到的 Initial 复活实体。
func (r *Receiver) Receive(frame *protocol.Frame) (Feedback, error) {
	if !r.owner.matches(frame.Owner) || frame.SessionId != r.observer {
		return Feedback{}, ErrStaleFrame
	}
	if frame.Entity == nil || frame.Entity.Type == "" || frame.Entity.Id == "" || frame.SubscriptionId == 0 || frame.Sequence == 0 ||
		frame.Scope == 0 || frame.Scope > uint32(diff.All) ||
		frame.Kind < protocol.Kind_KIND_INITIAL || frame.Kind > protocol.Kind_KIND_REMOVE {
		return Feedback{}, ErrInvalidFrame
	}
	key := EntityKey{
		Type: frame.Entity.Type,
		Id:   frame.Entity.Id,
	}
	state := r.entities[key]
	if state != nil && frame.SubscriptionId < state.subscription {
		return Feedback{}, nil
	}
	if state == nil || frame.SubscriptionId > state.subscription {
		state = &received{
			subscription: frame.SubscriptionId,
			scope:        diff.Scope(frame.Scope),
		}
		r.entities[key] = state
	}
	ack := &protocol.Ack{
		Owner:     r.owner.message(),
		SessionId: r.observer,
		Entity: &protocol.EntityKey{
			Type: key.Type,
			Id:   key.Id,
		},
		SubscriptionId: state.subscription,
		Sequence:       state.sequence,
	}
	reset := &protocol.ResetRequest{
		Owner:          ack.Owner,
		SessionId:      ack.SessionId,
		Entity:         ack.Entity,
		SubscriptionId: ack.SubscriptionId,
	}
	if frame.Sequence <= state.sequence {
		return Feedback{Ack: ack}, nil
	}
	if diff.Scope(frame.Scope) != state.scope || state.removed {
		return Feedback{}, ErrInvalidFrame
	}
	if frame.Kind == protocol.Kind_KIND_DELTA {
		if !state.initialized || state.needsFull || frame.Sequence != state.sequence+1 || frame.BaseVersion != state.version {
			state.needsFull = true
			return Feedback{Reset: reset}, nil
		}
		if frame.Version <= frame.BaseVersion || uint64(len(frame.Updates)) != frame.Version-frame.BaseVersion {
			return Feedback{}, ErrInvalidFrame
		}
		if err := r.handler.Delta(key, state.scope, frame.Updates); err != nil {
			state.needsFull = true
			return Feedback{Reset: reset}, err
		}
	} else if frame.Kind == protocol.Kind_KIND_REMOVE {
		if err := r.handler.Remove(key); err != nil {
			return Feedback{}, err
		}
		state.removed = true
	} else {
		if frame.Version < state.version {
			return Feedback{}, ErrInvalidFrame
		}
		if err := r.handler.Snapshot(key, state.scope, frame.Snapshot); err != nil {
			state.needsFull = true
			return Feedback{Reset: reset}, err
		}
		state.initialized = true
		state.needsFull = false
	}
	state.sequence = frame.Sequence
	state.version = frame.Version
	ack.Sequence = state.sequence
	return Feedback{Ack: ack}, nil
}
