package replica

import (
	"time"

	"github.com/2comjie/ngame/diff"
	"github.com/2comjie/ngame/replica/protocol"
	"google.golang.org/protobuf/proto"
)

type EntityKey struct {
	Type string
	Id   string
}

// Owner 标识状态所属 Actor 的一次激活。重新激活必须使用新的 ActivationId。
type Owner struct {
	Actor        EntityKey
	ActivationId string
}

func (o Owner) message() *protocol.Owner {
	return &protocol.Owner{
		Actor: &protocol.EntityKey{
			Type: o.Actor.Type,
			Id:   o.Actor.Id,
		},
		ActivationId: o.ActivationId,
	}
}

func (o Owner) matches(value *protocol.Owner) bool {
	return value != nil && value.Actor != nil && value.Actor.Type == o.Actor.Type &&
		value.Actor.Id == o.Actor.Id && value.ActivationId == o.ActivationId
}

type options struct {
	historyCount int
	historyBytes int
	retry        time.Duration
}

type Option func(*options)

// WithHistoryLimits 限制每个实体、每个 Scope 保留的历史。超出窗口的订阅通过全量恢复。
func WithHistoryLimits(count, bytes int) Option {
	return func(o *options) {
		o.historyCount = count
		o.historyBytes = bytes
	}
}

func WithRetryInterval(interval time.Duration) Option {
	return func(o *options) { o.retry = interval }
}

type change struct {
	version uint64
	data    []byte
}

type view struct {
	version   uint64
	oldest    uint64
	history   []change
	bytes     int
	full      []byte
	fullValid bool
}

type entity struct {
	key      EntityKey
	source   Source
	revision uint64
	views    map[diff.Scope]*view
	removing bool
}

type flight struct {
	frame  *protocol.Frame
	sentAt time.Time
}

type subscription struct {
	key          EntityKey
	id           uint64
	scope        diff.Scope
	sequence     uint64
	ackedVersion uint64
	sentVersion  uint64
	ready        bool
	closing      bool
	reset        bool
	pending      []flight
}

type capture struct {
	entity *entity
	data   proto.Message
}

// CapturedChange 是本轮完整增量，包含 Server 字段，供上层存档消费。
// Revision 只在当前实体注册期间递增，不是数据库持久化版本。
type CapturedChange struct {
	Entity   EntityKey
	Revision uint64
	Data     []byte
}

// Delivery 的 Frame 及其数据只读，可交给网络协程。补发可能共享同一份 Frame。
type Delivery struct {
	Observer string
	Frame    *protocol.Frame
}

type Result struct {
	Changes    []CapturedChange
	Deliveries []Delivery
}

// Manager 在状态所属 Actor 主循环内串行使用，不创建协程，不执行网络或数据库操作。
// observer 必须标识一次客户端会话，不能只使用会被重连复用的玩家 Id。
type Manager struct {
	owner     Owner
	options   options
	entities  map[EntityKey]*entity
	observers map[string]map[EntityKey]*subscription
	nextId    uint64
	captures  []capture
	changes   []CapturedChange
}

func New(owner Owner, opts ...Option) *Manager {
	if owner.Actor.Type == "" || owner.Actor.Id == "" || owner.ActivationId == "" {
		panic("replica: Actor identity and ActivationId are required")
	}
	o := options{
		historyCount: 128,
		historyBytes: 1024 * 1024,
		retry:        time.Second,
	}
	for _, option := range opts {
		option(&o)
	}
	if o.historyCount <= 0 || o.historyBytes <= 0 || o.retry <= 0 {
		panic("replica: history limits and retry interval must be positive")
	}
	return &Manager{
		owner:     owner,
		options:   o,
		entities:  make(map[EntityKey]*entity),
		observers: make(map[string]map[EntityKey]*subscription),
	}
}

func (m *Manager) Register(key EntityKey, source Source) {
	if key.Type == "" || key.Id == "" || source == nil {
		panic("replica: entity identity and Source are required")
	}
	if _, exists := m.entities[key]; exists {
		panic("replica: entity already registered")
	}
	m.entities[key] = &entity{
		key:    key,
		source: source,
		views:  make(map[diff.Scope]*view),
	}
}

// Unregister 在下一次 Flush 采集最后的变化并移除实体，移除确认记录仍继续保留。
func (m *Manager) Unregister(key EntityKey) {
	value, exists := m.entities[key]
	if !exists {
		return
	}
	value.removing = true
	for _, subscriptions := range m.observers {
		if sub := subscriptions[key]; sub != nil {
			sub.closing = true
		}
	}
}

// Observe 只登记目标关系。首次订阅、重新订阅或 Scope 改变会分配新订阅 Id。
// 同一会话对同一实体、同一 Scope 重复订阅是幂等的。
func (m *Manager) Observe(observer string, key EntityKey, scope diff.Scope) uint64 {
	value, exists := m.entities[key]
	if !exists || value.removing {
		panic("replica: observing an unregistered entity")
	}
	if observer == "" || scope == 0 || scope&^diff.All != 0 {
		panic("replica: observer and valid Scope are required")
	}
	subscriptions := m.observers[observer]
	if subscriptions == nil {
		subscriptions = make(map[EntityKey]*subscription)
		m.observers[observer] = subscriptions
	}
	if sub := subscriptions[key]; sub != nil && !sub.closing && sub.scope == scope {
		return sub.id
	}
	if value.views[scope] == nil {
		value.views[scope] = new(view)
	}
	m.nextId++
	subscriptions[key] = &subscription{
		key:   key,
		id:    m.nextId,
		scope: scope,
	}
	return m.nextId
}

func (m *Manager) Unobserve(observer string, key EntityKey) {
	if sub := m.observers[observer][key]; sub != nil {
		sub.closing = true
	}
}

// ForgetObserver 用于已经断开的会话，释放其所有订阅和待确认移除记录。
func (m *Manager) ForgetObserver(observer string) {
	delete(m.observers, observer)
}

// Ack 的 observer 来自已认证的会话上下文，不从消息内容信任玩家身份。
func (m *Manager) Ack(observer string, ack *protocol.Ack) bool {
	if ack.SessionId != observer || !m.owner.matches(ack.Owner) || ack.Entity == nil {
		return false
	}
	key := EntityKey{
		Type: ack.Entity.Type,
		Id:   ack.Entity.Id,
	}
	sub := m.observers[observer][key]
	if sub == nil || sub.id != ack.SubscriptionId {
		return false
	}
	for index, pending := range sub.pending {
		if pending.frame.Sequence != ack.Sequence {
			continue
		}
		if pending.frame.Kind == protocol.Kind_KIND_REMOVE {
			delete(m.observers[observer], key)
			if len(m.observers[observer]) == 0 {
				delete(m.observers, observer)
			}
			return true
		}
		sub.ackedVersion = pending.frame.Version
		sub.ready = true
		clear(sub.pending[:index+1])
		sub.pending = sub.pending[index+1:]
		return true
	}
	return false
}

// RequestReset 用于客户端缺少基线、发现序号断层或应用失败后的恢复。
func (m *Manager) RequestReset(observer string, request *protocol.ResetRequest) bool {
	if request.SessionId != observer || !m.owner.matches(request.Owner) || request.Entity == nil {
		return false
	}
	key := EntityKey{
		Type: request.Entity.Type,
		Id:   request.Entity.Id,
	}
	sub := m.observers[observer][key]
	if sub == nil || sub.id != request.SubscriptionId || sub.closing {
		return false
	}
	sub.reset = true
	sub.ready = false
	sub.pending = nil
	return true
}

// Flush 先集中采集，再为各订阅组织消息。无新变化时仍需调用，以处理补发。
// 失败时保留已经采集的数据，下次 Flush 可继续处理；错误直接返回。
// 成功后调用方负责把 Changes 交给存档，把 Deliveries 按观察者发送顺序交给网络层。
func (m *Manager) Flush(now time.Time) (Result, error) {
	for _, value := range m.entities {
		update := value.source.commit()
		if proto.Size(update) != 0 {
			m.captures = append(m.captures, capture{
				entity: value,
				data:   update,
			})
		}
	}
	for len(m.captures) != 0 {
		current := m.captures[0]
		data, err := proto.Marshal(current.data)
		if err != nil {
			return Result{}, err
		}
		encoded := make(map[diff.Scope][]byte, len(current.entity.views))
		for scope := range current.entity.views {
			filtered := current.entity.source.filter(current.data, scope)
			if proto.Size(filtered) == 0 {
				continue
			}
			payload, err := proto.Marshal(filtered)
			if err != nil {
				return Result{}, err
			}
			encoded[scope] = payload
		}
		current.entity.revision++
		m.changes = append(m.changes, CapturedChange{
			Entity:   current.entity.key,
			Revision: current.entity.revision,
			Data:     data,
		})
		for scope, payload := range encoded {
			state := current.entity.views[scope]
			state.version++
			state.history = append(state.history, change{
				version: state.version,
				data:    payload,
			})
			state.bytes += len(payload)
			state.full = nil
			state.fullValid = false
			for len(state.history) > m.options.historyCount || state.bytes > m.options.historyBytes {
				state.bytes -= len(state.history[0].data)
				state.oldest = state.history[0].version
				state.history[0] = change{}
				state.history = state.history[1:]
			}
		}
		m.captures[0] = capture{}
		m.captures = m.captures[1:]
	}

	// 先生成所有需要的快照，序列化失败时不把半批消息交给网络层。
	for _, subscriptions := range m.observers {
		for key, sub := range subscriptions {
			if sub.closing {
				continue
			}
			value := m.entities[key]
			state := value.views[sub.scope]
			needsFull := sub.reset || !sub.ready && len(sub.pending) == 0 || sub.ready && sub.ackedVersion < state.oldest
			if needsFull && !state.fullValid {
				full := value.source.filter(value.source.snapshot(), sub.scope)
				data, err := proto.Marshal(full)
				if err != nil {
					return Result{}, err
				}
				state.full = data
				state.fullValid = true
			}
		}
	}

	result := Result{Changes: m.changes}
	m.changes = nil
	for observer, subscriptions := range m.observers {
		for key, sub := range subscriptions {
			if sub.closing {
				if len(sub.pending) == 0 || sub.pending[0].frame.Kind != protocol.Kind_KIND_REMOVE {
					sub.pending = nil
					frame := m.frame(observer, sub, protocol.Kind_KIND_REMOVE)
					frame.Version = sub.sentVersion
					sub.pending = append(sub.pending, flight{frame: frame})
				}
			} else {
				state := m.entities[key].views[sub.scope]
				if sub.reset || !sub.ready && len(sub.pending) == 0 || sub.ready && sub.ackedVersion < state.oldest {
					kind := protocol.Kind_KIND_INITIAL
					if sub.reset || sub.ready {
						kind = protocol.Kind_KIND_RESET
					}
					sub.pending = nil
					frame := m.frame(observer, sub, kind)
					frame.Version = state.version
					frame.Snapshot = state.full
					sub.sentVersion = state.version
					sub.ready = false
					sub.reset = false
					sub.pending = append(sub.pending, flight{frame: frame})
				} else if sub.ready && sub.sentVersion < state.version {
					frame := m.frame(observer, sub, protocol.Kind_KIND_DELTA)
					frame.BaseVersion = sub.sentVersion
					frame.Version = state.version
					for _, update := range state.history {
						if update.version > sub.sentVersion {
							frame.Updates = append(frame.Updates, update.data)
						}
					}
					sub.sentVersion = state.version
					sub.pending = append(sub.pending, flight{frame: frame})
				}
			}
			for index := range sub.pending {
				pending := &sub.pending[index]
				if pending.sentAt.IsZero() || now.Sub(pending.sentAt) >= m.options.retry {
					result.Deliveries = append(result.Deliveries, Delivery{
						Observer: observer,
						Frame:    pending.frame,
					})
					pending.sentAt = now
				}
			}
		}
	}
	for key, value := range m.entities {
		if value.removing {
			delete(m.entities, key)
		}
	}
	return result, nil
}

func (m *Manager) frame(observer string, sub *subscription, kind protocol.Kind) *protocol.Frame {
	sub.sequence++
	return &protocol.Frame{
		Owner:     m.owner.message(),
		SessionId: observer,
		Entity: &protocol.EntityKey{
			Type: sub.key.Type,
			Id:   sub.key.Id,
		},
		SubscriptionId: sub.id,
		Sequence:       sub.sequence,
		Scope:          uint32(sub.scope),
		Kind:           kind,
	}
}
