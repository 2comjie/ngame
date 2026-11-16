package replica_test

import (
	"errors"
	"testing"
	"time"

	"github.com/2comjie/ngame/diff"
	"github.com/2comjie/ngame/replica"
	"github.com/2comjie/ngame/replica/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var owner = replica.Owner{
	Actor: replica.EntityKey{
		Type: "room",
		Id:   "1",
	},
	ActivationId: "activation-1",
}

var tank = replica.EntityKey{
	Type: "tank",
	Id:   "1",
}

type state struct {
	values    map[string]*structpb.Value
	dirty     map[string]*structpb.Value
	commits   int
	snapshots int
}

func newState() *state {
	return &state{
		values: make(map[string]*structpb.Value),
		dirty:  make(map[string]*structpb.Value),
	}
}

func (s *state) set(key string, value float64) {
	s.values[key] = structpb.NewNumberValue(value)
	s.dirty[key] = structpb.NewNumberValue(value)
}

func (s *state) snapshot() *structpb.Struct {
	s.snapshots++
	return proto.Clone(&structpb.Struct{Fields: s.values}).(*structpb.Struct)
}

func (s *state) commit() *structpb.Struct {
	s.commits++
	result := &structpb.Struct{Fields: s.dirty}
	s.dirty = make(map[string]*structpb.Value)
	return result
}

func filter(data *structpb.Struct, scope diff.Scope) *structpb.Struct {
	result := &structpb.Struct{Fields: make(map[string]*structpb.Value)}
	for key, value := range data.Fields {
		if key == "hp" && scope&diff.Public != 0 || key == "gold" && scope&diff.Private != 0 || key == "audit" && scope&diff.Server != 0 {
			result.Fields[key] = proto.Clone(value).(*structpb.Value)
		}
	}
	return result
}

func register(m *replica.Manager, key replica.EntityKey, s *state) {
	m.Register(key, replica.Bind(s.snapshot, s.commit, filter))
}

func flush(t *testing.T, m *replica.Manager, now time.Time) replica.Result {
	t.Helper()
	result, err := m.Flush(now)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

type client struct {
	receiver *replica.Receiver
	handler  replica.Handler
	data     map[replica.EntityKey]*structpb.Struct
	fulls    int
	deltas   int
	removes  int
}

func newClient(session string) *client {
	c := &client{data: make(map[replica.EntityKey]*structpb.Struct)}
	c.handler = replica.Handler{
		Snapshot: func(key replica.EntityKey, _ diff.Scope, data []byte) error {
			value := new(structpb.Struct)
			if err := proto.Unmarshal(data, value); err != nil {
				return err
			}
			c.data[key] = value
			c.fulls++
			return nil
		},
		Delta: func(key replica.EntityKey, _ diff.Scope, updates [][]byte) error {
			for _, update := range updates {
				value := new(structpb.Struct)
				if err := proto.Unmarshal(update, value); err != nil {
					return err
				}
				if c.data[key].Fields == nil {
					c.data[key].Fields = make(map[string]*structpb.Value)
				}
				for field, data := range value.Fields {
					c.data[key].Fields[field] = data
				}
			}
			c.deltas++
			return nil
		},
		Remove: func(key replica.EntityKey) error {
			delete(c.data, key)
			c.removes++
			return nil
		},
	}
	c.receiver = replica.NewReceiver(owner, session, c.handler)
	return c
}

// wire 走真实的协议序列化，模拟网络层交付一个独立的 Frame。
func wire(t *testing.T, frame *protocol.Frame) *protocol.Frame {
	t.Helper()
	data, err := proto.Marshal(frame)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(protocol.Frame)
	if err := proto.Unmarshal(data, decoded); err != nil {
		t.Fatal(err)
	}
	return decoded
}

func receive(t *testing.T, c *client, frame *protocol.Frame) replica.Feedback {
	t.Helper()
	feedback, err := c.receiver.Receive(wire(t, frame))
	if err != nil {
		t.Fatal(err)
	}
	return feedback
}

func deliver(t *testing.T, m *replica.Manager, clients map[string]*client, result replica.Result) {
	t.Helper()
	for _, delivery := range result.Deliveries {
		feedback := receive(t, clients[delivery.Observer], delivery.Frame)
		if feedback.Reset != nil {
			if !m.RequestReset(delivery.Observer, feedback.Reset) {
				t.Fatal("reset request rejected")
			}
		}
		if feedback.Ack != nil && !m.Ack(delivery.Observer, feedback.Ack) {
			t.Fatal("ack rejected")
		}
	}
}

func TestInitialDeltaScopesAndLateObserver(t *testing.T) {
	now := time.Unix(100, 0)
	m := replica.New(owner)
	s := newState()
	s.set("hp", 100)
	s.set("gold", 10)
	s.set("audit", 1)
	register(m, tank, s)
	m.Observe("self", tank, diff.Public|diff.Private)
	m.Observe("near", tank, diff.Public)
	m.Observe("near2", tank, diff.Public)
	clients := map[string]*client{
		"self":  newClient("self"),
		"near":  newClient("near"),
		"near2": newClient("near2"),
		"late":  newClient("late"),
	}
	initial := flush(t, m, now)
	if s.commits != 1 || s.snapshots != 2 || len(initial.Changes) != 1 || len(initial.Deliveries) != 3 {
		t.Fatalf("initial collection not shared: commits=%d snapshots=%d result=%+v", s.commits, s.snapshots, initial)
	}
	for _, delivery := range initial.Deliveries {
		if delivery.Frame.Kind != protocol.Kind_KIND_INITIAL {
			t.Fatal("new observer received delta")
		}
	}
	deliver(t, m, clients, initial)
	if clients["near"].data[tank].Fields["gold"] != nil || clients["self"].data[tank].Fields["audit"] != nil {
		t.Fatal("private/server fields leaked")
	}
	stored := new(structpb.Struct)
	if err := proto.Unmarshal(initial.Changes[0].Data, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Fields["audit"] == nil {
		t.Fatal("server change not captured for persistence")
	}
	s.set("gold", 20)
	private := flush(t, m, now.Add(time.Millisecond))
	if len(private.Deliveries) != 1 || private.Deliveries[0].Observer != "self" {
		t.Fatal("private-only change affected public view")
	}
	deliver(t, m, clients, private)
	s.set("hp", 80)
	m.Observe("late", tank, diff.Public)
	updated := flush(t, m, now.Add(2*time.Millisecond))
	for _, delivery := range updated.Deliveries {
		if delivery.Observer == "late" {
			if delivery.Frame.Kind != protocol.Kind_KIND_INITIAL || delivery.Frame.Version != 2 {
				t.Fatalf("wrong late baseline: %v", delivery.Frame)
			}
		} else if delivery.Frame.Kind != protocol.Kind_KIND_DELTA {
			t.Fatal("existing observer received another full state")
		}
	}
	deliver(t, m, clients, updated)
	for session, c := range clients {
		if c.data[tank].Fields["hp"].GetNumberValue() != 80 {
			t.Fatalf("%s did not converge", session)
		}
	}
}

func TestInitialAndLastDeltaLossAndLostAck(t *testing.T) {
	now := time.Unix(100, 0)
	m := replica.New(owner)
	s := newState()
	s.set("hp", 100)
	register(m, tank, s)
	m.Observe("client", tank, diff.Public)
	c := newClient("client")
	first := flush(t, m, now).Deliveries[0].Frame // 丢掉首次全量。
	s.set("hp", 80)
	if result := flush(t, m, now.Add(time.Millisecond)); len(result.Deliveries) != 0 {
		t.Fatal("delta sent before baseline acknowledgment")
	}
	retry := flush(t, m, now.Add(time.Second)).Deliveries[0].Frame
	if !proto.Equal(first, retry) {
		t.Fatal("initial retry changed its baseline")
	}
	feedback := receive(t, c, retry)
	if !m.Ack("client", feedback.Ack) {
		t.Fatal("initial ack rejected")
	}
	delta := flush(t, m, now.Add(time.Second+time.Millisecond)).Deliveries[0].Frame // 丢掉最后一条增量。
	retry = flush(t, m, now.Add(2*time.Second+time.Millisecond)).Deliveries[0].Frame
	if !proto.Equal(delta, retry) {
		t.Fatal("last delta was not retried without new changes")
	}
	receive(t, c, retry) // 丢掉 Ack。
	retry = flush(t, m, now.Add(3*time.Second+time.Millisecond)).Deliveries[0].Frame
	feedback = receive(t, c, retry)
	if c.deltas != 1 || c.data[tank].Fields["hp"].GetNumberValue() != 80 || !m.Ack("client", feedback.Ack) {
		t.Fatal("lost ack caused duplicate application or failed convergence")
	}
	if result := flush(t, m, now.Add(5*time.Second)); len(result.Deliveries) != 0 {
		t.Fatal("confirmed messages still retrying")
	}
}

func TestHistoryOverflowResetsAndRejectsOldFrames(t *testing.T) {
	oneChangeBytes := proto.Size(&structpb.Struct{Fields: map[string]*structpb.Value{"hp": structpb.NewNumberValue(80)}})
	for _, test := range []struct {
		name  string
		count int
		bytes int
	}{
		{
			name:  "count",
			count: 1,
			bytes: 1024,
		},
		{
			name:  "bytes",
			count: 128,
			bytes: oneChangeBytes,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			now := time.Unix(100, 0)
			m := replica.New(owner, replica.WithHistoryLimits(test.count, test.bytes))
			s := newState()
			register(m, tank, s)
			m.Observe("client", tank, diff.Public)
			c := newClient("client")
			deliver(t, m, map[string]*client{"client": c}, flush(t, m, now))
			s.set("hp", 80)
			old := flush(t, m, now.Add(time.Millisecond)).Deliveries[0].Frame
			s.set("hp", 60)
			latest := flush(t, m, now.Add(2*time.Millisecond)).Deliveries[0].Frame
			if latest.Kind != protocol.Kind_KIND_RESET {
				t.Fatal("expired history did not produce reset")
			}
			feedback := receive(t, c, latest)
			if !m.Ack("client", feedback.Ack) {
				t.Fatal("reset ack rejected")
			}
			receive(t, c, old)
			if c.data[tank].Fields["hp"].GetNumberValue() != 60 {
				t.Fatal("late frame rolled back reset state")
			}
		})
	}
}

func TestLeaveBeforeInitialArrives(t *testing.T) {
	now := time.Unix(100, 0)
	m := replica.New(owner)
	register(m, tank, newState())
	m.Observe("client", tank, diff.Public)
	initial := flush(t, m, now).Deliveries[0].Frame
	m.Unobserve("client", tank)
	remove := flush(t, m, now.Add(time.Millisecond)).Deliveries[0].Frame
	c := newClient("client")
	feedback := receive(t, c, remove)
	if !m.Ack("client", feedback.Ack) {
		t.Fatal("remove ack rejected before initial delivery")
	}
	receive(t, c, initial)
	if c.fulls != 0 || c.data[tank] != nil {
		t.Fatal("late initial revived an entity that already left")
	}
}

func TestReceiverApplicationFailureRequestsFreshSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	m := replica.New(owner)
	s := newState()
	register(m, tank, s)
	m.Observe("client", tank, diff.Public)
	fail := errors.New("application failed")
	fulls := 0
	r := replica.NewReceiver(owner, "client", replica.Handler{
		Snapshot: func(replica.EntityKey, diff.Scope, []byte) error {
			fulls++
			return nil
		},
		Delta:  func(replica.EntityKey, diff.Scope, [][]byte) error { return fail },
		Remove: func(replica.EntityKey) error { return nil },
	})
	initial := flush(t, m, now).Deliveries[0].Frame
	feedback, err := r.Receive(initial)
	if err != nil || !m.Ack("client", feedback.Ack) {
		t.Fatal("initial failed")
	}
	s.set("hp", 80)
	delta := flush(t, m, now.Add(time.Millisecond)).Deliveries[0].Frame
	feedback, err = r.Receive(delta)
	if !errors.Is(err, fail) || feedback.Ack != nil || feedback.Reset == nil || !m.RequestReset("client", feedback.Reset) {
		t.Fatal("failed application was acknowledged or failed to request reset")
	}
	reset := flush(t, m, now.Add(2*time.Millisecond)).Deliveries[0].Frame
	feedback, err = r.Receive(reset)
	if err != nil || !m.Ack("client", feedback.Ack) || fulls != 2 {
		t.Fatal("application failure did not recover through snapshot")
	}
}

func TestGapAndMissingBaselineRequestReset(t *testing.T) {
	now := time.Unix(100, 0)
	m := replica.New(owner)
	s := newState()
	register(m, tank, s)
	m.Observe("client", tank, diff.Public)
	c := newClient("client")
	deliver(t, m, map[string]*client{"client": c}, flush(t, m, now))
	s.set("hp", 80)
	flush(t, m, now.Add(time.Millisecond)) // 丢掉一条。
	s.set("hp", 60)
	newer := flush(t, m, now.Add(2*time.Millisecond)).Deliveries[0].Frame
	for _, receiver := range []*client{c, newClient("client")} {
		feedback := receive(t, receiver, newer)
		if feedback.Ack != nil || feedback.Reset == nil || !m.RequestReset("client", feedback.Reset) {
			t.Fatal("gap/missing baseline did not request recovery")
		}
	}
	reset := flush(t, m, now.Add(3*time.Millisecond))
	deliver(t, m, map[string]*client{"client": c}, reset)
	if c.data[tank].Fields["hp"].GetNumberValue() != 60 {
		t.Fatal("reset did not restore latest state")
	}
}

func TestRemoveLossReentryAndScopeChange(t *testing.T) {
	now := time.Unix(100, 0)
	m := replica.New(owner)
	s := newState()
	s.set("hp", 100)
	s.set("gold", 10)
	register(m, tank, s)
	firstId := m.Observe("client", tank, diff.Public)
	if again := m.Observe("client", tank, diff.Public); again != firstId {
		t.Fatal("Observe is not idempotent")
	}
	c := newClient("client")
	clients := map[string]*client{"client": c}
	initial := flush(t, m, now)
	deliver(t, m, clients, initial)
	m.Unobserve("client", tank)
	remove := flush(t, m, now.Add(time.Millisecond)).Deliveries[0].Frame // 移除丢失。
	if remove.Kind != protocol.Kind_KIND_REMOVE {
		t.Fatal("no removal generated")
	}
	retry := flush(t, m, now.Add(time.Second+time.Millisecond))
	deliver(t, m, clients, retry)
	receive(t, c, initial.Deliveries[0].Frame)
	if c.data[tank] != nil || c.removes != 1 {
		t.Fatal("late initial resurrected removed entity")
	}
	m.Observe("client", tank, diff.Public|diff.Private)
	reentered := flush(t, m, now.Add(2*time.Second))
	deliver(t, m, clients, reentered)
	receive(t, c, remove)
	if c.data[tank].Fields["gold"].GetNumberValue() != 10 {
		t.Fatal("late removal damaged new subscription")
	}
	m.Observe("client", tank, diff.Public)
	deliver(t, m, clients, flush(t, m, now.Add(3*time.Second)))
	if c.data[tank].Fields["gold"] != nil {
		t.Fatal("scope downgrade retained private fields")
	}
	m.Unregister(tank)
	deliver(t, m, clients, flush(t, m, now.Add(4*time.Second)))
	if c.data[tank] != nil {
		t.Fatal("unregister did not remove entity")
	}
	register(m, tank, newState())
	m.Observe("client", tank, diff.Public)
	deliver(t, m, clients, flush(t, m, now.Add(5*time.Second)))
	if c.data[tank] == nil {
		t.Fatal("reused entity Id did not create a new subscription")
	}
}

func TestSessionAndActivationIsolation(t *testing.T) {
	now := time.Unix(100, 0)
	m := replica.New(owner)
	s := newState()
	register(m, tank, s)
	m.Observe("old-session", tank, diff.Public)
	old := flush(t, m, now).Deliveries[0].Frame
	feedback := receive(t, newClient("old-session"), old)
	m.ForgetObserver("old-session")
	m.Observe("new-session", tank, diff.Public)
	if m.Ack("new-session", feedback.Ack) || m.Ack("old-session", feedback.Ack) {
		t.Fatal("old session acknowledged new subscription")
	}
	if _, err := newClient("new-session").receiver.Receive(old); !errors.Is(err, replica.ErrStaleFrame) {
		t.Fatal("old-session frame accepted")
	}
	restarted := owner
	restarted.ActivationId = "activation-2"
	m2 := replica.New(restarted)
	register(m2, tank, newState())
	m2.Observe("old-session", tank, diff.Public)
	flush(t, m2, now)
	if m2.Ack("old-session", feedback.Ack) {
		t.Fatal("old activation acknowledged restarted manager")
	}
}

func TestCaptureWithoutObserversAndRetryEncodingFailure(t *testing.T) {
	now := time.Unix(100, 0)
	m := replica.New(owner)
	s := newState()
	s.set("audit", 10)
	register(m, tank, s)
	result := flush(t, m, now)
	if len(result.Changes) != 1 || len(result.Deliveries) != 0 {
		t.Fatal("unobserved entity was not captured for persistence")
	}
	fail := true
	m2 := replica.New(owner)
	m2.Register(tank, replica.Bind(s.snapshot, s.commit, func(data *structpb.Struct, scope diff.Scope) *structpb.Struct {
		if fail {
			return &structpb.Struct{Fields: map[string]*structpb.Value{"\xff": structpb.NewNullValue()}}
		}
		return filter(data, scope)
	}))
	m2.Observe("client", tank, diff.Public)
	s.set("hp", 80)
	if _, err := m2.Flush(now); err == nil {
		t.Fatal("expected encoding error")
	}
	fail = false
	result = flush(t, m2, now.Add(time.Second))
	if len(result.Changes) != 1 || len(result.Deliveries) != 1 {
		t.Fatal("Commit data was lost after encoding failure")
	}
}
