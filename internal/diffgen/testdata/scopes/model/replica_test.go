package model

import (
	"testing"
	"time"

	pb "example.com/scopes/pb/model"
	"github.com/2comjie/ngame/diff"
	"github.com/2comjie/ngame/replica"
	"github.com/2comjie/ngame/replica/protocol"
	"google.golang.org/protobuf/proto"
)

func TestReplicaWithGeneratedState(t *testing.T) {
	owner := replica.Owner{
		Actor: replica.EntityKey{
			Type: "player",
			Id:   "1",
		},
		ActivationId: "activation-1",
	}
	key := replica.EntityKey{
		Type: "player",
		Id:   "1",
	}
	m := replica.New(owner)
	state := newPlayer()
	m.Register(key, replica.Bind(state.Snapshot, state.Commit, FilterPlayer))
	box := new(NumberBox)
	box.InitLink(diff.NewWriter())
	box.SetValue(5)
	m.Register(replica.EntityKey{
		Type: "box",
		Id:   "1",
	}, replica.Bind(
		func() *pb.NumberBox { return SnapshotNumberBox(box) },
		func() *pb.NumberBox { return CommitNumberBox(box) },
		FilterNumberBox,
	))
	client := new(Player)
	receiver := replica.NewReceiver(owner, "session", replica.Handler{
		Snapshot: func(_ replica.EntityKey, _ diff.Scope, data []byte) error {
			message := new(pb.Player)
			if err := proto.Unmarshal(data, message); err != nil {
				return err
			}
			client.LoadSnapshot(message)
			return nil
		},
		Delta: func(_ replica.EntityKey, _ diff.Scope, updates [][]byte) error {
			for _, data := range updates {
				message := new(pb.Player)
				if err := proto.Unmarshal(data, message); err != nil {
					return err
				}
				client.Merge(message)
			}
			return nil
		},
		Remove: func(replica.EntityKey) error {
			client = nil
			return nil
		},
	})
	m.Observe("session", key, diff.Public|diff.Private)
	now := time.Unix(100, 0)
	sync := func() replica.Result {
		t.Helper()
		result, err := m.Flush(now)
		if err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Millisecond)
		for _, delivery := range result.Deliveries {
			wire, err := proto.Marshal(delivery.Frame)
			if err != nil {
				t.Fatal(err)
			}
			frame := new(protocol.Frame)
			if err := proto.Unmarshal(wire, frame); err != nil {
				t.Fatal(err)
			}
			feedback, err := receiver.Receive(frame)
			if err != nil || feedback.Ack == nil || !m.Ack("session", feedback.Ack) {
				t.Fatalf("failed to apply/ack generated state: %v", err)
			}
		}
		return result
	}
	initial := sync()
	if len(initial.Changes) != 2 {
		t.Fatal("heterogeneous sources were not both captured")
	}
	state.SetSecret(0)
	state.SetAudit(50)
	state.GetChild().SetPublic(0)
	item, _ := state.Items().Load(1)
	item.SetPrivate(100)
	state.Items().Delete(2)
	state.Stats().Clear()
	state.History().Clear()
	state.GetBox().GetValue().SetPrivate(200)
	result := sync()
	if len(result.Changes) != 1 {
		t.Fatal("no complete delta for persistence")
	}
	if !proto.Equal(client.Snapshot(), FilterPlayer(state.Snapshot(), diff.Public|diff.Private)) {
		t.Fatal("generated state did not converge through replica")
	}
	stored := new(pb.Player)
	if err := proto.Unmarshal(result.Changes[0].Data, stored); err != nil {
		t.Fatal(err)
	}
	if stored.AuditUpdate == nil || *stored.AuditUpdate != 50 {
		t.Fatal("Server update was lost from persistence capture")
	}
	m.Unobserve("session", key)
	sync()
	if client != nil {
		t.Fatal("entity did not leave client view")
	}
}
