package model

import (
	"testing"

	"example.com/scopes/child"
	pbChild "example.com/scopes/pb/child"
	pb "example.com/scopes/pb/model"
	"github.com/2comjie/ngame/diff"
	"google.golang.org/protobuf/proto"
)

func newChild() *child.Data {
	value := new(child.Data)
	value.SetPublic(1)
	value.SetPrivate(2)
	value.SetServer(3)
	value.SetDefault(4)
	return value
}

func newPlayer() *Player {
	value := new(Player)
	value.InitLink(diff.NewWriter())
	value.SetId(1)
	value.SetSecret(2)
	value.SetAudit(3)
	value.SetDefault(4)
	value.SetChild(newChild())
	value.SetReplacement(newChild())
	value.SetPrivateData(newChild())
	value.SetServerData(newChild())
	value.Items().Store(1, newChild())
	value.Items().Store(2, newChild())
	value.Items().Store(3, newChild())
	value.History().Append(newChild())
	value.Stats().Store(1, 10)
	value.Numbers().Append(10)
	value.PrivateMap().Store(1, 20)
	value.ServerList().Append(30)
	box := new(ChildBox)
	box.SetValue(newChild())
	value.SetBox(box)
	numberBox := new(NumberBox)
	numberBox.SetValue(10)
	value.SetNumberBox(numberBox)
	return value
}

func TestFullScopes(t *testing.T) {
	full := newPlayer().Snapshot()
	original := proto.Clone(full)
	for _, test := range []struct {
		name  string
		scope diff.Scope
	}{
		{name: "public", scope: diff.Public},
		{name: "owner", scope: diff.Public | diff.Private},
		{name: "server", scope: diff.Server},
		{name: "all", scope: diff.All},
		{name: "none", scope: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			filtered := FilterPlayer(full, test.scope)
			want := new(pb.Player)
			if test.scope&diff.Public != 0 {
				want.Id = 1
				want.Child = expectedChild(test.scope)
				want.Replacement = expectedChild(test.scope)
				want.Items = map[int64]*pbChild.Data{
					1: expectedChild(test.scope),
					2: expectedChild(test.scope),
					3: expectedChild(test.scope),
				}
				want.History = []*pbChild.Data{expectedChild(test.scope)}
				want.Stats = map[int64]int64{1: 10}
				want.Numbers = []int64{10}
				want.Box = &pb.ChildBox{Value: expectedChild(test.scope)}
				want.NumberBox = &pb.NumberBox{Value: 10}
			}
			if test.scope&diff.Private != 0 {
				want.Secret = 2
				want.PrivateData = expectedChild(test.scope)
				want.PrivateMap = map[int64]int64{1: 20}
			}
			if test.scope&diff.Server != 0 {
				want.Audit = 3
				want.Default = 4
				want.ServerData = expectedChild(test.scope)
				want.ServerList = []int64{30}
			}
			if !proto.Equal(filtered, want) {
				t.Fatalf("filtered=%v, want=%v", filtered, want)
			}
		})
	}
	if !proto.Equal(full, original) {
		t.Fatal("filtering modified the full source")
	}
}

func expectedChild(scope diff.Scope) *pbChild.Data {
	value := new(pbChild.Data)
	if scope&diff.Public != 0 {
		value.Public = 1
	}
	if scope&diff.Private != 0 {
		value.Private = 2
	}
	if scope&diff.Server != 0 {
		value.Server = 3
		value.Default = 4
	}
	return value
}

func TestFilteredDeltaRoundTrip(t *testing.T) {
	state := newPlayer()
	scopes := []diff.Scope{diff.Public, diff.Public | diff.Private, diff.Server, diff.All}
	replicas := make(map[diff.Scope]*Player)
	for _, scope := range scopes {
		replica := new(Player)
		replica.LoadSnapshot(FilterPlayer(state.Snapshot(), scope))
		replicas[scope] = replica
	}
	state.Commit()
	state.SetId(0) // optional 零值更新不能被当作空更新丢弃。
	state.SetSecret(20)
	state.SetAudit(30)
	state.SetDefault(40)
	state.GetChild().SetPublic(11)
	state.GetChild().SetPrivate(22)
	state.GetChild().SetServer(33)
	emptyPublic := new(child.Data)
	emptyPublic.SetPrivate(99)
	state.SetReplacement(emptyPublic)
	privateItem, _ := state.Items().Load(1)
	privateItem.SetPrivate(22)
	publicItem, _ := state.Items().Load(2)
	publicItem.SetPublic(0)
	state.Items().Delete(3)
	state.Items().Store(4, emptyPublic)
	state.History().GetValue(0).SetPrivate(22)
	state.History().Append(emptyPublic)
	state.Stats().Store(1, 0)
	state.Numbers().SetValue(0, 0)
	state.GetPrivateData().SetPrivate(22)
	state.GetServerData().SetServer(33)
	state.GetBox().GetValue().SetPrivate(22)
	state.GetNumberBox().SetValue(0)
	state.PrivateMap().Store(1, 0)
	state.ServerList().SetValue(0, 0)
	update := state.Commit()
	public := FilterPlayer(update, diff.Public)
	if public.IdUpdate == nil || *public.IdUpdate != 0 {
		t.Fatal("lost optional zero update")
	}
	if public.ItemsUpdate[1] != nil || public.BoxDiff != nil {
		t.Fatal("private-only nested update was retained")
	}
	if public.SecretUpdate != nil || public.AuditUpdate != nil || public.PrivateMapSet != nil || public.ServerListUpdated {
		t.Fatal("hidden delta fields leaked")
	}
	apply := func(update *pb.Player) {
		t.Helper()
		original := proto.Clone(update)
		for _, scope := range scopes {
			data, err := proto.Marshal(FilterPlayer(update, scope))
			if err != nil {
				t.Fatal(err)
			}
			decoded := new(pb.Player)
			if err := proto.Unmarshal(data, decoded); err != nil {
				t.Fatal(err)
			}
			replicas[scope].Merge(decoded)
			want := FilterPlayer(state.Snapshot(), scope)
			if !proto.Equal(replicas[scope].Snapshot(), want) {
				t.Fatalf("scope=%d replica=%v want=%v", scope, replicas[scope].Snapshot(), want)
			}
		}
		if !proto.Equal(update, original) {
			t.Fatal("filtering modified the delta source")
		}
	}
	apply(update)

	state.SetReplacement(nil)
	state.Items().Clear()
	state.Items().Store(5, newChild())
	state.Stats().Clear()
	state.Stats().Store(2, 20)
	state.History().Clear()
	state.Numbers().Clear()
	state.PrivateMap().Clear()
	state.ServerList().Clear()
	state.GetBox().GetValue().SetPublic(0)
	update = state.Commit()
	public = FilterPlayer(update, diff.Public)
	if !public.ItemsClear || !public.StatsClear || !public.HistoryUpdated || !public.NumbersUpdated {
		t.Fatal("lost clear operations")
	}
	if public.PrivateMapClear || public.ServerListUpdated {
		t.Fatal("hidden clear operations leaked")
	}
	apply(update)
}

func TestHiddenOnlyDelta(t *testing.T) {
	state := newPlayer()
	state.Commit()
	state.SetAudit(30)
	state.GetChild().SetServer(30)
	item, _ := state.Items().Load(1)
	item.SetServer(30)
	state.GetBox().GetValue().SetServer(30)
	update := state.Commit()
	for _, scope := range []diff.Scope{0, diff.Public, diff.Public | diff.Private} {
		if filtered := FilterPlayer(update, scope); proto.Size(filtered) != 0 {
			t.Fatalf("scope=%d retained hidden-only delta: %v", scope, filtered)
		}
	}
	if !proto.Equal(FilterPlayer(update, diff.All), update) {
		t.Fatal("all scope lost a delta")
	}
}

func TestFilterCopiesMutableData(t *testing.T) {
	state := newPlayer()
	full := state.Snapshot()
	fullCopy := proto.Clone(full)
	filtered := FilterPlayer(full, diff.All)
	filtered.Child.Public = 99
	filtered.Items[1].Public = 99
	delete(filtered.Items, 2)
	filtered.History[0].Public = 99
	filtered.Stats[1] = 99
	filtered.Numbers[0] = 99
	filtered.Box.Value.Public = 99
	if !proto.Equal(full, fullCopy) {
		t.Fatal("filtered full aliases its source")
	}
	state.Commit()
	state.SetId(0)
	state.GetChild().SetPublic(10)
	state.SetReplacement(newChild())
	state.Items().Store(4, newChild())
	item, _ := state.Items().Load(1)
	item.SetPublic(10)
	state.Items().Delete(2)
	state.History().Append(newChild())
	state.Stats().Store(2, 20)
	state.Stats().Delete(1)
	state.Numbers().Append(20)
	update := state.Commit()
	updateCopy := proto.Clone(update)
	filtered = FilterPlayer(update, diff.All)
	*filtered.IdUpdate = 99
	*filtered.GetChildUpdate().PublicUpdate = 99
	filtered.GetReplacementSet().Public = 99
	filtered.ItemsSet[4].Public = 99
	*filtered.ItemsUpdate[1].PublicUpdate = 99
	filtered.ItemsDelete[0] = 99
	filtered.HistoryUpdate[0].Public = 99
	filtered.StatsSet[2] = 99
	filtered.StatsDelete[0] = 99
	filtered.NumbersUpdate[0] = 99
	if !proto.Equal(update, updateCopy) {
		t.Fatal("filtered delta aliases its source")
	}
}
