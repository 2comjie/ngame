package replica

import (
	"github.com/2comjie/ngame/diff"
	"google.golang.org/protobuf/proto"
)

// Source 绑定一个复制实体的状态。只能注册给一个 Manager，Commit 由该 Manager 独占。
type Source interface {
	snapshot() proto.Message
	commit() proto.Message
	filter(proto.Message, diff.Scope) proto.Message
}

type binding[T proto.Message] struct {
	snapshotFunc func() T
	commitFunc   func() T
	filterFunc   func(T, diff.Scope) T
}

// Bind 直接适配 diff 生成的 Snapshot、Commit 和 FilterXXX。
func Bind[T proto.Message](snapshot func() T, commit func() T, filter func(T, diff.Scope) T) Source {
	if snapshot == nil || commit == nil || filter == nil {
		panic("replica: Snapshot, Commit and Filter are required")
	}
	return &binding[T]{
		snapshotFunc: snapshot,
		commitFunc:   commit,
		filterFunc:   filter,
	}
}

func (b *binding[T]) snapshot() proto.Message {
	return b.snapshotFunc()
}

func (b *binding[T]) commit() proto.Message {
	return b.commitFunc()
}

func (b *binding[T]) filter(data proto.Message, scope diff.Scope) proto.Message {
	return b.filterFunc(data.(T), scope)
}
