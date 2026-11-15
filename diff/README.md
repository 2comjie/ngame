# 属性增量同步

业务通过生成的 setter、`Map`、`Slice` 修改状态，变化沿父链接汇入根 `Writer`。`Commit()` 合并并导出本轮增量 Proto，同时清空 Writer；`Snapshot()` 导出独立的全量 Proto。

## 定义与生成

定义文件使用 `diff_fast` 构建标签，字段编号在 `1..999` 内，不复用已删除的编号：

```go
//go:build diff_fast

package model

type Player struct {
    Id    int64           `diff:"1"`
    Hp    int32           `diff:"2"`
    Items map[int64]*Item `diff:"3"`
}

type Item struct {
    Count int64 `diff:"1"`
}
```

项目根目录的 `diffgen.yaml`：

```yaml
source_root: ./model
packages:
  - ./model
proto:
  dir: ./proto
  go_out: .
```

在使用方模块根目录执行：

```sh
go run github.com/2comjie/ngame/cmd/diff-gen -config diffgen.yaml
protoc -I proto --go_out=. --go_opt=module=你的模块路径 proto/*.proto
```

需要安装 `protoc` 和 `protoc-gen-go`。生成器输出 `model/*_diff.gen.go` 和 `.proto`，不自动编译 Proto。正常构建不加 `diff_fast` 标签，使用生成的运行时对象。Proto 的 C# namespace 前缀为 `Ngame.Generated`；客户端需要按增量字段实现合并。

## 修改与同步

```go
player := new(model.Player)
player.InitLink(diff.NewWriter())
player.SetId(1001)
player.SetHp(100)
item := new(model.Item)
item.SetCount(5)
player.Items().Store(2001, item)

baseline := player.Snapshot()
player.Commit() // 本轮初始化变化已包含在全量中。

replica := new(model.Player)
replica.LoadSnapshot(baseline)

player.SetHp(80)
item.SetCount(4)
update := player.Commit()
replica.Merge(update)
```

- 普通字段输出最终设置值；对象支持设置、清除和子字段更新。
- Map 按 Key 设置、删除、更新子字段，支持整体清空。
- Slice 的修改及其元素的子字段修改均输出整个列表。
- 同一位置反复修改由 Writer 合并；不与旧基线比较，改回原值仍会输出一次设置。
- `LoadSnapshot()` 恢复全量和父链接，不记录变化。重置基线前由调用方处理已有待提交变化。

## 对象与生命周期

允许多个位置引用同一个子对象，变化沿所有父链接传播；不允许循环引用。`AddParent()` 在挂接时检查所有祖先，包括 Map、Slice 和绑定了 Writer 的节点，发现循环直接 panic。

对象不可按值复制。对象修改、快照、提交在同一 Actor 或串行执行环境中进行，提交后的 Proto 再交给网络层。Writer 在提交前可以持有对象引用，不能把未提交的运行时记录交给其他线程。

`Merge()` 应用增量时，若接收对象绑定了 Writer，也会记录这些本地变化。普通接收副本不绑定 Writer 即可。

版本号、订阅权限、消息发送和重连恢复由上层管理。每轮只提交一次，再向各订阅者分发同一份增量。
