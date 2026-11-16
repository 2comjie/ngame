# 状态复制

`replica` 把 diff 的变化追踪接到客户端订阅生命周期：初始状态、后续增量、确认、补发、全量恢复和移除。一个 Manager 属于一个 Actor 激活实例，可以注册多种类型的复制实体。

所有 Manager 方法在所属 Actor 主循环内串行调用。框架不启动协程，不执行网络和数据库操作。跨 Actor 使用消息；网络协程只处理 `Flush` 产生的只读数据帧。

## 绑定状态

在 Actor 的 `OnStart` 中初始化。以下 `model.Player` 为 diff 生成的业务类型：

```go
func (p *PlayerActor) OnStart(ctx actor.StartContext) error {
    p.state.InitLink(diff.NewWriter())
    p.replication = replica.New(replica.Owner{
        Actor: replica.EntityKey{
            Type: ctx.Self.Key().Type,
            Id:   ctx.Self.Key().Id,
        },
        ActivationId: ctx.Self.ActivationId(),
    })
    p.entity = replica.EntityKey{
        Type: "player",
        Id:   p.Id,
    }
    p.replication.Register(p.entity, replica.Bind(
        p.state.Snapshot,
        p.state.Commit,
        model.FilterPlayer,
    ))
    return nil
}
```

`Bind` 通过泛型保存具体 Proto 类型，Manager 可以同时管理 Player、Tank 等不同数据源。Source 只绑定到一个实体和一个 Manager；注册后，业务不再自行调用该数据源的 Commit。

## 建立和取消订阅

`observer` 是一次逻辑客户端会话的唯一标识。重新连接必须换标识；网络适配层自行维护它到 Uid、连接的映射。客户端通过登录/订阅握手获得当前会话标识以及 Owner，包括 Actor 的 ActivationId。

```go
// 登录：固定订阅本人数据。
p.replication.Observe(sessionId, p.entity, diff.Public|diff.Private)

// AOI 的 Enter：订阅地图实体的公开字段。
room.replication.Observe(sessionId, tankKey, diff.Public)

// AOI 的 Leave：等待客户端确认移除。
room.replication.Unobserve(sessionId, tankKey)

// 实体销毁：停止注册，并通知所有订阅者移除。
room.replication.Unregister(tankKey)

// 会话已断开：直接释放该会话的全部复制记录。
room.replication.ForgetObserver(sessionId)
```

Observe 只登记关系。相同会话、实体、Scope 重复调用是幂等的；取消后重新订阅、Scope 改变，会分配新的订阅 Id。Scope 降级时通过新快照替换本地对象，清除客户端旧视图中的私有字段。

AOI 负责空间索引和可见关系，replica 不读取坐标。SLG 镜头订阅、养成本人订阅与战斗 AOI 都使用相同接口。Scope 必须由已授权的业务逻辑选择。

## 采集和发送

业务通过 setter 和容器修改状态，在业务处理结束或 Actor 的 OnUpdate 中执行：

```go
result, err := p.replication.Flush(time.Now())
if err != nil {
    return err
}
// result.Changes 交给业务存档逻辑，保留到持久化成功。
// result.Deliveries 交给网络发送层，保持同一观察者的交付顺序。
```

一次 Flush 会：

1. 每个已注册实体 Commit 一次，即使没有观察者也采集完整变化。
2. 按已使用的 Scope 生成增量、维护历史和版本。
3. 给新订阅者生成当前版本的初始快照，不重复应用快照前的增量。
4. 给已有订阅者组织从其已发送版本开始的有序增量。
5. 对未确认的初始化、增量、重置或移除消息执行超时补发。

同一实体、同一 Scope 共用历史和编码后的快照。只有过滤后存在变化时，该 Scope 的版本才递增；私有变化不会导致公开视图跳号。完整采集结果中的 `CapturedChange.Revision` 是当前实体注册期间的本地采集版本，不能当成跨 Actor 激活的数据库版本。

`Result.Changes` 含 Server 字段，只交给可信的存档/服务端消费者，不能原样广播。Flush 成功即把这份采集结果的消费责任交给业务，replica 不代替持久化队列或事务。客户端 Ack 不会确认数据已经落库。

网络层可以使用现有 `gate.Client.Push`：

```go
data, err := proto.Marshal(delivery.Frame)
if err != nil {
    return err
}
return gateClient.Push(ctx, uid, &packet.Message{
    Header: packet.Header{
        Route: replicaPushRoute, // 由业务分配协议路由。
    },
    Body: data,
})
```

`Delivery.Frame` 及其所有字节切片只读，不得修改或放回字节池。Manager 后续修改状态不会改变已输出消息，补发可能复用同一个消息对象。Push 返回成功也不推进客户端确认版本；网络发送失败无需把错误存在 Manager 中，待确认消息仍会在后续 Flush 中重试。

现有可靠有序连接负责传输。帧必须满足网络层的消息大小限制；历史字节上限不等于消息分片能力。大对象可按业务拆成多个复制实体，或在网络适配层处理分片。

## 客户端应用和反馈

协议位于 `protocol/replica.proto`，同时声明了 C# namespace `Ngame.Replica`。Go 接收器可直接使用，也定义了其他语言客户端应遵循的处理规则：

```go
receiver := replica.NewReceiver(owner, sessionId, replica.Handler{
    Snapshot: func(key replica.EntityKey, scope diff.Scope, data []byte) error {
        message := new(pb.Player)
        if err := proto.Unmarshal(data, message); err != nil {
            return err
        }
        // 根据 key 创建或替换本地对象。替换必须清除旧 Scope 的字段。
        local.LoadSnapshot(message)
        return nil
    },
    Delta: func(key replica.EntityKey, scope diff.Scope, updates [][]byte) error {
        for _, data := range updates {
            message := new(pb.Player)
            if err := proto.Unmarshal(data, message); err != nil {
                return err
            }
            local.Merge(message)
        }
        return nil
    },
    Remove: func(key replica.EntityKey) error {
        // 移除本地实体；实体尚未创建时也应成功。
        return nil
    },
})
```

回调通过 key.Type 选择业务类型和本地对象。普通接收副本不绑定 diff.Writer。收到 Frame 后调用 `Receive`：

```go
feedback, err := receiver.Receive(frame)
// Ack/Reset 通过网络发回状态所属 Actor；出现回调错误时也要处理 Reset。
// 错误由当前调用链处理。
```

所属 Actor 在处理认证后的回执消息时调用：

```go
p.replication.Ack(authenticatedSessionId, ack)
p.replication.RequestReset(authenticatedSessionId, request)
```

返回 bool 表示回执是否匹配当前待确认记录。会话身份从认证上下文取得，不信任 Ack.Body 自报的身份。Owner 和 ActivationId 用于路由和隔离旧激活，Receiver 的 Owner 从可信握手设置，不能自动信任任意收到的 Frame。重连和 Actor 重新激活时建立新订阅、创建新 Receiver，重新发送初始状态。

## 恢复与边界

- 初始快照确认前不发送增量，期间的新变化保存在历史中。
- 每条消息有订阅内 Sequence。重复消息不重复应用，仍返回确认；序号或基线断层会请求 Reset。
- Delta 的 Updates 是有序变化链，必须依次应用，不能直接拼接 Proto 或随意覆盖字段。
- 初始状态、重置和移除可越过缺失的旧消息；较旧序号不能覆盖新状态。
- 历史超过条数或字节限制时，落后的订阅获取最新快照。Reset 确认后继续后续变化。
- 移除记录保留到确认，或被新订阅替代，或由 ForgetObserver 随断线释放。实体在服务端注销后，移除仍会补发。
- Receiver 保留本会话见过的最新订阅序号，包括已移除实体的轻量记录，防止旧初始化/移除消息影响新对象；会话结束时释放 Receiver。
- 没有新业务变化时仍需定时 Flush，否则无法发现最后一条消息或 Ack 丢失。默认重试间隔为 1 秒，可通过 `WithRetryInterval` 配置。
- 默认每个实体、每个 Scope 保留 128 条、最多 1 MiB 的增量，通过 `WithHistoryLimits` 配置。慢观察者通过全量恢复，不无限阻塞历史释放。
- Snapshot/Delta 回调失败时不确认，返回错误并请求全量恢复；Manager 的序列化错误直接返回，已采集的变化保留供后续处理。

该模块负责结构化状态的复制，不提供空间 AOI 算法、客户端运动预测、插值或数据库实现。
