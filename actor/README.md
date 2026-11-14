# Actor

System 管理本节点的激活、主循环和生命周期。业务通过独立的 Client/Ref 调用 Actor， System 不提供公开的
Ask/Tell。业务实体由上层创建，Loader 只负责加载已有实体。

## 包与文件

业务统一导入 `github.com/2comjie/ngame/actor`，RPC 协议放在 `internal/protocol`，由框架内部使用。

| 文件 | 职责 |
| --- | --- |
| `actor.go` | Actor、Handle、Key、消息和回调上下文 |
| `system.go` | 生命周期、类型注册和配置选项 |
| `runner.go` | 状态、回调生命周期和主循环调度 |
| `mailbox.go` | 投递、同步等待、消息执行和取消 |
| `timer.go` | 定时任务、取消和调度堆 |
| `guard.go` | Redis 归属抢占、续租和释放 |
| `client.go` | Client、Ref、激活策略和远程调用 |
| `locator.go` | 归属查询、本地缓存和刷新合并 |
| `rpc.go` | Actor RPC 服务端适配 |

Lua 脚本集中在 `lua`，测试按系统、邮箱、路由缓存、分布式调用和进程恢复组织。

## 服务端

```go
// nodeId 每次进程启动使用新值，与服务注册中的 Instance.Id 保持一致。
system := actor.New(actor.NewGuard(nodeId, redisClient))
system.Register("room", loadRoom)
system.Register("hall", loadHall)

rpcServer := rpc.NewServer(nodeId)
system.RegisterRPC(rpcServer)
```

在现有 NodeApp 中，把 `system` 放入 `node.Config.Components`，把 `rpcServer` 传给
`node.Config.RPCServer`，由 NodeApp 统一启动、注册与关闭。Actor RPC 与 Node RPC 共用一个 Server。该节点注册的服务名为 `game`
，同服务内参与激活的节点都需要注册 `room`、`hall` Loader。

Loader 返回已装载业务状态的 Actor。数据不存在时返回 `actor.ErrEntityNotFound`； 只有为该类型配置了 `WithOnNotFound`
，才会调用业务提供的缺失处理回调并重新加载一次。

```go
func (p *Player) OnMessage(ctx actor.MessageContext, message actor.Message) ([]byte, error) {
switch message.Method {
case "get-profile":
return json.Marshal(p.profile)
case "save":
return nil, p.save(ctx)
default:
return nil, fmt.Errorf("未知方法: %s", message.Method)
}
}
```

`OnMessage`、`OnUpdate`、定时任务和闭包任务在同一个主循环中串行执行。

## Client 与 Ref

Client 初始化一次，按服务复用 gRPC 连接；Locator 在多个调用方之间共享。 以下使用明文凭证做本机调试，也可以通过相同选项传入
TLS 凭证。

```go
locator := actor.NewLocator(redisClient)
defer locator.Close()

client, err := actor.NewClient(discover, locator, map[string]string{
"room":   "game",
"hall":   "game",
"player": "player",
}, actor.WithClientDialOptions(
grpc.WithTransportCredentials(insecure.NewCredentials()),
))
if err != nil {
return err
}
defer client.Close()

// 创建 Ref 没有网络请求，也不会激活，可以长期保存或随用随建。
ref := client.Ref(actor.Key{
Type: "player",
Id:   playerId,
})

body, err := ref.Ask(ctx, actor.Message{
Method: "get-profile",
}, actor.ActivateIfMissing)
if err != nil {
return err
}
fmt.Printf("profile: %s\n", body)

if err := ref.Tell(ctx, actor.Message{
Method: "save",
}, actor.ExistingOnly); err != nil {
return err
}
```

每次调用都明确传入激活策略：

| 策略                | 没有运行实例时的行为                       |
|---------------------|--------------------------------------------|
| `ExistingOnly`      | 返回 `ErrNotFound`，不执行 Loader          |
| `ActivateIfMissing` | 按类型对应的服务选择节点并激活，再执行消息 |

激活不等于创建业务实体。未注册账号的玩家在 Loader 中仍会返回实体不存在，远程调用得到
`codes.NotFound`。同一个 Actor Type 在集群中须使用相同服务映射，Key 的 Type/Id 是该命名空间 内的全局身份，不因服务实例变化而改变。

## 定位与激活

1. Locator 优先读取 Ristretto 本地缓存，miss 时合并查询 Guard 使用的 Redis 归属记录。
2. 有归属时，通过现有 `rpc.WithInstance` 定向调用，并携带目标 `ActivationId`。
3. 无归属且允许激活时，向对应服务发送 Activate，由现有 RPC picker 按实例权重选点。
4. 目标节点通过 Guard 的 Lua 抢占归属，再执行 Loader、OnStart。其他节点抢占失败会返回实际 Owner，Client 转向该 Owner
   等待启动完成。初始化时写入的 Owner 不等于已经就绪。
5. 入队前发现目标激活已变化时，返回 NOT_HERE；Client 合并回源 Redis、更新缓存并重试， 最多尝试 3 次。相同节点重新激活也必须使用新的
   ActivationId。

Ref 只保存 Key 和 Client，路由存在 Locator 中，因此节点切换或实例卸载后仍可使用同一个 Ref。 缓存默认保存 5 分钟、容量
10000，不缓存不存在的 Key。`WithLocatorCacheTTL`、
`WithLocatorCacheSize`、`WithLocatorTimeout` 可以调整缓存与共享查询超时。 自定义 Redis 命名空间时，`WithGuardPrefix` 与
`WithLocatorPrefix` 必须一致。

同一个 Client 对同 Key 的并发激活会合并，取消一个等待者不会取消其他调用方共享的激活。
`WithActivationTimeout` 设置共享激活 RPC 的等待超时，默认 5 秒；Loader 和 OnStart 的生命周期 超时仍由 System 的
`WithLifecycleTimeout` 控制。

## 调用语义

- Ask 等待业务结果；排队时已取消的请求会跳过，执行中的回调应响应上下文取消。
- Tell 成功只确认入队，随后使用 Actor 的生命周期执行。业务错误写日志，停止时排队消息可能 被丢弃；队列不提供持久化保证。
- Body 在服务端入队时复制，Ask 回包在交给等待者前复制，可以复用原缓冲区。
- 只有入队前明确返回的 NOT_HERE 允许自动重放。业务返回 NotFound、Unavailable 等错误也不 等于
  NOT_HERE；超时或通信失败可能发生在业务已经执行之后，不自动重放。连接异常时刷新 后续调用使用的路由，节点失联期间仍需等待权威租约过期。
- 远程业务错误通过 gRPC status 返回。ExistingOnly 找不到运行实例时，由 Ref 返回本地
  `ErrNotFound`；队列满对应 `codes.ResourceExhausted`。
- Actor 回调中同步调用其他 Ref 时须传递当前回调上下文。同步 Ask 自己会被拒绝，多 Actor 的同步调用环需要业务避免，可改用
  Tell。Actor 通过 `ctx.Self.Stop()` 请求停止自己。
- 先关闭 Client，再关闭 Locator；共享的 Discover 与 Redis 由上层关闭。

## 本地主循环任务

Actor 在生命周期和消息回调中通过 `ctx.Self` 获得当前激活实例的 Handle。它支持 Stop、 After、Every，以及不等待结果的
`RunOnMainLoop`。

外部协程需要投递闭包并等待完成时，可以使用当前实例的 Handle：

```go
err := handle.WaitResultOnMainLoop(ctx, func (execCtx context.Context) error {
return persist(execCtx)
})
```

它与 Ask 共用队列、取消和停止通知。需要读取闭包写入的结果时，必须等方法成功返回后再读取；
超时返回时，已开始的回调可能仍在执行。使用当前回调上下文同步等待自己的主循环会返回
`ErrSelfWait`。Handle 只操作当前激活实例，实例停止后不会自动转向新实例；业务跨节点调用使用 Ref。
