# 服务注册与发现

Redis 和 etcd 后端都实现 `registry.Registry`、`registry.Discover`，注册后返回 `registry.Registration`。业务层选择一个后端，同一集群内使用相同的命名空间。

## 创建后端

调用方创建并持有底层客户端，负责认证、TLS、连接超时和关闭；后端不会关闭共享客户端。

```go
import (
    redisRegistry "github.com/2comjie/ngame/registry/redis"
    etcdRegistry "github.com/2comjie/ngame/registry/etcd"
)

// Redis：redisClient 为 redis.UniversalClient。
store := redisRegistry.New(redisClient,
    redisRegistry.WithPrefix("ngame:prod:zone-1"),
    redisRegistry.WithTTL(10*time.Second),
    redisRegistry.WithSyncInterval(5*time.Second),
)

// 或使用 etcd：etcdClient 为 *clientv3.Client。
store := etcdRegistry.New(etcdClient,
    etcdRegistry.WithPrefix("/ngame/prod/zone-1/services"),
    etcdRegistry.WithTTL(10*time.Second),
)
```

以上是两种可选的初始化方式。实例 Id 在命名空间内唯一，每次进程启动生成新 Id。缺少客户端、必要实例字段或配置无效时 panic；重复注册返回 `registry.ErrAlreadyRegistered`。

`New` 会启动共享发现缓存，同一进程内复用同一个 `store`。退出时调用 `store.Close()` 停止发现；它不会注销实例或关闭底层客户端。应先结束 RPC 等订阅方、注销注册句柄，再关闭 store 和底层客户端。

## 注册和更新

```go
instance := endpoint.ServiceInst{
    Id:      instanceId,
    Name:    "player", // 由业务定义服务名。
    RpcHost: "10.0.0.3",
    RpcPort: 9000,
    Status:  endpoint.Working,
    Weight:  1,
    Meta:    map[string]string{"zone": "1"},
}
registration, err := store.Register(ctx, instance)
if err != nil {
    return err
}
```

`Register` 的 ctx 只限制注册操作。成功后自动续期，直到主动注销或注册失效。业务层应监听 `registration.Done()`，在注册失效时停止接收业务或重新申请注册。

- `Update(ctx, instance)` 整体替换实例信息，Id 和 Name 不可更改。
- `UpdateMeta(ctx, meta)` 只替换 Meta，nil 或空 map 清空元信息。
- `Unregister(ctx)` 停止续期并注销，重复调用安全。即使注销失败或 ctx 已取消，也会关闭 Done 并停止续期，遗留记录随后过期；可以再次调用注销。
- 注册结束后更新返回 `registry.ErrRegistrationClosed`，不会重建记录。

每次注册使用独立的所有权标识，旧句柄无法更新或注销新注册。结束注册后，调用方再按自身资源所有权关闭 Redis 或 etcd 客户端。

## 查询和监听

- `Get(ctx, instanceId)` 按实例 Id 查询，不存在时返回 `found=false`。
- `List(ctx, serviceName)` 获取指定服务的实例，服务名为空时返回全部实例。
- `Watch(ctx, serviceName, onChange)` 阻塞监听；首次回调返回完整列表，之后返回变更后的完整快照。没有实例时仍回调空列表。

每次 Watch 独立订阅本地缓存，回调串行执行，应及时返回并响应 ctx 取消。回调参数与内部状态独立，可以保留或修改。快照不保证逐个交付每次中间状态；慢订阅者合并积压通知，读取最新完整列表，不阻塞后端同步或其他订阅者。取消时 Watch 返回 `ctx.Err()`，store 关闭时返回 `registry.ErrDiscoveryClosed`。

Get 和 List 只读本地缓存，所有查询和订阅共享一条命名空间同步链路，不会逐次访问 Redis 或 etcd。首次同步完成前等待调用方 ctx；同步失败时记录日志、保留最后成功的快照并重试，不让查询请求各自回源。单条 JSON 损坏时记录错误并跳过该条，不阻断其他实例的同步。

这些查询属于最终一致的服务发现，即使使用 etcd 后端也不提供线性一致的 Get/List。发现保留所有状态和权重，RPC 层消费本地 Watch 管理连接及选择目标。

## 后端行为

Redis 要求 **Redis 7.4+**，沿用 Hash 字段 TTL、通知和周期同步。实例信息与所有权存放在同一 hash slot 的两个 Hash，Lua 脚本通过 `go:embed` 加载，注册、更新、续期和注销原子执行。续期间隔为 TTL/3。

发现端独立订阅变更频道和 `hexpired` 字段过期事件，消息或订阅确认触发全量拉取，覆盖启动和重连间隙。过期事件需要 Redis 开启相应 keyspace notifications；不开启仍可通过默认 5 秒的周期同步发现自然过期、补偿漏通知。Redis Cluster 的过期事件仅来自订阅所在节点，周期同步同样负责补偿。两路订阅均不阻塞周期同步，通知不构成可回放的变更日志。

etcd 使用独立 Lease 保活，注册用事务防止覆盖，更新校验 Lease 所有权。共享同步源先读取实例表，再从读取 revision 的下一条开始 Watch；历史已被压缩时重新获取完整列表。SDK 负责可恢复的连接重试，同步源退出后公共缓存层重试。租约失效后关闭 Done，不自动重新注册。

两个后端的存储互相独立；接口相同不代表可以在同一集群内混用两套实例表。
