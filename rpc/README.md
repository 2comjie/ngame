# Gate 与 Node RPC

协议及生成的 gRPC Client、Server 接口位于：

- `github.com/2comjie/ngame/gate/protocol`，建议导入别名 `gatepb`。
- `github.com/2comjie/ngame/node/protocol`，建议导入别名 `nodepb`。

这两个包提供消息和 RPC 定义。业务层实现对应 Server，组合现有 `network.Server`、注册发现和路由逻辑。

## Node

Node 只有 `Call`，请求包含 `Uid`、来源 `GateId`、`SessionId`、`Route`、`Values` 和 `Body`。Uid 从网关已认证 Session 获取。Values
对应 `packet.Header.Values`，Body 是业务明文。

响应包含 `Replied`、`Values` 和 `Body`。`Replied=false` 表示业务处理完成，无需客户端回包；`Replied=true` 时，网关使用原
`network.ReqContext.Write` 返回响应，由原请求关联 Route、Seq 和连接。业务失败通过 gRPC error 返回。

上层决定服务名、目标节点、业务分发和并发策略；不需要额外的 Tell RPC。请求超时不能据此自动重放业务操作。

## Gate

| 方法         | 作用                                            | 返回                                |
|--------------|-------------------------------------------------|-------------------------------------|
| Push         | 按 Uid、SessionId 向确切会话推送                | PushCode                            |
| MultiPush    | 向本节点多个确切会话推送同一消息                | 每个去重目标的 PushResult           |
| Broadcast    | 向本节点取得的在线会话快照广播                  | TargetCount、SentCount、FailedCount |
| Kick         | 按 Uid、SessionId 关闭确切会话                  | KickCode                            |
| MockCall     | 校验会话后进入上层业务处理，结果返回 RPC 调用方 | MockCallCode、Replied、Values、Body |
| SessionCount | 查询本节点已绑定 Uid 的会话数量                 | Count，不计未认证连接               |

定向操作的 SessionId 为必需字段，不能用 0 表示任意会话。目标 GateId 通过 `rpc.WithInstance` 选择，不放入请求体。发送时必须取得并操作匹配的
Session，不能先检查 SessionId，再重新按 Uid 查找发送。

`PUSH_SENT` 只表示成功写入连接，不代表客户端已收到或处理。`PUSH_SESSION_NOT_HERE` 和 `MOCK_CALL_SESSION_NOT_HERE`
表示在执行前确认会话不存在或不匹配，上层可以重新定位。`PUSH_DELIVERY_UNKNOWN`、普通 gRPC 错误及超时不允许据此自动重放。

批量推送仅重新定位 SessionNotHere 的目标，不能重发整个批次。广播的 FailedCount 包括无法确认写入成功的会话，不自动重放整个广播。Kick
的 SessionNotHere 表示指定旧会话已不存在，不追踢玩家的新会话。

这些 RPC 都只操作本节点。归属查询、本地缓存、跨网关分组、全服广播和重试由上层组合实现。

## 注册与调用

使用现有 `rpc.NewServer` 创建服务器，再注册业务实现。业务实现需嵌入生成的 `UnimplementedNodeServer` 或
`UnimplementedGateServer`。

```go
grpcServer := rpc.NewServer(instanceId, serverOptions...)
nodepb.RegisterNodeServer(grpcServer, nodeImplementation)
// Gate 进程注册：gatepb.RegisterGateServer(grpcServer, gateImplementation)
```

客户端每个服务初始化一次，复用连接及生成的 Client。服务名与 Registry 注册时的 Name 一致，不强制使用 "node" 或 "gate"。

```go
conn, err := rpc.NewClient(discover, "player", dialOptions...)
if err != nil {
    return err
}
defer conn.Close()
client := nodepb.NewNodeClient(conn)

// 无状态服务可以直接传 ctx，有状态服务由上层确定目标实例。
response, err := client.Call(rpc.WithInstance(ctx, nodeId), request)
```

## 生成代码

需要 `protoc`、`protoc-gen-go` 和 `protoc-gen-go-grpc`，在项目根目录执行：

```sh
go generate ./gate/protocol ./node/protocol
```

生成文件随源码提交，普通编译不需要安装这些生成工具。
