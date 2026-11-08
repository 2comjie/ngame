package rpc

import "context"

type instanceContextKey struct{}

// WithInstance 指定本次 RPC 的目标实例；实例不存在或不可用时，不会改选其他实例。
// 调用方先通过业务 Locator 查询归属，再传入实例 Id。
func WithInstance(ctx context.Context, instanceId string) context.Context {
	if instanceId == "" {
		panic("rpc: 指定实例 Id 不能为空")
	}
	return context.WithValue(ctx, instanceContextKey{}, instanceId)
}
