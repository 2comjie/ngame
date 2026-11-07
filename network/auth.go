package network

import "context"

// Auther 验证并原子消费一次性凭证，返回凭证所属的 Uid。
// 实现必须响应 ctx 取消；不能使用客户端另行声明的 Uid 作为身份。
type Auther interface {
	Auth(ctx context.Context, token []byte) (uid uint64, err error)
}
type AuthFunc func(ctx context.Context, token []byte) (uid uint64, err error)

func (f AuthFunc) Auth(ctx context.Context, token []byte) (uint64, error) {
	return f(ctx, token)
}
