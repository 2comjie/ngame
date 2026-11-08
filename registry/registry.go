package registry

import (
	"context"

	"github.com/2comjie/ngame/endpoint"
)

type Registry interface {
	// Register 注册实例并启动自动续租，成功后返回当前注册的管理句柄。
	// ctx 只限制本次注册操作；成功后的续租由 Registration 管理。
	// 相同 Id 已被注册时返回错误，不覆盖已有注册。
	Register(ctx context.Context, instance endpoint.ServiceInst) (Registration, error)
}

// Registration 管理一次注册，更新和注销只能影响这次注册拥有的记录。
// 方法可并发调用；注册结束后，Update 和 UpdateMeta 不得重新创建记录。
type Registration interface {
	// Update 整体替换实例信息，包括 Status、Weight 和 Meta，Id 和 Name 不可更改。
	Update(ctx context.Context, instance endpoint.ServiceInst) error

	// UpdateMeta 整体替换元信息，nil 或空 map 表示清空，其余实例字段保持不变。
	UpdateMeta(ctx context.Context, meta map[string]string) error

	// Unregister 停止续租并注销，重复调用安全。
	// 即使 ctx 取消或注销失败，也必须停止本地续租并关闭 Done；遗留记录由租约过期清理。
	Unregister(ctx context.Context) error

	// Done 在主动注销或无法维持注册租约时关闭。
	// 关闭后不再自动恢复注册，由业务层决定停止服务或重新注册。
	Done() <-chan struct{}
}

// Discover 返回的实例及其 Meta 均与实现内部状态独立，调用方可以保留或修改。
type Discover interface {
	// Get 按实例 Id 查询，不存在时返回 found=false、err=nil。
	Get(ctx context.Context, instanceId string) (instance endpoint.ServiceInst, found bool, err error)

	// List 查询指定服务的全部实例，name 为空时查询注册命名空间内的全部实例。
	// 保留所有状态和权重，由调用方决定哪些实例可被选用。
	List(ctx context.Context, name string) ([]endpoint.ServiceInst, error)

	// Watch 阻塞监听指定服务，name 为空时监听注册命名空间内的全部实例。
	// 首次回调提供完整列表（可能为空），后续回调提供变更后的完整快照。
	// 首次读取与后续监听须衔接，避免漏掉期间的变更；监听历史不可恢复时重新获取完整列表。
	// 每次调用拥有独立订阅，回调在当前调用中串行执行，应及时返回并响应 ctx 取消。
	// ctx 取消时返回 ctx.Err()，其他无法恢复的监听错误直接返回，不通过回调传递。
	Watch(ctx context.Context, name string, onChange func([]endpoint.ServiceInst)) error
}
