package endpoint

import (
	"net"

	"github.com/spf13/cast"
)

// Status 表示实例声明的服务状态，存活由注册租约维护。
type Status int32

const (
	Working  Status = 0 // 正常服务，可参与负载均衡。
	Hung     Status = 1 // 暂停参与负载均衡，已有业务的定向调用由业务层决定。
	Shutdown Status = 2 // 正在退出，不再接收新请求。
)

// ServiceInst 描述一个进程提供的服务实例。
// Id 在注册命名空间内唯一，每次进程启动使用新 Id，避免旧进程影响重启后的实例。
type ServiceInst struct {
	Id      string            `json:"id"`       // 实例标识。
	Name    string            `json:"name"`     // 服务名，同类实例使用相同名称，由业务层定义。
	RpcHost string            `json:"rpc_host"` // 其他实例可访问的 RPC 主机地址，不使用通配监听地址。
	RpcPort int               `json:"rpc_port"` // 实际 RPC 监听端口，不使用动态分配前的 0。
	Status  Status            `json:"status"`   // 默认 Working；调用方据此筛选目标。
	Weight  int               `json:"weight"`   // 加权负载均衡权重，正数参与，0 不参与，不允许负数。
	Meta    map[string]string `json:"meta"`     // 业务元数据，例如区服、版本。
}

// Validate 检查必需的注册配置，配置错误时直接 panic。
func (s ServiceInst) Validate() {
	if s.Id == "" || s.Name == "" || s.RpcHost == "" || s.RpcPort <= 0 || s.RpcPort > 65535 {
		panic("endpoint: 必须提供实例 Id、服务名和有效 RPC 地址")
	}
	if ip := net.ParseIP(s.RpcHost); ip != nil && ip.IsUnspecified() {
		panic("endpoint: RPC 地址不能使用通配监听地址")
	}
	if s.Weight < 0 || (s.Status != Working && s.Status != Hung && s.Status != Shutdown) {
		panic("endpoint: 实例状态或权重无效")
	}
}

// RpcTarget 返回供 RPC 客户端连接的 host:port，支持 IPv6 地址。
func (s ServiceInst) RpcTarget() string {
	return net.JoinHostPort(s.RpcHost, cast.ToString(s.RpcPort))
}
