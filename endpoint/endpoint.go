package endpoint

import (
	"net"

	"github.com/spf13/cast"
)

type Status int32

const (
	Working  Status = 0
	Hung     Status = 1
	Shutdown Status = 2
)

type ServiceInst struct {
	Id      string            `json:"id"`
	Name    string            `json:"name"`
	RpcHost string            `json:"rpc_host"`
	RpcPort int               `json:"rpc_port"`
	Meta    map[string]string `json:"meta"`
	Weight  int               `json:"weight"` // 负载均衡权重
}

func (s ServiceInst) RpcTarget() string {
	return net.JoinHostPort(s.RpcHost, cast.ToString(s.RpcPort))
}
