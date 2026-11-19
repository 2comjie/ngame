package node

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/2comjie/ngame/app"
	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ngame/internal/host"
	"github.com/2comjie/ngame/registry"
	"google.golang.org/grpc"
)

type Config struct {
	Instance    endpoint.ServiceInst
	Registry    registry.Registry
	RPCServer   *grpc.Server
	RPCListener net.Listener
	// StartupTimeout 限制注册操作及启动失败后的清理，默认各 5 秒
	StartupTimeout time.Duration
}

// App 是普通业务节点组件，由外层 app.App 管理生命周期
// Context 取消表示停止或服务失效，调用方应随后执行 Shutdown
type App struct {
	host    *host.Host
	started bool
	closed  bool
}

var _ app.Component = (*App)(nil)

func New(config Config) *App {
	h := host.New(host.Config{
		Instance:       config.Instance,
		Registry:       config.Registry,
		RPCServer:      config.RPCServer,
		RPCListener:    config.RPCListener,
		StartupTimeout: config.StartupTimeout,
	})
	return &App{host: h}
}

func (n *App) Context() context.Context    { return n.host.Context() }
func (n *App) InstanceId() string          { return n.host.InstanceId() }
func (n *App) Registry() registry.Registry { return n.host.Registry() }

// RPCServer 供启动前注册业务 RPC
func (n *App) RPCServer() *grpc.Server { return n.host.RPCServer() }

// Instance 返回独立快照，元信息通过 ReportMeta 更新
func (n *App) Instance() endpoint.ServiceInst { return n.host.Instance() }

// ReportMeta 合并元信息，注册前暂存，注册后更新当前注册
func (n *App) ReportMeta(ctx context.Context, values map[string]string) error {
	return n.host.ReportMeta(ctx, values)
}

func (n *App) Start() error {
	if n.Context().Err() != nil {
		return app.ErrClosed
	}
	if n.started {
		return app.ErrStarted
	}
	n.started = true
	if err := n.host.Start(); err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), n.host.StartupTimeout())
		defer cancel()
		if rollbackErr := n.Shutdown(ctx); rollbackErr != nil {
			return errors.Join(err, rollbackErr)
		}
		return err
	}
	return nil
}

func (n *App) RequestStop() {
	n.host.RequestStop()
}

// Shutdown 摘除实例并等待在途 RPC 退出
func (n *App) Shutdown(ctx context.Context) error {
	if n.closed {
		return nil
	}
	n.RequestStop()
	unregisterErr := n.host.Unregister(ctx)
	if err := n.host.ShutdownRPC(ctx); err != nil {
		return errors.Join(unregisterErr, err)
	}
	n.closed = true
	return unregisterErr
}
