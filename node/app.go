package node

import (
	"context"
	"errors"
	"maps"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/2comjie/ngame/app"
	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ngame/registry"
	"github.com/2comjie/ntool/logx"
	"google.golang.org/grpc"
)

type Config struct {
	Instance    endpoint.ServiceInst
	Registry    registry.Registry
	RPCServer   *grpc.Server
	RPCListener net.Listener
	Components  []app.Component
	// StartupTimeout 限制注册操作及启动失败后的清理，默认各 5 秒。
	StartupTimeout time.Duration
}

// App 管理业务组件、RPC 服务与当前注册，生命周期方法由入口串行调用
// Context 取消表示停止或服务失效，调用方应随后执行 Shutdown。
type App struct {
	*app.App

	instance        endpoint.ServiceInst
	registry        registry.Registry
	registration    registry.Registration
	metaMu          sync.Mutex
	rpcServer       *grpc.Server
	rpcListener     net.Listener
	startupTimeout  time.Duration
	ctx             context.Context
	cancel          context.CancelFunc
	started         bool
	closed          bool
	stopping        atomic.Bool
	rpcStarted      bool
	rpcDone         chan struct{}
	rpcStopDone     chan struct{}
	rpcForceStopped bool
}

var _ app.Component = (*App)(nil)

func New(config Config) *App {
	config.Instance.Validate()
	if config.Registry == nil || config.RPCServer == nil || config.RPCListener == nil {
		panic("node: 必须提供 Registry、RPCServer 和 RPCListener")
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = 5 * time.Second
	}
	if config.StartupTimeout < 0 {
		panic("node: StartupTimeout 必须大于0")
	}
	config.Instance.Meta = maps.Clone(config.Instance.Meta)
	ctx, cancel := context.WithCancel(context.Background())
	n := &App{
		App:            app.New(config.Components...),
		instance:       config.Instance,
		registry:       config.Registry,
		rpcServer:      config.RPCServer,
		rpcListener:    config.RPCListener,
		startupTimeout: config.StartupTimeout,
		ctx:            ctx,
		cancel:         cancel,
		rpcDone:        make(chan struct{}),
	}
	return n
}

func (n *App) Context() context.Context    { return n.ctx }
func (n *App) InstanceId() string          { return n.instance.Id }
func (n *App) Registry() registry.Registry { return n.registry }

// RPCServer 供启动前注册业务 RPC
func (n *App) RPCServer() *grpc.Server { return n.rpcServer }

// Instance 返回独立快照，元信息通过 ReportMeta 更新
func (n *App) Instance() endpoint.ServiceInst {
	n.metaMu.Lock()
	defer n.metaMu.Unlock()
	instance := n.instance
	instance.Meta = maps.Clone(instance.Meta)
	return instance
}

// ReportMeta 合并元信息，注册前暂存，注册后串行更新当前注册
func (n *App) ReportMeta(ctx context.Context, values map[string]string) error {
	n.metaMu.Lock()
	defer n.metaMu.Unlock()
	if n.stopping.Load() {
		return app.ErrClosed
	}
	meta := make(map[string]string, len(n.instance.Meta)+len(values))
	maps.Copy(meta, n.instance.Meta)
	maps.Copy(meta, values)
	if n.registration != nil {
		if err := n.registration.UpdateMeta(ctx, meta); err != nil {
			return err
		}
	}
	n.instance.Meta = meta
	return nil
}

func (n *App) Start() (err error) {
	if n.stopping.Load() || n.closed {
		return app.ErrClosed
	}
	if n.started {
		return app.ErrStarted
	}
	n.started = true
	defer func() {
		if err != nil {
			ctx, cancel := context.WithTimeout(context.Background(), n.startupTimeout)
			defer cancel()
			if cleanupErr := n.Shutdown(ctx); cleanupErr != nil {
				logx.Errorf("node: 启动失败后清理失败 error=%v", cleanupErr)
			}
		}
	}()
	if err = n.App.Start(); err != nil {
		return err
	}
	n.rpcStarted = true
	go func() {
		defer close(n.rpcDone)
		if serveErr := n.rpcServer.Serve(n.rpcListener); serveErr != nil && !n.stopping.Load() {
			logx.Errorf("node: gRPC服务退出 error=%v", serveErr)
			n.cancel()
		}
	}()
	ctx, cancel := context.WithTimeout(n.ctx, n.startupTimeout)
	defer cancel()
	n.metaMu.Lock()
	n.registration, err = n.registry.Register(ctx, n.instance)
	n.metaMu.Unlock()
	if err != nil {
		return err
	}
	if err = n.ctx.Err(); err != nil {
		return err
	}
	registration := n.registration
	go func() {
		select {
		case <-n.ctx.Done():
		case <-registration.Done():
			if n.ctx.Err() == nil {
				logx.Warnf("node: 注册已失效 id=%s", n.instance.Id)
				n.cancel()
			}
		}
	}()
	return nil
}

func (n *App) RequestStop() {
	if !n.stopping.CompareAndSwap(false, true) {
		return
	}
	n.cancel()
	n.App.RequestStop()
}

// Shutdown 等待 gRPC 回调退出后再释放业务组件。
// 超时后保留仍被使用的组件，允许以新 context 再次等待完成清理。
func (n *App) Shutdown(ctx context.Context) error {
	if n.closed {
		return nil
	}
	n.RequestStop()
	var shutdownErr error
	if n.registration != nil {
		shutdownErr = n.registration.Unregister(ctx)
	}
	if n.rpcStopDone == nil {
		n.rpcStopDone = make(chan struct{})
		go func() {
			defer close(n.rpcStopDone)
			n.rpcServer.GracefulStop()
		}()
	}
	select {
	case <-n.rpcStopDone:
	case <-ctx.Done():
		n.forceStopRPC()
		return errors.Join(shutdownErr, ctx.Err())
	}
	_ = n.rpcListener.Close()
	if n.rpcStarted {
		select {
		case <-n.rpcDone:
		case <-ctx.Done():
			return errors.Join(shutdownErr, ctx.Err())
		}
	}
	shutdownErr = errors.Join(shutdownErr, n.App.Shutdown(ctx))
	n.closed = true
	return shutdownErr
}

func (n *App) forceStopRPC() {
	if !n.rpcForceStopped {
		n.rpcForceStopped = true
		// GracefulStop 或 WaitForHandlers 可能仍在等待回调，不阻塞退出期限。
		go n.rpcServer.Stop()
	}
	_ = n.rpcListener.Close()
}
