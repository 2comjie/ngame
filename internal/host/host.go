package host

import (
	"context"
	"maps"
	"net"
	"sync"
	"time"

	"github.com/2comjie/ngame/app"
	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ngame/registry"
	"github.com/2comjie/ntool/logx"
	"google.golang.org/grpc"
)

type Config struct {
	Instance       endpoint.ServiceInst
	Registry       registry.Registry
	RPCServer      *grpc.Server
	RPCListener    net.Listener
	StartupTimeout time.Duration
}

// Host 管理实例、注册和 RPC 服务，生命周期方法由调用方串行调用
type Host struct {
	instance       endpoint.ServiceInst
	registry       registry.Registry
	registration   registry.Registration
	metaMu         sync.Mutex
	rpcServer      *grpc.Server
	rpcListener    net.Listener
	startupTimeout time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	rpcDone        chan struct{}
	rpcStopDone    chan struct{}
	rpcForced      bool
}

func New(config Config) *Host {
	config.Instance.Validate()
	if config.Registry == nil || config.RPCServer == nil || config.RPCListener == nil {
		panic("host: 必须提供 Registry、RPCServer 和 RPCListener")
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = 5 * time.Second
	}
	if config.StartupTimeout < 0 {
		panic("host: StartupTimeout 必须大于0")
	}
	config.Instance.Meta = maps.Clone(config.Instance.Meta)
	ctx, cancel := context.WithCancel(context.Background())
	return &Host{
		instance:       config.Instance,
		registry:       config.Registry,
		rpcServer:      config.RPCServer,
		rpcListener:    config.RPCListener,
		startupTimeout: config.StartupTimeout,
		ctx:            ctx,
		cancel:         cancel,
	}
}

func (h *Host) Context() context.Context      { return h.ctx }
func (h *Host) InstanceId() string            { return h.instance.Id }
func (h *Host) Registry() registry.Registry   { return h.registry }
func (h *Host) RPCServer() *grpc.Server       { return h.rpcServer }
func (h *Host) StartupTimeout() time.Duration { return h.startupTimeout }

func (h *Host) Instance() endpoint.ServiceInst {
	h.metaMu.Lock()
	defer h.metaMu.Unlock()
	instance := h.instance
	instance.Meta = maps.Clone(instance.Meta)
	return instance
}

func (h *Host) ReportMeta(ctx context.Context, values map[string]string) error {
	h.metaMu.Lock()
	defer h.metaMu.Unlock()
	if h.ctx.Err() != nil {
		return app.ErrClosed
	}
	meta := make(map[string]string, len(h.instance.Meta)+len(values))
	maps.Copy(meta, h.instance.Meta)
	maps.Copy(meta, values)
	if h.registration != nil {
		if err := h.registration.UpdateMeta(ctx, meta); err != nil {
			return err
		}
	}
	h.instance.Meta = meta
	return nil
}

// Start 启动 RPC 和注册实例，失败时由调用方执行关停
func (h *Host) Start() error {
	h.rpcDone = make(chan struct{})
	go func() {
		defer close(h.rpcDone)
		if err := h.rpcServer.Serve(h.rpcListener); err != nil && h.ctx.Err() == nil {
			logx.Errorf("host: gRPC 服务退出 id=%s error=%v", h.instance.Id, err)
			h.cancel()
		}
	}()

	ctx, cancel := context.WithTimeout(h.ctx, h.startupTimeout)
	defer cancel()
	h.metaMu.Lock()
	registration, err := h.registry.Register(ctx, h.instance)
	h.registration = registration
	h.metaMu.Unlock()
	if err != nil {
		return err
	}
	if err := h.ctx.Err(); err != nil {
		return err
	}
	go func() {
		select {
		case <-h.ctx.Done():
		case <-registration.Done():
			if h.ctx.Err() == nil {
				logx.Warnf("host: 注册已失效 id=%s", h.instance.Id)
				h.cancel()
			}
		}
	}()
	return nil
}

func (h *Host) RequestStop() { h.cancel() }

func (h *Host) Unregister(ctx context.Context) error {
	if h.registration == nil {
		return nil
	}
	return h.registration.Unregister(ctx)
}

// ShutdownRPC 等待在途回调退出，超时后仍可继续等待
func (h *Host) ShutdownRPC(ctx context.Context) error {
	if h.rpcStopDone == nil {
		h.rpcStopDone = make(chan struct{})
		go func() {
			defer close(h.rpcStopDone)
			h.rpcServer.GracefulStop()
			_ = h.rpcListener.Close()
		}()
	}
	select {
	case <-h.rpcStopDone:
	case <-ctx.Done():
		if !h.rpcForced {
			h.rpcForced = true
			go h.rpcServer.Stop()
		}
		_ = h.rpcListener.Close()
		return ctx.Err()
	}
	if h.rpcDone != nil {
		select {
		case <-h.rpcDone:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
