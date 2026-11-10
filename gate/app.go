package gate

import (
	"context"
	"errors"
	"maps"
	"net"
	"sync/atomic"
	"time"

	"github.com/2comjie/ngame/app"
	"github.com/2comjie/ngame/endpoint"
	gatepb "github.com/2comjie/ngame/gate/protocol"
	"github.com/2comjie/ngame/network"
	"github.com/2comjie/ngame/packet"
	"github.com/2comjie/ngame/registry"
	"github.com/2comjie/ntool/logx"
	"google.golang.org/grpc"
)

type Config struct {
	Instance       endpoint.ServiceInst
	Registry       registry.Registry
	Binder         *Binder
	RPCServer      *grpc.Server
	RPCListener    net.Listener
	NetworkOptions []network.Option
	Forward        ForwardFunc
	ErrorHandler   ErrorHandler
	Components     []app.Component
	// StartupTimeout 限制注册操作及启动失败后的清理，默认各 5 秒。
	StartupTimeout time.Duration
}

// App 的生命周期方法由应用入口串行调用。
// Context 取消表示已请求退出或服务失效，调用方应随后执行 Shutdown。
type App struct {
	gatepb.UnimplementedGateServer
	*app.App

	instance        endpoint.ServiceInst
	server          *network.Server
	registry        registry.Registry
	registration    registry.Registration
	binder          *Binder
	syncStarted     bool
	syncDone        chan struct{}
	rpcServer       *grpc.Server
	rpcListener     net.Listener
	forward         ForwardFunc
	errorHandler    ErrorHandler
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
var _ gatepb.GateServer = (*App)(nil)

func New(config Config) *App {
	config.Instance.Validate()
	if config.Registry == nil || config.Binder == nil || config.RPCServer == nil || config.RPCListener == nil {
		panic("gate: 必须提供 Registry、Binder、RPCServer 和 RPCListener")
	}
	if config.Forward == nil || config.ErrorHandler == nil {
		panic("gate: 必须提供 Forward 和 ErrorHandler")
	}
	if config.StartupTimeout == 0 {
		config.StartupTimeout = 5 * time.Second
	}
	if config.StartupTimeout < 0 {
		panic("gate: StartupTimeout 必须大于0")
	}
	config.Instance.Meta = maps.Clone(config.Instance.Meta)
	ctx, cancel := context.WithCancel(context.Background())
	g := &App{
		App:            app.New(config.Components...),
		instance:       config.Instance,
		registry:       config.Registry,
		binder:         config.Binder,
		syncDone:       make(chan struct{}),
		rpcServer:      config.RPCServer,
		rpcListener:    config.RPCListener,
		forward:        config.Forward,
		errorHandler:   config.ErrorHandler,
		startupTimeout: config.StartupTimeout,
		ctx:            ctx,
		cancel:         cancel,
		rpcDone:        make(chan struct{}),
	}
	opts := []network.Option{
		network.OnSessionEnd(g.onSessionEnd),
	}
	opts = append(opts, config.NetworkOptions...)
	opts = append(opts, network.BeforeSessionBind(g.onSessionBind), network.OnReq(g.onReq))
	g.server = network.NewServer(opts...)
	gatepb.RegisterGateServer(g.rpcServer, g)
	return g
}

func (g *App) Network() *network.Server { return g.server }
func (g *App) Context() context.Context { return g.ctx }

func (g *App) Start() (err error) {
	if g.stopping.Load() || g.closed {
		return app.ErrClosed
	}
	if g.started {
		return app.ErrStarted
	}
	g.started = true
	defer func() {
		if err != nil {
			ctx, cancel := context.WithTimeout(context.Background(), g.startupTimeout)
			defer cancel()
			if cleanupErr := g.Shutdown(ctx); cleanupErr != nil {
				logx.Errorf("gate: 启动失败后清理失败 error=%v", cleanupErr)
			}
		}
	}()
	if err = g.App.Start(); err != nil {
		return err
	}
	g.syncStarted = true
	go g.syncSessions()
	g.rpcStarted = true
	go func() {
		defer close(g.rpcDone)
		if serveErr := g.rpcServer.Serve(g.rpcListener); serveErr != nil && !g.stopping.Load() {
			logx.Errorf("gate: gRPC服务退出 error=%v", serveErr)
			g.cancel()
			g.server.RequestStop()
		}
	}()
	if err = g.server.Start(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(g.ctx, g.startupTimeout)
	defer cancel()
	if g.registration, err = g.registry.Register(ctx, g.instance); err != nil {
		return err
	}
	if err = g.ctx.Err(); err != nil {
		return err
	}
	registration := g.registration
	go func() {
		select {
		case <-g.ctx.Done():
		case <-registration.Done():
			if g.ctx.Err() == nil {
				logx.Warnf("gate: 注册已失效 id=%s", g.instance.Id)
				g.cancel()
				g.server.RequestStop()
			}
		}
	}()
	return nil
}

func (g *App) RequestStop() {
	if !g.stopping.CompareAndSwap(false, true) {
		return
	}
	g.cancel()
	g.server.RequestStop()
	g.App.RequestStop()
}

func (g *App) Shutdown(ctx context.Context) error {
	if g.closed {
		return nil
	}
	g.RequestStop()
	var shutdownErr error
	if g.registration != nil {
		shutdownErr = g.registration.Unregister(ctx)
	}
	if err := g.server.Shutdown(ctx); err != nil {
		g.forceStopRPC()
		return errors.Join(shutdownErr, err)
	}
	if g.syncStarted {
		select {
		case <-g.syncDone:
		case <-ctx.Done():
			g.forceStopRPC()
			return errors.Join(shutdownErr, ctx.Err())
		}
	}
	if g.rpcStopDone == nil {
		g.rpcStopDone = make(chan struct{})
		go func() {
			defer close(g.rpcStopDone)
			g.rpcServer.GracefulStop()
		}()
	}
	select {
	case <-g.rpcStopDone:
	case <-ctx.Done():
		g.forceStopRPC()
		return errors.Join(shutdownErr, ctx.Err())
	}
	_ = g.rpcListener.Close()
	if g.rpcStarted {
		select {
		case <-g.rpcDone:
		case <-ctx.Done():
			return errors.Join(shutdownErr, ctx.Err())
		}
	}
	shutdownErr = errors.Join(shutdownErr, g.App.Shutdown(ctx))
	g.closed = true
	return shutdownErr
}

func (g *App) forceStopRPC() {
	if !g.rpcForceStopped {
		g.rpcForceStopped = true
		// GracefulStop 或 WaitForHandlers 可能仍在等待回调，不阻塞退出期限。
		go g.rpcServer.Stop()
	}
	_ = g.rpcListener.Close()
}

func (g *App) onReq(c *network.ReqContext) {
	if g.ctx.Err() != nil {
		_ = c.Session.Conn.Close()
		return
	}
	response, err := g.forward(c.Context, &Request{
		Uid:       c.Session.Uid(),
		GateId:    g.instance.Id,
		SessionId: c.Session.Id,
		NeedReply: c.NeedReply(),
		Route:     c.Request.Header.Route,
		Values:    c.Request.Header.Values,
		Body:      c.Request.Body,
	})
	if err != nil {
		g.errorHandler(c, err)
		return
	}
	if response.Replied && c.NeedReply() {
		if err := c.Write(&packet.Message{
			Header: packet.Header{
				Values: response.Values,
			},
			Body: response.Body,
		}); err != nil {
			logx.Warnf("gate: 客户端响应失败 uid=%d error=%v", c.Session.Uid(), err)
			_ = c.Session.Conn.Close()
		}
	}
}
