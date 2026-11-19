package gate

import (
	"context"
	"crypto/rand"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/2comjie/ngame/app"
	"github.com/2comjie/ngame/endpoint"
	gatepb "github.com/2comjie/ngame/gate/protocol"
	"github.com/2comjie/ngame/internal/host"
	"github.com/2comjie/ngame/network"
	"github.com/2comjie/ngame/packet"
	"github.com/2comjie/ngame/registry"
	"github.com/2comjie/ngame/rpc"
	"github.com/2comjie/ntool/logx"
	"google.golang.org/grpc"
)

type Config struct {
	// Instance.Id 是节点名前缀，实际实例 Id 每次构造时自动生成
	Instance       endpoint.ServiceInst
	Registry       registry.Registry
	Binder         *Binder
	RPCOptions     []grpc.ServerOption
	RPCListener    net.Listener
	NetworkOptions []network.Option
	Forward        ForwardFunc
	ErrorHandler   ErrorHandler
	// StartupTimeout 限制注册操作及启动失败后的清理，默认各 5 秒
	StartupTimeout time.Duration
}

// App 是网关组件，由外层 app.App 管理生命周期
// Context 取消表示已请求退出或服务失效，调用方应随后执行 Shutdown
type App struct {
	gatepb.UnimplementedGateServer

	host         *host.Host
	server       *network.Server
	binder       *Binder
	bindings     [256]sync.Mutex
	syncDone     chan struct{}
	forward      ForwardFunc
	errorHandler ErrorHandler
	started      bool
	closed       bool
}

var _ app.Component = (*App)(nil)
var _ gatepb.GateServer = (*App)(nil)

func New(config Config) *App {
	config.Instance.Validate()
	if config.Binder == nil {
		panic("gate: 必须提供 Binder")
	}
	if config.Forward == nil || config.ErrorHandler == nil {
		panic("gate: 必须提供 Forward 和 ErrorHandler")
	}
	config.Instance.Id += "-" + rand.Text()
	g := &App{
		host: host.New(host.Config{
			Instance:       config.Instance,
			Registry:       config.Registry,
			RPCServer:      rpc.NewServer(config.Instance.Id, config.RPCOptions...),
			RPCListener:    config.RPCListener,
			StartupTimeout: config.StartupTimeout,
		}),
		binder:       config.Binder,
		forward:      config.Forward,
		errorHandler: config.ErrorHandler,
	}
	opts := []network.Option{
		network.OnSessionEnd(g.onSessionEnd),
	}
	opts = append(opts, config.NetworkOptions...)
	opts = append(opts, network.BeforeSessionBind(g.onSessionBind), network.OnReq(g.onReq))
	g.server = network.NewServer(opts...)
	context.AfterFunc(g.Context(), g.server.RequestStop)
	gatepb.RegisterGateServer(g.RPCServer(), g)
	return g
}

func (g *App) Network() *network.Server    { return g.server }
func (g *App) Context() context.Context    { return g.host.Context() }
func (g *App) InstanceId() string          { return g.host.InstanceId() }
func (g *App) Registry() registry.Registry { return g.host.Registry() }

// RPCServer 供启动前注册业务 RPC
func (g *App) RPCServer() *grpc.Server { return g.host.RPCServer() }

// Instance 返回包含实际进程 Id 的独立快照
func (g *App) Instance() endpoint.ServiceInst { return g.host.Instance() }

func (g *App) Start() (err error) {
	if g.Context().Err() != nil {
		return app.ErrClosed
	}
	if g.started {
		return app.ErrStarted
	}
	g.started = true
	defer func() {
		if err != nil {
			ctx, cancel := context.WithTimeout(context.Background(), g.host.StartupTimeout())
			defer cancel()
			if rollbackErr := g.Shutdown(ctx); rollbackErr != nil {
				err = errors.Join(err, rollbackErr)
			}
		}
	}()
	if err = g.server.Start(); err != nil {
		return err
	}
	g.syncDone = make(chan struct{})
	go g.syncSessions()
	return g.host.Start()
}

func (g *App) RequestStop() {
	g.host.RequestStop()
	g.server.RequestStop()
}

func (g *App) Shutdown(ctx context.Context) error {
	if g.closed {
		return nil
	}
	g.RequestStop()
	unregisterErr := g.host.Unregister(ctx)
	g.host.RequestRPCStop()
	sessionErr := g.server.Shutdown(ctx)
	if sessionErr == nil && g.syncDone != nil {
		select {
		case <-g.syncDone:
		case <-ctx.Done():
			sessionErr = ctx.Err()
		}
	}
	rpcErr := g.host.ShutdownRPC(ctx)
	if sessionErr != nil || rpcErr != nil {
		return errors.Join(unregisterErr, sessionErr, rpcErr)
	}
	g.closed = true
	return unregisterErr
}

func (g *App) onReq(c *network.ReqContext) {
	if g.Context().Err() != nil {
		_ = c.Session.Conn.Close()
		return
	}
	response, err := g.forward(c.Context, &Request{
		Uid:       c.Session.Uid(),
		GateId:    g.InstanceId(),
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
