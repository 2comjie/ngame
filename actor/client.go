package actor

import (
	"context"
	"errors"
	"fmt"
	"time"

	actorpb "github.com/2comjie/ngame/actor/internal/protocol"
	"github.com/2comjie/ngame/registry"
	"github.com/2comjie/ngame/rpc"
	"github.com/2comjie/ntool/logx"
	"golang.org/x/sync/singleflight"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrRouteChanged = errors.New("actor: 路由连续变化，重试次数已用尽")

const maxRouteAttempts = 3

type clientOptions struct {
	activationTimeout time.Duration
	dialOptions       []grpc.DialOption
}

type ClientOption func(*clientOptions)

func WithActivationTimeout(timeout time.Duration) ClientOption {
	return func(o *clientOptions) { o.activationTimeout = timeout }
}

func WithClientDialOptions(opts ...grpc.DialOption) ClientOption {
	return func(o *clientOptions) { o.dialOptions = append(o.dialOptions, opts...) }
}

// Client 按 Actor 类型绑定 RPC Client，按服务复用连接
// services 为 Actor Type -> Service Name，候选节点须注册对应 Loader
type Client struct {
	locator           *Locator
	clients           map[string]actorpb.ActorClient // Actor Type -> RPC Client
	connections       map[string]*grpc.ClientConn    // Service Name -> 共享连接
	activationTimeout time.Duration
	activations       singleflight.Group
	ctx               context.Context
	cancel            context.CancelFunc
}

func NewClient(discover registry.Discover, locator *Locator, services map[string]string, opts ...ClientOption) (*Client, error) {
	if discover == nil || locator == nil || len(services) == 0 {
		panic("actor: Client 必须提供 Discover、Locator 和 Actor 类型到服务名的映射")
	}
	o := clientOptions{activationTimeout: 5 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	if o.activationTimeout <= 0 {
		panic("actor: 激活超时必须大于 0")
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		locator:           locator,
		clients:           make(map[string]actorpb.ActorClient, len(services)),
		connections:       make(map[string]*grpc.ClientConn),
		activationTimeout: o.activationTimeout,
		ctx:               ctx,
		cancel:            cancel,
	}
	for actorType, service := range services {
		if actorType == "" || service == "" {
			panic("actor: Actor 类型和服务名不能为空")
		}
		connection := c.connections[service]
		if connection == nil {
			var err error
			connection, err = rpc.NewClient(discover, service, o.dialOptions...)
			if err != nil {
				return nil, errors.Join(err, c.Close())
			}
			c.connections[service] = connection
		}
		c.clients[actorType] = actorpb.NewActorClient(connection)
	}
	return c, nil
}

// Ref 只创建逻辑引用，不查询路由或激活
func (c *Client) Ref(key Key) *Ref {
	return &Ref{client: c, key: key}
}

// Close 取消激活并关闭自建连接，不关闭共享依赖
func (c *Client) Close() error {
	c.cancel()
	var err error
	for _, connection := range c.connections {
		err = errors.Join(err, connection.Close())
	}
	return err
}

func (c *Client) activate(ctx context.Context, key Key, previous Owner) (Owner, error) {
	result := c.activations.DoChan(routeKey(key), func() (any, error) {
		callCtx, cancel := context.WithTimeout(c.ctx, c.activationTimeout)
		defer cancel()
		owner := previous
		request := &actorpb.ActivateRequest{Type: key.Type, Id: key.Id}
		for range maxRouteAttempts {
			targetCtx := callCtx
			if owner.NodeId != "" {
				targetCtx = rpc.WithInstance(callCtx, owner.NodeId)
			}
			response, err := c.clients[key.Type].Activate(targetCtx, request)
			if err != nil {
				return nil, err
			}
			owner = Owner{NodeId: response.NodeId, ActivationId: response.ActivationId}
			if owner.NodeId == "" || owner.ActivationId == "" {
				return nil, errors.New("actor: Activate 返回无效归属")
			}
			switch response.Code {
			case actorpb.ActivationCode_ACTIVATION_READY:
				c.locator.remember(key, owner)
				return owner, nil
			case actorpb.ActivationCode_ACTIVATION_OWNED:
				// 到 Owner 节点确认启动完成
				continue
			default:
				return nil, fmt.Errorf("actor: 不支持的 ActivationCode %d", response.Code)
			}
		}
		return nil, ErrRouteChanged
	})
	select {
	case value := <-result:
		if value.Err != nil {
			return Owner{}, value.Err
		}
		return value.Val.(Owner), nil
	case <-ctx.Done():
		return Owner{}, ctx.Err()
	case <-c.ctx.Done():
		return Owner{}, ErrStopped
	}
}

type ActivationPolicy uint8

const (
	// ExistingOnly 只调用正在运行的实例，不激活
	ExistingOnly ActivationPolicy = 0
	// ActivateIfMissing 无运行实例时按服务选点并激活，业务实体仍由 Loader 判断是否存在
	ActivateIfMissing ActivationPolicy = 1
)

// Ref 可并发使用的逻辑引用，不绑定激活实例
type Ref struct {
	client *Client
	key    Key
}

func (r *Ref) Key() Key { return r.key }

// Ask 仅在入队前的 NOT_HERE 时自动重试
// 回调中传当前上下文，禁止同步自调用或循环调用
func (r *Ref) Ask(ctx context.Context, message Message, policy ActivationPolicy) ([]byte, error) {
	return r.call(ctx, message, policy, true)
}

// Tell 只确认入队，错误写日志，停止时可能丢弃排队消息
func (r *Ref) Tell(ctx context.Context, message Message, policy ActivationPolicy) error {
	_, err := r.call(ctx, message, policy, false)
	return err
}

func (r *Ref) call(ctx context.Context, message Message, policy ActivationPolicy, ask bool) ([]byte, error) {
	c := r.client
	if r.key.Type == "" || r.key.Id == "" {
		return nil, ErrInvalidKey
	}
	if policy != ExistingOnly && policy != ActivateIfMissing {
		return nil, status.Error(codes.InvalidArgument, "actor: 无效激活策略")
	}
	client, exists := c.clients[r.key.Type]
	if !exists {
		return nil, ErrTypeNotRegistered
	}
	if current, ok := ctx.Value(executionContextKey{}).(*runner); ok && current.key == r.key {
		if ask {
			return nil, ErrSelfAsk
		}
		current.mu.Lock()
		state := current.state
		current.mu.Unlock()
		if state >= stateStopping {
			return nil, ErrStopping
		}
		if state != stateRunning {
			// OnStart 不能等待自身激活
			return nil, ErrNotFound
		}
	}
	owner, found, err := c.locator.Locate(ctx, r.key)
	if err != nil {
		return nil, err
	}
	call := client.Tell
	if ask {
		call = client.Ask
	}
	for range maxRouteAttempts {
		if !found {
			if policy == ExistingOnly {
				return nil, ErrNotFound
			}
			owner, err = c.activate(ctx, r.key, owner)
			if err != nil {
				return nil, err
			}
			found = true
		}
		request := &actorpb.CallRequest{
			Type:         r.key.Type,
			Id:           r.key.Id,
			ActivationId: owner.ActivationId,
			Method:       message.Method,
			Body:         message.Body,
		}
		response, err := call(rpc.WithInstance(ctx, owner.NodeId), request)
		if err != nil {
			// 连接异常只刷新路由，不重放本次请求
			if status.Code(err) == codes.Unavailable {
				if _, _, refreshErr := c.locator.Refresh(ctx, r.key, owner); refreshErr != nil {
					logx.Warnf("actor: 刷新路由失败 type=%s id=%s error=%v", r.key.Type, r.key.Id, refreshErr)
				}
			}
			return nil, err
		}
		if response.Code == actorpb.CallCode_CALL_OK {
			return response.Body, nil
		}
		if response.Code != actorpb.CallCode_CALL_NOT_HERE {
			return nil, fmt.Errorf("actor: 不支持的 CallCode %d", response.Code)
		}
		previous := owner
		owner, found, err = c.locator.Refresh(ctx, r.key, previous)
		if err != nil {
			return nil, err
		}
		if found && owner == previous {
			if policy == ExistingOnly {
				return nil, ErrNotFound
			}
			// 归属未变，下一轮定向激活
			found = false
		}
	}
	return nil, ErrRouteChanged
}
