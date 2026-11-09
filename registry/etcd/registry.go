package etcdRegistry

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ngame/registry"
	"github.com/2comjie/ntool/logx"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type options struct {
	prefix string
	ttl    time.Duration
}

type Option func(*options)

func WithPrefix(prefix string) Option {
	return func(o *options) { o.prefix = prefix }
}

// WithTTL 设置租约时长，必须为正整数秒，默认 10 秒。
func WithTTL(ttl time.Duration) Option {
	return func(o *options) { o.ttl = ttl }
}

type Registry struct {
	*registry.Discovery
	client *clientv3.Client
	prefix string
	ttl    int64
}

var _ registry.Registry = (*Registry)(nil)
var _ registry.Discover = (*Registry)(nil)

// New 同时提供注册和发现；client 的创建、认证、TLS 和关闭由调用方管理。
// 自动启动共享发现缓存，调用方退出时须 Close；Close 不影响注册句柄。
func New(client *clientv3.Client, opts ...Option) *Registry {
	o := options{prefix: "/ngame/services", ttl: 10 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	if client == nil || strings.Trim(o.prefix, "/") == "" || o.ttl <= 0 || o.ttl%time.Second != 0 {
		panic("registry/etcd: 必须提供 client、非空前缀和正整数秒 TTL")
	}
	r := &Registry{client: client, prefix: strings.TrimRight(o.prefix, "/") + "/", ttl: int64(o.ttl / time.Second)}
	r.Discovery = registry.NewDiscover((&discoverySource{Registry: r}).Watch)
	return r
}

func (r *Registry) Register(ctx context.Context, instance endpoint.ServiceInst) (registry.Registration, error) {
	instance.Validate()
	instance.Meta = maps.Clone(instance.Meta)
	data, err := json.Marshal(instance)
	if err != nil {
		return nil, err
	}
	lease, err := r.client.Grant(ctx, r.ttl)
	if err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(r.client.Ctx())
	registered := false
	defer func() {
		if registered {
			return
		}
		cancel()
		cleanup, stop := context.WithTimeout(r.client.Ctx(), 3*time.Second)
		defer stop()
		if _, err := r.client.Revoke(cleanup, lease.ID); err != nil && !errors.Is(err, rpctypes.ErrLeaseNotFound) {
			logx.Warnf("registry/etcd: 清理未完成的注册租约失败 error=%v", err)
		}
	}()
	key := r.prefix + url.PathEscape(instance.Id)
	response, err := r.client.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", 0)).
		Then(clientv3.OpPut(key, string(data), clientv3.WithLease(lease.ID))).Commit()
	if err != nil {
		return nil, err
	}
	if !response.Succeeded {
		return nil, registry.ErrAlreadyRegistered
	}
	keepAlive, err := r.client.KeepAlive(lifetime, lease.ID)
	if err != nil {
		return nil, err
	}
	registration := &registration{client: r.client, key: key, lease: lease.ID, instance: instance, ctx: lifetime, cancel: cancel}
	registered = true
	go func() {
		defer cancel()
		for {
			select {
			case <-lifetime.Done():
				return
			case response, ok := <-keepAlive:
				if !ok || response == nil || response.TTL <= 0 {
					if lifetime.Err() == nil {
						logx.Warnf("registry/etcd: 注册租约失效 id=%s", instance.Id)
					}
					return
				}
			}
		}
	}()
	return registration, nil
}

type registration struct {
	client       *clientv3.Client
	key          string
	lease        clientv3.LeaseID
	ctx          context.Context
	cancel       context.CancelFunc
	mutex        sync.Mutex
	instance     endpoint.ServiceInst
	unregistered bool
}

func (r *registration) Done() <-chan struct{} { return r.ctx.Done() }

func (r *registration) Update(ctx context.Context, instance endpoint.ServiceInst) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if instance.Id != r.instance.Id || instance.Name != r.instance.Name {
		return registry.ErrIdentityChanged
	}
	instance.Validate()
	return r.update(ctx, instance)
}

func (r *registration) UpdateMeta(ctx context.Context, meta map[string]string) error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	instance := r.instance
	instance.Meta = meta
	return r.update(ctx, instance)
}

func (r *registration) update(ctx context.Context, instance endpoint.ServiceInst) error {
	if r.ctx.Err() != nil {
		return registry.ErrRegistrationClosed
	}
	instance.Meta = maps.Clone(instance.Meta)
	data, err := json.Marshal(instance)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(r.ctx, cancel)
	defer stop()
	response, err := r.client.Txn(ctx).
		If(clientv3.Compare(clientv3.LeaseValue(r.key), "=", int64(r.lease))).
		Then(clientv3.OpPut(r.key, string(data), clientv3.WithLease(r.lease))).Commit()
	if err != nil {
		if errors.Is(err, rpctypes.ErrLeaseNotFound) {
			r.cancel()
		}
		if r.ctx.Err() != nil {
			return registry.ErrRegistrationClosed
		}
		return err
	}
	if !response.Succeeded {
		r.cancel()
		return registry.ErrRegistrationClosed
	}
	r.instance = instance
	return nil
}

func (r *registration) Unregister(ctx context.Context) error {
	r.cancel()
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.unregistered {
		return nil
	}
	// 每次注册拥有独立租约，撤销旧租约不会删除后来注册的记录。
	if _, err := r.client.Revoke(ctx, r.lease); err != nil && !errors.Is(err, rpctypes.ErrLeaseNotFound) {
		return err
	}
	r.unregistered = true
	return nil
}
