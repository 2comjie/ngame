// Package redisRegistry 使用 Redis 7.4+ 的 Hash 字段过期管理服务注册。
package redisRegistry

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/json"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ngame/registry"
	"github.com/2comjie/ntool/logx"
	"github.com/redis/go-redis/v9"
)

//go:embed register.lua
var registerScript string

//go:embed update.lua
var updateScript string

//go:embed renew.lua
var renewScript string

//go:embed unregister.lua
var unregisterScript string

type options struct {
	prefix       string
	ttl          time.Duration
	syncInterval time.Duration
}

type Option func(*options)

func WithPrefix(prefix string) Option {
	return func(o *options) { o.prefix = prefix }
}

// WithTTL 设置注册时长，必须为正整数秒，默认 10 秒，每 TTL/3 续期。
func WithTTL(ttl time.Duration) Option {
	return func(o *options) { o.ttl = ttl }
}

// WithSyncInterval 设置发现列表的定时同步间隔，默认 5 秒，用于发现过期和修正通知遗漏。
func WithSyncInterval(interval time.Duration) Option {
	return func(o *options) { o.syncInterval = interval }
}

type Registry struct {
	*registry.Discovery
	client       redis.UniversalClient
	keys         []string
	channel      string
	ttl          time.Duration
	syncInterval time.Duration
}

var _ registry.Registry = (*Registry)(nil)
var _ registry.Discover = (*Registry)(nil)

// New 同时提供注册和发现；client 的创建、认证、TLS 和关闭由调用方管理。
// 自动启动共享发现缓存，调用方退出时须 Close；Close 不影响注册句柄。
func New(client redis.UniversalClient, opts ...Option) *Registry {
	o := options{prefix: "ngame:registry", ttl: 10 * time.Second, syncInterval: 5 * time.Second}
	for _, opt := range opts {
		opt(&o)
	}
	if client == nil || o.prefix == "" || strings.ContainsAny(o.prefix, "{}") || o.ttl <= 0 || o.ttl%time.Second != 0 || o.syncInterval <= 0 {
		panic("registry/redis: 必须提供 client、非空且不含花括号的前缀、正整数秒 TTL 和正同步间隔")
	}
	// 同一注册空间的两个 Hash 使用相同 hash tag，支持 Redis Cluster 中的 Lua 原子操作。
	prefix := "{" + o.prefix + "}"
	r := &Registry{client: client, keys: []string{prefix + ":instances", prefix + ":owners"}, channel: prefix + ":changed", ttl: o.ttl, syncInterval: o.syncInterval}
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
	owner := rand.Text()
	result, err := r.client.Eval(ctx, registerScript, r.keys, instance.Id, owner, data, int64(r.ttl/time.Second), r.channel).Int()
	if err != nil {
		return nil, err
	}
	if result == 0 {
		return nil, registry.ErrAlreadyRegistered
	}
	lifetime, cancel := context.WithCancel(context.Background())
	registration := &registration{registry: r, instance: instance, owner: owner, ctx: lifetime, cancel: cancel}
	go func() {
		defer cancel()
		ticker := time.NewTicker(r.ttl / 3)
		defer ticker.Stop()
		for {
			select {
			case <-lifetime.Done():
				return
			case <-ticker.C:
				ctx, stop := context.WithTimeout(lifetime, r.ttl/3)
				result, err := r.client.Eval(ctx, renewScript, r.keys, instance.Id, owner, int64(r.ttl/time.Second)).Int()
				stop()
				if err != nil {
					if lifetime.Err() == nil {
						logx.Warnf("registry/redis: 注册续期失败 id=%s error=%v", instance.Id, err)
					}
					return
				}
				if result == 0 {
					logx.Warnf("registry/redis: 注册所有权已失效 id=%s", instance.Id)
					return
				}
			}
		}
	}()
	return registration, nil
}

type registration struct {
	registry     *Registry
	owner        string
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
	result, err := r.registry.client.Eval(ctx, updateScript, r.registry.keys, instance.Id, r.owner, data, int64(r.registry.ttl/time.Second), r.registry.channel).Int()
	if err != nil {
		if r.ctx.Err() != nil {
			return registry.ErrRegistrationClosed
		}
		return err
	}
	if result == 0 {
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
	if err := r.registry.client.Eval(ctx, unregisterScript, r.registry.keys, r.instance.Id, r.owner, r.registry.channel).Err(); err != nil {
		return err
	}
	r.unregistered = true
	return nil
}
