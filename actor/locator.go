package actor

import (
	"context"
	"hash/maphash"
	"net/url"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

type locatorOptions struct {
	prefix    string
	cacheSize int
	cacheTTL  time.Duration
	timeout   time.Duration
}

type LocatorOption func(*locatorOptions)

func WithLocatorPrefix(prefix string) LocatorOption {
	return func(o *locatorOptions) { o.prefix = prefix }
}

func WithLocatorCacheSize(size int) LocatorOption {
	return func(o *locatorOptions) { o.cacheSize = size }
}

func WithLocatorCacheTTL(ttl time.Duration) LocatorOption {
	return func(o *locatorOptions) { o.cacheTTL = ttl }
}

func WithLocatorTimeout(timeout time.Duration) LocatorOption {
	return func(o *locatorOptions) { o.timeout = timeout }
}

// Locator 缓存 Guard 归属，不缓存不存在的 Key
// 缓存仅用于路由，目标仍须校验 ActivationId
type Locator struct {
	client  redis.UniversalClient
	options locatorOptions
	cache   *ristretto.Cache[string, Owner]
	seed    maphash.Seed
	locks   [256]sync.Mutex
	queries singleflight.Group
	ctx     context.Context
	cancel  context.CancelFunc
}

type ownerLookup struct {
	owner Owner
	found bool
}

func NewLocator(client redis.UniversalClient, opts ...LocatorOption) *Locator {
	o := locatorOptions{
		prefix:    "ngame:actor:",
		cacheSize: 10000,
		cacheTTL:  5 * time.Minute,
		timeout:   5 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if client == nil || o.prefix == "" || o.cacheSize < 1 || o.cacheTTL <= 0 || o.timeout <= 0 {
		panic("actor: Locator 必须提供 Redis、非空前缀以及正数缓存容量、TTL 和查询超时")
	}
	cache, err := ristretto.NewCache(&ristretto.Config[string, Owner]{
		NumCounters:        int64(o.cacheSize) * 10,
		MaxCost:            int64(o.cacheSize),
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Locator{
		client:  client,
		options: o,
		cache:   cache,
		seed:    maphash.MakeSeed(),
		ctx:     ctx,
		cancel:  cancel,
	}
}

func (l *Locator) Locate(ctx context.Context, key Key) (Owner, bool, error) {
	return l.lookup(ctx, key, nil)
}

// Refresh 合并旧归属的回源，已刷新时复用新缓存
func (l *Locator) Refresh(ctx context.Context, key Key, previous Owner) (Owner, bool, error) {
	return l.lookup(ctx, key, &previous)
}

func (l *Locator) lookup(ctx context.Context, key Key, previous *Owner) (Owner, bool, error) {
	if key.Type == "" || key.Id == "" {
		return Owner{}, false, ErrInvalidKey
	}
	suffix := routeKey(key)
	if owner, ok := l.cache.Get(suffix); ok && (previous == nil || owner != *previous) {
		return owner, true, nil
	}
	queryKey := suffix
	if previous != nil {
		queryKey += "/" + url.PathEscape(previous.NodeId) + "/" + url.PathEscape(previous.ActivationId)
	}
	result := l.queries.DoChan(queryKey, func() (any, error) {
		lock := &l.locks[maphash.String(l.seed, suffix)%uint64(len(l.locks))]
		lock.Lock()
		defer lock.Unlock()
		if owner, ok := l.cache.Get(suffix); ok && (previous == nil || owner != *previous) {
			return ownerLookup{owner: owner, found: true}, nil
		}
		// 等待者取消不影响共享查询
		queryCtx, cancel := context.WithTimeout(l.ctx, l.options.timeout)
		defer cancel()
		owner, found, err := readOwner(queryCtx, l.client, l.options.prefix, key)
		if err != nil {
			return nil, err
		}
		l.cache.Del(suffix)
		if found && l.cache.SetWithTTL(suffix, owner, 1, l.options.cacheTTL) {
			l.cache.Wait()
		}
		return ownerLookup{owner: owner, found: found}, nil
	})
	select {
	case value := <-result:
		if value.Err != nil {
			return Owner{}, false, value.Err
		}
		lookup := value.Val.(ownerLookup)
		return lookup.owner, lookup.found, nil
	case <-ctx.Done():
		return Owner{}, false, ctx.Err()
	case <-l.ctx.Done():
		return Owner{}, false, ErrStopped
	}
}

func (l *Locator) remember(key Key, owner Owner) {
	suffix := routeKey(key)
	lock := &l.locks[maphash.String(l.seed, suffix)%uint64(len(l.locks))]
	lock.Lock()
	defer lock.Unlock()
	if l.cache.SetWithTTL(suffix, owner, 1, l.options.cacheTTL) {
		l.cache.Wait()
	}
}

// Close 在停止使用 Client 后调用，不关闭共享 Redis 客户端
func (l *Locator) Close() {
	l.cancel()
	// 等待回源与回填退出后关闭缓存
	for i := range l.locks {
		l.locks[i].Lock()
	}
	l.cache.Close()
	for i := range l.locks {
		l.locks[i].Unlock()
	}
}
