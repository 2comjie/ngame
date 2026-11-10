package gate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

type LocatorConfig struct {
	Redis     redis.UniversalClient
	Prefix    string
	CacheSize int
	CacheTTL  time.Duration
}

type Locator struct {
	client redis.UniversalClient
	keys   [sessionRouteBuckets]string
	ttl    time.Duration
	cache  *ristretto.Cache[uint64, SessionRoute]
	locks  [256]sync.Mutex
}

func NewLocator(config LocatorConfig) *Locator {
	if config.Redis == nil {
		panic("gate: Locator 必须提供 Redis 客户端")
	}
	if config.Prefix == "" {
		config.Prefix = "ngame:gate"
	}
	if config.CacheSize == 0 {
		config.CacheSize = 10000
	}
	if config.CacheTTL == 0 {
		config.CacheTTL = time.Second
	}
	if strings.ContainsAny(config.Prefix, "{}") || config.CacheSize < 1 || config.CacheTTL <= 0 {
		panic("gate: 路由前缀不能含花括号，缓存容量和 TTL 必须大于0")
	}
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, SessionRoute]{
		NumCounters:        int64(config.CacheSize) * 10,
		MaxCost:            int64(config.CacheSize),
		BufferItems:        64,
		IgnoreInternalCost: true,
	})
	if err != nil {
		panic(err)
	}
	return &Locator{
		client: config.Redis,
		keys:   sessionRouteKeys(config.Prefix),
		ttl:    config.CacheTTL,
		cache:  cache,
	}
}

func (l *Locator) Locate(ctx context.Context, uid uint64) (SessionRoute, bool, error) {
	if route, ok := l.cache.Get(uid); ok {
		return route, true, nil
	}
	lock := &l.locks[uid%uint64(len(l.locks))]
	lock.Lock()
	defer lock.Unlock()
	if route, ok := l.cache.Get(uid); ok {
		return route, true, nil
	}
	return l.load(ctx, uid)
}

func (l *Locator) Refresh(ctx context.Context, uid uint64, previous SessionRoute) (SessionRoute, bool, error) {
	lock := &l.locks[uid%uint64(len(l.locks))]
	lock.Lock()
	defer lock.Unlock()
	if route, ok := l.cache.Get(uid); ok && route == previous {
		l.cache.Del(uid)
	}
	return l.load(ctx, uid)
}

func (l *Locator) load(ctx context.Context, uid uint64) (SessionRoute, bool, error) {
	value, err := l.client.HGet(ctx, l.keys[uid%sessionRouteBuckets], cast.ToString(uid)).Bytes()
	if errors.Is(err, redis.Nil) {
		l.cache.Del(uid)
		return SessionRoute{}, false, nil
	}
	if err != nil {
		return SessionRoute{}, false, err
	}
	var route SessionRoute
	if err := json.Unmarshal(value, &route); err != nil {
		return SessionRoute{}, false, fmt.Errorf("gate: Uid %d 的路由损坏: %w", uid, err)
	}
	if route.GateId == "" || route.SessionId == 0 {
		return SessionRoute{}, false, fmt.Errorf("gate: Uid %d 的网关归属无效", uid)
	}
	if l.cache.SetWithTTL(uid, route, 1, l.ttl) {
		// 等待回填处理完成再释放 Uid 锁，避免等待者立即 miss 再次回源。
		l.cache.Wait()
	}
	return route, true, nil
}

// Close 在停止使用 Locator 后调用，释放缓存的后台资源，不关闭共享 Redis 客户端。
func (l *Locator) Close() {
	l.cache.Close()
}
