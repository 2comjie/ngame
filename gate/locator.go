package gate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
	"golang.org/x/sync/singleflight"
)

type LocatorConfig struct {
	Redis     redis.UniversalClient
	Prefix    string
	CacheSize int
	// CacheTTL 默认 5 分钟；目标会话失效时主动刷新。
	CacheTTL time.Duration
}

type Locator struct {
	client    redis.UniversalClient
	keys      [sessionRouteBuckets]string
	ttl       time.Duration
	cache     *ristretto.Cache[uint64, string]
	locks     [256]sync.Mutex
	refreshes singleflight.Group
}

type routeLookup struct {
	gateId string
	found  bool
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
		config.CacheTTL = 5 * time.Minute
	}
	if strings.ContainsAny(config.Prefix, "{}") || config.CacheSize < 1 || config.CacheTTL <= 0 {
		panic("gate: 路由前缀不能含花括号，缓存容量和 TTL 必须大于0")
	}
	cache, err := ristretto.NewCache(&ristretto.Config[uint64, string]{
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

func (l *Locator) Locate(ctx context.Context, uid uint64) (string, bool, error) {
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

func (l *Locator) Refresh(ctx context.Context, uid uint64, previous string) (string, bool, error) {
	if route, ok := l.cache.Get(uid); ok && route != previous {
		return route, true, nil
	}
	// 同一条旧归属的并发失败共享查询，不与新网关归属发起的刷新混用。
	key := fmt.Sprintf("%d/%s", uid, previous)
	value, err, _ := l.refreshes.Do(key, func() (any, error) {
		lock := &l.locks[uid%uint64(len(l.locks))]
		lock.Lock()
		defer lock.Unlock()
		if route, ok := l.cache.Get(uid); ok && route != previous {
			return routeLookup{gateId: route, found: true}, nil
		}
		l.cache.Del(uid)
		route, found, err := l.load(ctx, uid)
		return routeLookup{gateId: route, found: found}, err
	})
	if err != nil {
		return "", false, err
	}
	result := value.(routeLookup)
	return result.gateId, result.found, nil
}

func (l *Locator) load(ctx context.Context, uid uint64) (string, bool, error) {
	gateId, err := l.client.HGet(ctx, l.keys[uid%sessionRouteBuckets], cast.ToString(uid)).Result()
	if errors.Is(err, redis.Nil) {
		l.cache.Del(uid)
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if gateId == "" {
		return "", false, fmt.Errorf("gate: Uid %d 的网关归属为空", uid)
	}
	if l.cache.SetWithTTL(uid, gateId, 1, l.ttl) {
		// 等待回填处理完成再释放 Uid 锁，避免等待者立即 miss 再次回源。
		l.cache.Wait()
	}
	return gateId, true, nil
}

// Close 在停止使用 Locator 后调用，释放缓存的后台资源，不关闭共享 Redis 客户端。
func (l *Locator) Close() {
	l.cache.Close()
}
