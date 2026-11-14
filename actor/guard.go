package actor

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/2comjie/ntool/logx"
	"github.com/2comjie/ntool/util/gopool"
	"github.com/redis/go-redis/v9"
)

//go:embed lua/acquire.lua
var acquireScript string

//go:embed lua/renew.lua
var renewScript string

//go:embed lua/release.lua
var releaseScript string

var (
	guardAcquire = redis.NewScript(acquireScript)
	guardRenew   = redis.NewScript(renewScript)
	guardRelease = redis.NewScript(releaseScript)

	ErrGuardLost    = errors.New("actor: 激活租约已失效")
	errActorNotHere = errors.New("actor: 目标激活不在本节点")
)

type Owner struct {
	NodeId       string `json:"node_id"`
	ActivationId string `json:"activation_id"`
}

// OwnedError 当前权威归属，不保证启动完成
type OwnedError struct {
	Key   Key
	Owner Owner
}

func (e *OwnedError) Error() string {
	return fmt.Sprintf("actor: %s/%s 已归属节点 %s，激活实例 %s", e.Key.Type, e.Key.Id, e.Owner.NodeId, e.Owner.ActivationId)
}

type guardOptions struct {
	prefix string
	ttl    time.Duration
}

type GuardOption func(*guardOptions)

func WithGuardTTL(ttl time.Duration) GuardOption {
	return func(o *guardOptions) { o.ttl = ttl }
}

func WithGuardPrefix(prefix string) GuardOption {
	return func(o *guardOptions) { o.prefix = prefix }
}

// Guard 用 Redis 租约协调归属，所有节点共享 Redis 和前缀
// Redis 客户端由调用方关闭
type Guard struct {
	nodeId  string
	client  redis.UniversalClient
	options guardOptions
}

func NewGuard(nodeId string, client redis.UniversalClient, opts ...GuardOption) *Guard {
	o := guardOptions{
		prefix: "ngame:actor:",
		ttl:    15 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if nodeId == "" || client == nil || o.prefix == "" || o.ttl < 3*time.Millisecond || o.ttl%time.Millisecond != 0 {
		panic("actor: Guard 必须提供节点 Id、Redis、非空前缀和至少 3ms 的整数毫秒 TTL")
	}
	return &Guard{
		nodeId:  nodeId,
		client:  client,
		options: o,
	}
}

// TryAcquire 抢占后自动续租，ctx 仅限制抢占
// ActivationId 只用于本次激活，重新激活须更换
func (g *Guard) TryAcquire(ctx context.Context, key Key, activationId string) (*Lease, Owner, bool, error) {
	if key.Type == "" || key.Id == "" {
		return nil, Owner{}, false, ErrInvalidKey
	}
	if activationId == "" {
		panic("actor: 抢占必须提供 ActivationId")
	}
	owner := Owner{
		NodeId:       g.nodeId,
		ActivationId: activationId,
	}
	data, _ := json.Marshal(owner) // Owner 只有字符串字段
	// 转义分隔符，避免不同 Key 使用同一记录
	redisKey := g.options.prefix + routeKey(key)
	began := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, g.options.ttl/3)
	value, err := guardAcquire.Run(callCtx, g.client, []string{redisKey}, data, g.options.ttl.Milliseconds()).Text()
	cancel()
	if err != nil {
		return nil, Owner{}, false, err
	}
	if value != string(data) {
		owner = Owner{}
		if err := json.Unmarshal([]byte(value), &owner); err != nil {
			return nil, Owner{}, false, fmt.Errorf("actor: 归属记录损坏: %w", err)
		}
		if owner.NodeId == "" || owner.ActivationId == "" {
			return nil, Owner{}, false, errors.New("actor: 归属记录缺少 NodeId 或 ActivationId")
		}
		return nil, owner, false, nil
	}
	deadline := began.Add(g.options.ttl)
	if !time.Now().Before(deadline) {
		return nil, Owner{}, false, ErrGuardLost
	}
	lifetime, stop := context.WithCancel(context.Background())
	lease := &Lease{
		guard:    g,
		key:      redisKey,
		value:    string(data),
		ctx:      lifetime,
		cancel:   stop,
		deadline: deadline,
	}
	lease.expiry = time.AfterFunc(time.Until(deadline), stop)
	gopool.CtxGo(lease.ctx, lease.renew)
	return lease, owner, true, nil
}

// Owner 直接查询 Redis，未命中返回 found=false
func (g *Guard) Owner(ctx context.Context, key Key) (Owner, bool, error) {
	return readOwner(ctx, g.client, g.options.prefix, key)
}

func routeKey(key Key) string {
	return url.PathEscape(key.Type) + "/" + url.PathEscape(key.Id)
}

func readOwner(ctx context.Context, client redis.UniversalClient, prefix string, key Key) (Owner, bool, error) {
	if key.Type == "" || key.Id == "" {
		return Owner{}, false, ErrInvalidKey
	}
	value, err := client.Get(ctx, prefix+routeKey(key)).Bytes()
	if errors.Is(err, redis.Nil) {
		return Owner{}, false, nil
	}
	if err != nil {
		return Owner{}, false, err
	}
	var owner Owner
	if err := json.Unmarshal(value, &owner); err != nil {
		return Owner{}, false, fmt.Errorf("actor: 归属记录损坏: %w", err)
	}
	if owner.NodeId == "" || owner.ActivationId == "" {
		return Owner{}, false, errors.New("actor: 归属记录缺少 NodeId 或 ActivationId")
	}
	return owner, true, nil
}

// Lease 本次激活的租约，失效后不自动抢占
type Lease struct {
	guard  *Guard
	key    string
	value  string
	ctx    context.Context
	cancel context.CancelFunc
	expiry *time.Timer // Redis 调用阻塞时也能按本地期限关闭 Done

	mu       sync.Mutex
	deadline time.Time // 从 Redis 请求发出时间计算，保守估计本地有效期
}

// Done 在主动释放或无法继续确认所有权时关闭
func (l *Lease) Done() <-chan struct{} { return l.ctx.Done() }

// Active 检查本地期限，避免调度延迟放行旧实例
func (l *Lease) Active() bool {
	l.mu.Lock()
	expired := !time.Now().Before(l.deadline)
	l.mu.Unlock()
	if expired {
		l.cancel()
	}
	return l.ctx.Err() == nil
}

// Release 按所有权释放，正常退出须在 OnStop 完成后调用
// 删除失败的记录由 TTL 回收
func (l *Lease) Release(ctx context.Context) error {
	l.cancel()
	released, err := guardRelease.Run(ctx, l.guard.client, []string{l.key}, l.value).Int()
	if err != nil {
		return err
	}
	if released == 0 {
		return ErrGuardLost
	}
	return nil
}

func (l *Lease) renew() {
	defer l.cancel()
	defer l.expiry.Stop()
	interval := l.guard.options.ttl / 3
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			if !l.Active() {
				return
			}
			began := time.Now()
			ctx, cancel := context.WithTimeout(l.ctx, interval)
			renewed, err := guardRenew.Run(ctx, l.guard.client, []string{l.key}, l.value, l.guard.options.ttl.Milliseconds()).Int()
			cancel()
			if err != nil || renewed == 0 || !l.Active() {
				if l.ctx.Err() == nil {
					logx.Warnf("actor: 激活续租失败 key=%s renewed=%d error=%v", l.key, renewed, err)
				}
				return
			}
			l.mu.Lock()
			l.deadline = began.Add(l.guard.options.ttl)
			l.expiry.Reset(time.Until(l.deadline))
			l.mu.Unlock()
		}
	}
}
