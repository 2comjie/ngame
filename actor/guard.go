package actor

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

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

	ErrGuardLost = errors.New("actor: 激活租约已失效")
)

type Owner struct {
	NodeId       string `json:"node_id"`
	ActivationId string `json:"activation_id"`
}

// OwnedError 表示 Key 已被占用。Owner 是抢占时的权威归属，不代表初始化已经完成。
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

// Guard 使用一条带 TTL 的 Redis 记录管理每个 Key 的归属。
// 所有节点须使用同一 Redis 和前缀；共享 Redis 客户端由调用方关闭。
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

// TryAcquire ctx 只限制本次抢占；成功后自动续租，直到 Release 或租约失效。
// 相同 ActivationId 只能用于同一次激活的确认，重新激活必须更换。
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
	data, _ := json.Marshal(owner) // Owner 只有字符串字段。
	// 分段转义使 Type、Id 中的分隔符不会导致两个不同 Key 使用同一条记录。
	redisKey := g.options.prefix + url.PathEscape(key.Type) + "/" + url.PathEscape(key.Id)
	began := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, g.options.ttl/3)
	value, err := guardAcquire.Run(callCtx, g.client, []string{redisKey}, data, g.options.ttl.Milliseconds()).Text()
	cancel()
	if err != nil {
		return nil, Owner{}, false, err
	}
	if value != string(data) {
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

// Owner 直接读取 Redis，不经过路由缓存。不存在时 found=false。
func (g *Guard) Owner(ctx context.Context, key Key) (Owner, bool, error) {
	if key.Type == "" || key.Id == "" {
		return Owner{}, false, ErrInvalidKey
	}
	value, err := g.client.Get(ctx, g.options.prefix+url.PathEscape(key.Type)+"/"+url.PathEscape(key.Id)).Bytes()
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
