package gate

import (
	"context"
	_ "embed"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cast"
)

//go:embed bind.lua
var bindScript string

//go:embed unbind.lua
var unbindScript string

//go:embed renew.lua
var renewScript string

const sessionRouteBuckets = 10
const sessionSyncBatch = 256

// 桶号放入 Hash Tag，使不同桶可以分布到不同的 Redis Cluster 槽。
func sessionRouteKeys(prefix string) [sessionRouteBuckets]string {
	var keys [sessionRouteBuckets]string
	for bucket := range keys {
		keys[bucket] = "{" + prefix + ":" + cast.ToString(bucket) + "}:sessions"
	}
	return keys
}

type BinderConfig struct {
	Redis        redis.UniversalClient
	Prefix       string
	TTL          time.Duration
	SyncInterval time.Duration
}

type Binder struct {
	client       redis.UniversalClient
	keys         [sessionRouteBuckets]string
	ttl          time.Duration
	syncInterval time.Duration
}

func NewBinder(config BinderConfig) *Binder {
	if config.Redis == nil {
		panic("gate: 必须提供 Redis 客户端")
	}
	if config.Prefix == "" {
		config.Prefix = "ngame:gate"
	}
	if config.TTL == 0 {
		config.TTL = 15 * time.Second
	}
	if config.SyncInterval == 0 {
		config.SyncInterval = 5 * time.Second
	}
	if strings.ContainsAny(config.Prefix, "{}") || config.TTL < time.Second || config.TTL%time.Second != 0 || config.SyncInterval <= 0 || config.SyncInterval >= config.TTL {
		panic("gate: 路由前缀不能含花括号，TTL 须为正整数秒，且 0 < SyncInterval < TTL")
	}
	return &Binder{
		client:       config.Redis,
		keys:         sessionRouteKeys(config.Prefix),
		ttl:          config.TTL,
		syncInterval: config.SyncInterval,
	}
}

// Bind 将玩家归属绑定到本次 Gate 实例，同网关的连接替换由网络层处理。
func (r *Binder) Bind(ctx context.Context, uid uint64, gateId string) error {
	if uid == 0 || gateId == "" {
		return errors.New("gate: 绑定必须提供 Uid、GateId")
	}
	return r.client.Eval(ctx, bindScript, []string{r.keys[uid%sessionRouteBuckets]}, cast.ToString(uid), gateId, int64(r.ttl/time.Second)).Err()
}

func (r *Binder) Unbind(ctx context.Context, uid uint64, gateId string) error {
	return r.client.Eval(ctx, unbindScript, []string{r.keys[uid%sessionRouteBuckets]}, cast.ToString(uid), gateId).Err()
}

// renew 返回已不属于本实例的 Uid，不覆盖新归属或重建过期记录。
func (r *Binder) renew(ctx context.Context, gateId string, uids []uint64) (map[uint64]struct{}, error) {
	var buckets [sessionRouteBuckets][]uint64
	for _, uid := range uids {
		bucket := uid % sessionRouteBuckets
		buckets[bucket] = append(buckets[bucket], uid)
	}
	pipeline := r.client.Pipeline()
	var commands []*redis.Cmd
	for bucket, values := range buckets {
		for start := 0; start < len(values); start += sessionSyncBatch {
			batch := values[start:min(start+sessionSyncBatch, len(values))]
			args := make([]any, 0, 2+len(batch))
			args = append(args, int64(r.ttl/time.Second), gateId)
			for _, uid := range batch {
				args = append(args, cast.ToString(uid))
			}
			commands = append(commands, pipeline.Eval(ctx, renewScript, []string{r.keys[bucket]}, args...))
		}
	}
	if _, err := pipeline.Exec(ctx); err != nil {
		return nil, err
	}
	var lost map[uint64]struct{}
	for _, command := range commands {
		uids, err := command.StringSlice()
		if err != nil {
			return nil, err
		}
		for _, uid := range uids {
			if lost == nil {
				lost = make(map[uint64]struct{})
			}
			lost[cast.ToUint64(uid)] = struct{}{}
		}
	}
	return lost, nil
}
