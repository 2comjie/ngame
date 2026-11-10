package gate

import (
	"context"
	_ "embed"
	"encoding/json"
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

type sessionBinding struct {
	uid       uint64
	sessionId uint64
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

func (r *Binder) Bind(ctx context.Context, uid uint64, route SessionRoute) (bool, error) {
	if uid == 0 || route.GateId == "" || route.SessionId == 0 {
		return false, errors.New("gate: 绑定必须提供 Uid、GateId、SessionId")
	}
	value, err := json.Marshal(route)
	if err != nil {
		return false, err
	}
	result, err := r.client.Eval(ctx, bindScript, []string{r.keys[uid%sessionRouteBuckets]}, cast.ToString(uid), route.GateId, cast.ToString(route.SessionId), value, int64(r.ttl/time.Second)).Int()
	return result == 1, err
}

func (r *Binder) Unbind(ctx context.Context, uid uint64, route SessionRoute) error {
	value, err := json.Marshal(route)
	if err != nil {
		return err
	}
	return r.client.Eval(ctx, unbindScript, []string{r.keys[uid%sessionRouteBuckets]}, cast.ToString(uid), value).Err()
}

func (r *Binder) renew(ctx context.Context, gateId string, sessions []sessionBinding) ([]int64, error) {
	var buckets [sessionRouteBuckets][]int
	for index, session := range sessions {
		bucket := session.uid % sessionRouteBuckets
		buckets[bucket] = append(buckets[bucket], index)
	}
	pipeline := r.client.Pipeline()
	var commands []*redis.Cmd
	var batches [][]int
	for bucket, indices := range buckets {
		for start := 0; start < len(indices); start += sessionSyncBatch {
			batch := indices[start:min(start+sessionSyncBatch, len(indices))]
			args := make([]any, 1, 1+2*len(batch))
			args[0] = int64(r.ttl / time.Second)
			for _, index := range batch {
				session := sessions[index]
				value, err := json.Marshal(SessionRoute{
					GateId:    gateId,
					SessionId: session.sessionId,
				})
				if err != nil {
					return nil, err
				}
				args = append(args, cast.ToString(session.uid), value)
			}
			commands = append(commands, pipeline.Eval(ctx, renewScript, []string{r.keys[bucket]}, args...))
			batches = append(batches, batch)
		}
	}
	if _, err := pipeline.Exec(ctx); err != nil {
		return nil, err
	}
	var lost []int64
	for i, command := range commands {
		indices, err := command.Int64Slice()
		if err != nil {
			return nil, err
		}
		for _, index := range indices {
			lost = append(lost, int64(batches[i][index-1]+1))
		}
	}
	return lost, nil
}
