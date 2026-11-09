package redisRegistry

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ntool/logx"
	"github.com/redis/go-redis/v9"
)

type discoverySource struct {
	*Registry
}

func (r *discoverySource) Watch(ctx context.Context, onChange func([]endpoint.ServiceInst)) error {
	ctx, cancel := context.WithCancel(ctx)
	refresh := make(chan struct{}, 1)
	db := 0
	if client, ok := r.client.(*redis.Client); ok {
		db = client.Options().DB
	}
	var wait sync.WaitGroup
	wait.Go(func() { r.watchSignals(ctx, r.channel, "", refresh) })
	wait.Go(func() { r.watchSignals(ctx, "__keyevent@"+strconv.Itoa(db)+"__:hexpired", r.keys[0], refresh) })
	defer func() { cancel(); wait.Wait() }()
	ticker := time.NewTicker(r.syncInterval)
	defer ticker.Stop()
	for {
		values, err := r.client.HGetAll(ctx, r.keys[0]).Result()
		if err != nil {
			return err
		}
		instances := make([]endpoint.ServiceInst, 0, len(values))
		for id, value := range values {
			var instance endpoint.ServiceInst
			if err := json.Unmarshal([]byte(value), &instance); err != nil {
				logx.Errorf("registry/redis: 实例信息损坏 id=%s error=%v", id, err)
				continue
			}
			instances = append(instances, instance)
		}
		onChange(instances)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-refresh:
		}
	}
}

// 通知和过期事件独立订阅；任何一路异常时，前台全量同步仍能继续。
func (r *discoverySource) watchSignals(ctx context.Context, channel, key string, refresh chan<- struct{}) {
	for {
		if ctx.Err() != nil {
			return
		}
		subscription := r.client.Subscribe(ctx, channel)
		messages := subscription.ChannelWithSubscriptions()
		stop := context.AfterFunc(ctx, func() { _ = subscription.Close() })
		listening := true
		for listening {
			select {
			case <-ctx.Done():
				listening = false
			case message, ok := <-messages:
				if !ok {
					listening = false
					continue
				}
				switch message := message.(type) {
				case *redis.Subscription:
					if message.Kind != "subscribe" {
						continue
					}
					// 订阅确认后重新拉取，覆盖初次订阅及重连间隙的变更。
				case *redis.Message:
					if key != "" && message.Payload != key {
						continue
					}
				default:
					continue
				}
				select {
				case refresh <- struct{}{}:
				default:
				}
			}
		}
		stop()
		_ = subscription.Close()
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
}
