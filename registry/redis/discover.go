package redisRegistry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"sort"
	"time"

	"github.com/2comjie/ngame/endpoint"
	"github.com/redis/go-redis/v9"
)

func (r *Registry) Get(ctx context.Context, instanceId string) (endpoint.ServiceInst, bool, error) {
	data, err := r.client.HGet(ctx, r.keys[0], instanceId).Bytes()
	if errors.Is(err, redis.Nil) {
		return endpoint.ServiceInst{}, false, nil
	}
	if err != nil {
		return endpoint.ServiceInst{}, false, err
	}
	var instance endpoint.ServiceInst
	if err := json.Unmarshal(data, &instance); err != nil {
		return endpoint.ServiceInst{}, false, err
	}
	return instance, true, nil
}

func (r *Registry) List(ctx context.Context, name string) ([]endpoint.ServiceInst, error) {
	values, err := r.client.HGetAll(ctx, r.keys[0]).Result()
	if err != nil {
		return nil, err
	}
	instances := make([]endpoint.ServiceInst, 0, len(values))
	for _, value := range values {
		var instance endpoint.ServiceInst
		if err := json.Unmarshal([]byte(value), &instance); err != nil {
			return nil, err
		}
		if name == "" || instance.Name == name {
			instances = append(instances, instance)
		}
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].Id < instances[j].Id })
	return instances, nil
}

func (r *Registry) Watch(ctx context.Context, name string, onChange func([]endpoint.ServiceInst)) error {
	if onChange == nil {
		panic("registry/redis: 必须提供监听回调")
	}
	subscription := r.client.Subscribe(ctx, r.channel)
	defer subscription.Close()
	// 先确认订阅，再读取初始列表，避免在二者之间漏掉变更通知。
	if _, err := subscription.Receive(ctx); err != nil {
		return err
	}
	updates := subscription.ChannelWithSubscriptions()
	ticker := time.NewTicker(r.syncInterval)
	defer ticker.Stop()
	var previous []endpoint.ServiceInst
	initial := true
	for {
		instances, err := r.List(ctx, name)
		if err != nil {
			return err
		}
		if initial || !reflect.DeepEqual(previous, instances) {
			// 保留独立副本用于比较，回调可以保留或修改收到的快照。
			previous = make([]endpoint.ServiceInst, len(instances))
			copy(previous, instances)
			for i := range previous {
				previous[i].Meta = maps.Clone(instances[i].Meta)
			}
			initial = false
			onChange(instances)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case _, ok := <-updates:
			// 变更通知和重连后的订阅确认都会触发一次重新拉取。
			if !ok {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("registry/redis: 订阅已结束")
			}
		}
	}
}
