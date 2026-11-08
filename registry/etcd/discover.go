package etcdRegistry

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/url"

	"github.com/2comjie/ngame/endpoint"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func (r *Registry) Get(ctx context.Context, instanceId string) (endpoint.ServiceInst, bool, error) {
	response, err := r.client.Get(ctx, r.prefix+url.PathEscape(instanceId))
	if err != nil {
		return endpoint.ServiceInst{}, false, err
	}
	if len(response.Kvs) == 0 {
		return endpoint.ServiceInst{}, false, nil
	}
	var instance endpoint.ServiceInst
	if err := json.Unmarshal(response.Kvs[0].Value, &instance); err != nil {
		return endpoint.ServiceInst{}, false, err
	}
	return instance, true, nil
}

func (r *Registry) List(ctx context.Context, name string) ([]endpoint.ServiceInst, error) {
	response, err := r.client.Get(ctx, r.prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}
	instances := make([]endpoint.ServiceInst, 0, len(response.Kvs))
	for _, kv := range response.Kvs {
		var instance endpoint.ServiceInst
		if err := json.Unmarshal(kv.Value, &instance); err != nil {
			return nil, err
		}
		if name == "" || instance.Name == name {
			instances = append(instances, instance)
		}
	}
	return instances, nil
}

func (r *Registry) Watch(ctx context.Context, name string, onChange func([]endpoint.ServiceInst)) error {
	if onChange == nil {
		panic("registry/etcd: 必须提供监听回调")
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	for {
		response, err := r.client.Get(ctx, r.prefix, clientv3.WithPrefix())
		if err != nil {
			return err
		}
		instances := make(map[string]endpoint.ServiceInst)
		for _, kv := range response.Kvs {
			var instance endpoint.ServiceInst
			if err := json.Unmarshal(kv.Value, &instance); err != nil {
				return err
			}
			if name == "" || instance.Name == name {
				instances[string(kv.Key)] = instance
			}
		}
		updates := r.client.Watch(clientv3.WithRequireLeader(ctx), r.prefix, clientv3.WithPrefix(), clientv3.WithRev(response.Header.Revision+1))
		onChange(snapshot(instances))
		resync := false
		for !resync {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case update, ok := <-updates:
				if !ok {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if r.client.Ctx().Err() != nil {
						return r.client.Ctx().Err()
					}
					return fmt.Errorf("registry/etcd: 监听已结束")
				}
				if update.CompactRevision != 0 {
					resync = true
					continue
				}
				if err := update.Err(); err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					return err
				}
				changed := false
				for _, event := range update.Events {
					key := string(event.Kv.Key)
					if event.Type == mvccpb.DELETE {
						if _, exists := instances[key]; exists {
							delete(instances, key)
							changed = true
						}
						continue
					}
					var instance endpoint.ServiceInst
					if err := json.Unmarshal(event.Kv.Value, &instance); err != nil {
						return err
					}
					if name == "" || instance.Name == name {
						instances[key] = instance
						changed = true
					} else if _, exists := instances[key]; exists {
						delete(instances, key)
						changed = true
					}
				}
				if changed {
					onChange(snapshot(instances))
				}
			}
		}
	}
}

func snapshot(instances map[string]endpoint.ServiceInst) []endpoint.ServiceInst {
	result := make([]endpoint.ServiceInst, 0, len(instances))
	for _, instance := range instances {
		instance.Meta = maps.Clone(instance.Meta)
		result = append(result, instance)
	}
	return result
}
