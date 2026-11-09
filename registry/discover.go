package registry

import (
	"context"
	"maps"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/2comjie/ngame/endpoint"
	"github.com/2comjie/ntool/logx"
)

// WatchFunc 同步一个命名空间内的全部实例，首次和后续回调均提供完整快照。
// 回调按顺序执行；ctx 取消时必须退出，其他错误直接返回。
type WatchFunc func(ctx context.Context, onChange func([]endpoint.ServiceInst)) error

type discoverySnapshot struct {
	instances []endpoint.ServiceInst
}

// Discovery 共享一条后端同步链路，Get/List 读取本地，Watch 订阅本地快照。
// 首次同步前等待调用方 ctx；同步失败保留最后成功的快照，不承诺线性一致读取。
type Discovery struct {
	ctx      context.Context
	cancel   context.CancelFunc
	ready    chan struct{}
	done     chan struct{}
	mutex    sync.RWMutex
	byId     map[string]endpoint.ServiceInst
	byName   map[string]*discoverySnapshot
	all      *discoverySnapshot
	watchers map[chan struct{}]string
}

var _ Discover = (*Discovery)(nil)

// NewDiscover 启动一个共享发现缓存；调用方必须 Close，不关闭后端客户端。
func NewDiscover(watch WatchFunc) *Discovery {
	if watch == nil {
		panic("registry: 必须提供后端同步函数")
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &Discovery{ctx: ctx, cancel: cancel, ready: make(chan struct{}), done: make(chan struct{}), watchers: make(map[chan struct{}]string)}
	go func() {
		defer close(d.done)
		for {
			err := watch(ctx, d.update)
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				logx.Warnf("registry: 同步实例失败，保留本地快照 error=%v", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return d
}

func (d *Discovery) Get(ctx context.Context, instanceId string) (endpoint.ServiceInst, bool, error) {
	if err := d.waitReady(ctx); err != nil {
		return endpoint.ServiceInst{}, false, err
	}
	d.mutex.RLock()
	instance, found := d.byId[instanceId]
	d.mutex.RUnlock()
	instance.Meta = maps.Clone(instance.Meta)
	return instance, found, nil
}

func (d *Discovery) List(ctx context.Context, name string) ([]endpoint.ServiceInst, error) {
	if err := d.waitReady(ctx); err != nil {
		return nil, err
	}
	d.mutex.RLock()
	snapshot := d.all
	if name != "" {
		snapshot = d.byName[name]
	}
	d.mutex.RUnlock()
	return cloneSnapshot(snapshot), nil
}

func (d *Discovery) Watch(ctx context.Context, name string, onChange func([]endpoint.ServiceInst)) error {
	if onChange == nil {
		panic("registry: 必须提供监听回调")
	}
	if err := d.waitReady(ctx); err != nil {
		return err
	}
	notify := make(chan struct{}, 1)
	// 先订阅再取初始快照，并与发布使用同一把锁，避免漏掉衔接期间的变更。
	d.mutex.Lock()
	d.watchers[notify] = name
	snapshot := d.all
	if name != "" {
		snapshot = d.byName[name]
	}
	d.mutex.Unlock()
	defer func() {
		d.mutex.Lock()
		delete(d.watchers, notify)
		d.mutex.Unlock()
	}()
	onChange(cloneSnapshot(snapshot))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.ctx.Done():
			return ErrDiscoveryClosed
		case <-notify:
			d.mutex.RLock()
			current := d.all
			if name != "" {
				current = d.byName[name]
			}
			d.mutex.RUnlock()
			if current == snapshot {
				continue
			}
			snapshot = current
			onChange(cloneSnapshot(snapshot))
		}
	}
}

// Close 停止服务发现并唤醒本地订阅者，不注销实例、不关闭共享客户端。
func (d *Discovery) Close() {
	d.cancel()
	<-d.done
}

func (d *Discovery) waitReady(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.ctx.Done():
		return ErrDiscoveryClosed
	case <-d.ready:
	}
	if d.ctx.Err() != nil {
		return ErrDiscoveryClosed
	}
	return nil
}

func (d *Discovery) update(instances []endpoint.ServiceInst) {
	if d.ctx.Err() != nil {
		return
	}
	values := make([]endpoint.ServiceInst, len(instances))
	byId := make(map[string]endpoint.ServiceInst, len(instances))
	groups := make(map[string][]endpoint.ServiceInst)
	for i, instance := range instances {
		instance.Meta = maps.Clone(instance.Meta)
		values[i] = instance
	}
	sort.Slice(values, func(i, j int) bool { return values[i].Id < values[j].Id })
	for _, instance := range values {
		byId[instance.Id] = instance
		groups[instance.Name] = append(groups[instance.Name], instance)
	}
	d.mutex.Lock()
	defer d.mutex.Unlock()
	if d.all != nil && reflect.DeepEqual(d.all.instances, values) {
		return
	}
	byName := make(map[string]*discoverySnapshot, len(groups))
	for name, group := range groups {
		previous := d.byName[name]
		if previous != nil && reflect.DeepEqual(previous.instances, group) {
			byName[name] = previous
		} else {
			byName[name] = &discoverySnapshot{instances: group}
		}
	}
	previous := d.byName
	d.byId, d.byName = byId, byName
	if d.all == nil {
		close(d.ready)
	}
	d.all = &discoverySnapshot{instances: values}
	for notify, name := range d.watchers {
		if name != "" && previous[name] == byName[name] {
			continue
		}
		// 合并积压通知，慢订阅者下次读取最新完整快照，不阻塞后端同步。
		select {
		case notify <- struct{}{}:
		default:
		}
	}
}

func cloneSnapshot(snapshot *discoverySnapshot) []endpoint.ServiceInst {
	if snapshot == nil {
		return []endpoint.ServiceInst{}
	}
	instances := make([]endpoint.ServiceInst, len(snapshot.instances))
	for i, instance := range snapshot.instances {
		instance.Meta = maps.Clone(instance.Meta)
		instances[i] = instance
	}
	return instances
}
