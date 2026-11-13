package actor

import (
	"context"
	"sync"
	"time"

	"github.com/2comjie/ntool/logx"
)

// Lease 持有一次激活的所有权。失效后不会自动重新抢占。
type Lease struct {
	guard  *Guard
	key    string
	value  string
	ctx    context.Context
	cancel context.CancelFunc
	expiry *time.Timer // Redis 调用阻塞时也能按本地期限关闭 Done。

	mu       sync.Mutex
	deadline time.Time // 从 Redis 请求发出时间计算，保守估计本地有效期。
}

// Done 在主动释放或无法继续确认所有权时关闭。
func (l *Lease) Done() <-chan struct{} { return l.ctx.Done() }

// Active 同时检查本地期限，防止进程暂停后续租协程尚未调度就执行旧实例。
func (l *Lease) Active() bool {
	l.mu.Lock()
	expired := !time.Now().Before(l.deadline)
	l.mu.Unlock()
	if expired {
		l.cancel()
	}
	return l.ctx.Err() == nil
}

// Release 停止自动续租，再按完整所有权比较删除；不能影响同 Key 的新激活。
// 正常停止须在 OnStop 完成后调用。失败时遗留记录由 TTL 清理。
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
