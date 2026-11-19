package gate

import (
	"context"
	"errors"
	"time"

	"github.com/2comjie/ngame/network"
	"github.com/2comjie/ntool/logx"
)

func (g *App) onSessionBind(session *network.Session) error {
	uid := session.Uid()
	lock := &g.bindings[uid%uint64(len(g.bindings))]
	lock.Lock()
	defer lock.Unlock()
	if g.server.Session(uid) != session {
		return ErrSessionNotHere
	}
	ctx, cancel := context.WithTimeout(session.Context(), g.host.StartupTimeout())
	defer cancel()
	if err := g.binder.Bind(ctx, uid, g.InstanceId()); err != nil {
		return err
	}
	if g.server.Session(uid) != session {
		return ErrSessionNotHere
	}
	return nil
}

func (g *App) onSessionEnd(session *network.Session) {
	uid := session.Uid()
	lock := &g.bindings[uid%uint64(len(g.bindings))]
	lock.Lock()
	defer lock.Unlock()
	// 旧连接退出时，新连接可能已经接管该玩家。
	if g.server.Session(uid) != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), g.host.StartupTimeout())
	defer cancel()
	if err := g.binder.Unbind(ctx, uid, g.InstanceId()); err != nil {
		logx.Warnf("gate: 解绑失败 uid=%d error=%v", uid, err)
	}
}

func (g *App) syncSessions() {
	defer close(g.syncDone)
	ctx := g.Context()
	gateId := g.InstanceId()
	ticker := time.NewTicker(g.binder.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		current := g.server.Sessions()
		uids := make([]uint64, 0, len(current))
		for _, session := range current {
			uids = append(uids, session.Uid())
		}
		if len(uids) == 0 {
			continue
		}
		renewCtx, cancel := context.WithTimeout(ctx, g.binder.syncInterval)
		lost, err := g.binder.renew(renewCtx, gateId, uids)
		cancel()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logx.Warnf("gate: 批量同步会话失败 id=%s error=%v", gateId, err)
			}
			continue
		}
		for _, session := range current {
			if _, expired := lost[session.Uid()]; !expired {
				continue
			}
			// 只关闭本轮快照中的连接，不按 Uid 查找后来替换的新连接。
			if err := session.Close(); err != nil {
				logx.Warnf("gate: 关闭失去归属的连接失败 uid=%d error=%v", session.Uid(), err)
			}
		}
	}
}
