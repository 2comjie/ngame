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
	ctx, cancel := context.WithTimeout(session.Context(), g.startupTimeout)
	defer cancel()
	if err := g.binder.Bind(ctx, uid, g.instance.Id); err != nil {
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
	ctx, cancel := context.WithTimeout(context.Background(), g.startupTimeout)
	defer cancel()
	if err := g.binder.Unbind(ctx, uid, g.instance.Id); err != nil {
		logx.Warnf("gate: 解绑失败 uid=%d error=%v", uid, err)
	}
}

func (g *App) syncSessions() {
	defer close(g.syncDone)
	ticker := time.NewTicker(g.binder.syncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-g.ctx.Done():
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
		ctx, cancel := context.WithTimeout(g.ctx, g.binder.syncInterval)
		lost, err := g.binder.renew(ctx, g.instance.Id, uids)
		cancel()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logx.Warnf("gate: 批量同步会话失败 id=%s error=%v", g.instance.Id, err)
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
