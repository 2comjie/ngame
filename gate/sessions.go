package gate

import (
	"context"
	"errors"
	"time"

	"github.com/2comjie/ngame/network"
	"github.com/2comjie/ntool/logx"
)

func (g *App) onSessionBind(session *network.Session) error {
	ctx, cancel := context.WithTimeout(session.Context(), g.startupTimeout)
	defer cancel()
	bound, err := g.binder.Bind(ctx, session.Uid(), SessionRoute{
		GateId:    g.instance.Id,
		SessionId: session.Id,
	})
	if err != nil {
		return err
	}
	if !bound {
		return ErrSessionNotHere
	}
	return nil
}

func (g *App) onSessionEnd(session *network.Session) {
	// Session context 已取消，条件解绑使用独立的短期限。
	ctx, cancel := context.WithTimeout(context.Background(), g.startupTimeout)
	defer cancel()
	if err := g.binder.Unbind(ctx, session.Uid(), SessionRoute{
		GateId:    g.instance.Id,
		SessionId: session.Id,
	}); err != nil {
		logx.Warnf("gate: 解绑失败 uid=%d session=%d error=%v", session.Uid(), session.Id, err)
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
		sessions := make([]sessionBinding, 0, len(current))
		for _, session := range current {
			sessions = append(sessions, sessionBinding{
				uid:       session.Uid(),
				sessionId: session.Id,
			})
		}
		if len(sessions) == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(g.ctx, g.binder.syncInterval)
		lost, err := g.binder.renew(ctx, g.instance.Id, sessions)
		cancel()
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logx.Warnf("gate: 批量同步会话失败 id=%s error=%v", g.instance.Id, err)
			}
			continue
		}
		for _, index := range lost {
			session := sessions[index-1]
			if _, err := g.server.KickUidSession(session.uid, session.sessionId); err != nil {
				logx.Warnf("gate: 关闭失去归属的会话失败 uid=%d error=%v", session.uid, err)
			}
		}
	}
}
