package actor

import (
	"bytes"
	"context"

	"github.com/2comjie/ntool/logx"
)

type executionContextKey struct{}

// mailboxEntry 承载主循环闭包或业务消息
type mailboxEntry struct {
	task    func(context.Context) error
	ctx     context.Context
	message Message
	reply   chan messageResult
}

type messageResult struct {
	body []byte
	err  error
}

// execute 在主循环执行，返回是否更新活跃时间
func (r *runner) execute(value Actor, entry *mailboxEntry) bool {
	ctx := r.ctx
	if entry.reply != nil {
		// 同步调用响应双方取消；异步任务只跟随 Actor 生命周期
		callCtx, cancel := context.WithCancel(entry.ctx)
		stop := context.AfterFunc(r.ctx, cancel)
		defer stop()
		defer cancel()
		ctx = context.WithValue(callCtx, executionContextKey{}, r)
	}
	err := ctx.Err()
	executed := err == nil
	var body []byte
	if executed {
		if entry.task != nil {
			err = entry.task(ctx)
		} else {
			body, err = value.OnMessage(MessageContext{Context: ctx, Self: r}, entry.message)
		}
	}
	if entry.reply != nil {
		entry.reply <- messageResult{body: bytes.Clone(body), err: err}
	} else if err != nil {
		logx.Errorf("actor: 异步任务失败 type=%s id=%s method=%s error=%v", r.key.Type, r.key.Id, entry.message.Method, err)
	}
	return executed
}

func (r *runner) waitResult(ctx context.Context, reply <-chan messageResult) ([]byte, error) {
	select {
	case result := <-reply:
		return result.body, result.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.ctx.Done():
		if !r.lease.Active() {
			return nil, ErrGuardLost
		}
		return nil, ErrStopping
	}
}

func (s *System) enqueue(ctx context.Context, key Key, message Message, reply chan messageResult, activationId string) (*runner, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if key.Type == "" || key.Id == "" {
		return nil, ErrInvalidKey
	}
	s.mutex.Lock()
	if s.stopping {
		s.mutex.Unlock()
		return nil, ErrStopped
	}
	if !s.started {
		s.mutex.Unlock()
		return nil, ErrNotStarted
	}
	r := s.actors[key]
	if r == nil {
		s.mutex.Unlock()
		if activationId != "" {
			return nil, errActorNotHere
		}
		return nil, ErrNotFound
	}
	if reply != nil && ctx.Value(executionContextKey{}) == r {
		s.mutex.Unlock()
		return nil, ErrSelfAsk
	}
	r.mu.Lock()
	s.mutex.Unlock()
	defer r.mu.Unlock()
	// 校验与入队原子执行，入队前拒绝才允许重试
	if activationId != "" && (r.activationId != activationId || r.state != stateRunning || !r.lease.Active()) {
		return nil, errActorNotHere
	}
	if r.state >= stateStopping {
		return nil, ErrStopping
	}
	if r.state != stateRunning {
		return nil, ErrNotFound
	}
	message.Body = bytes.Clone(message.Body)
	entry := &mailboxEntry{
		message: message,
		reply:   reply,
	}
	if reply != nil {
		entry.ctx = ctx
	}
	select {
	case r.mailbox <- entry:
		return r, nil
	default:
		return nil, ErrMailboxFull
	}
}

func (r *runner) RunOnMainLoop(task Task) error {
	if task == nil {
		panic("actor: 必须提供 Task")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.state >= stateStopping {
		return ErrStopping
	}
	select {
	case r.mailbox <- &mailboxEntry{
		task: func(ctx context.Context) error {
			task(ctx)
			return nil
		},
	}:
		return nil
	default:
		return ErrMailboxFull
	}
}

func (r *runner) WaitResultOnMainLoop(ctx context.Context, task func(context.Context) error) error {
	if task == nil {
		panic("actor: 必须提供 Task")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if ctx.Value(executionContextKey{}) == r {
		return ErrSelfWait
	}
	reply := make(chan messageResult, 1)
	r.mu.Lock()
	if r.state >= stateStopping {
		r.mu.Unlock()
		return ErrStopping
	}
	if r.state != stateRunning {
		r.mu.Unlock()
		return ErrNotFound
	}
	select {
	case r.mailbox <- &mailboxEntry{
		task:  task,
		ctx:   ctx,
		reply: reply,
	}:
		r.mu.Unlock()
	default:
		r.mu.Unlock()
		return ErrMailboxFull
	}
	_, err := r.waitResult(ctx, reply)
	return err
}

// 调用方持有 r.mu，丢弃排队任务并通知等待者
func (r *runner) drainMailbox(err error) {
	for {
		select {
		case entry := <-r.mailbox:
			if entry.reply != nil {
				entry.reply <- messageResult{err: err}
			}
		default:
			return
		}
	}
}
