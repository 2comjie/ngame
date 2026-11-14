package actor

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"

	"github.com/2comjie/ngame/app"
	"github.com/2comjie/ntool/logx"
)

var (
	ErrNotStarted        = errors.New("actor: System 尚未启动")
	ErrStopped           = errors.New("actor: System 已停止")
	ErrStopping          = errors.New("actor: Actor 正在停止")
	ErrNotFound          = errors.New("actor: Actor 不存在")
	ErrEntityNotFound    = errors.New("actor: 业务实体不存在")
	ErrInvalidKey        = errors.New("actor: 必须提供 Type 和 Id")
	ErrTypeNotRegistered = errors.New("actor: Actor 类型未注册")
	ErrMailboxFull       = errors.New("actor: 邮箱已满")
	ErrInvalidTimer      = errors.New("actor: After 延迟不能为负数，Every 间隔必须大于 0")
)

const stateStarting = 1
const stateRunning = 2
const stateStopping = 3
const stateStopped = 4

type System struct {
	guard    *Guard
	mutex    sync.Mutex
	types    map[string]actorType
	actors   map[Key]*runner
	options  options
	ctx      context.Context
	cancel   context.CancelFunc
	started  bool
	stopping bool
	wait     sync.WaitGroup
	done     chan struct{}
}

var _ app.Component = (*System)(nil)

func New(guard *Guard, opts ...Option) *System {
	if guard == nil {
		panic("actor: 必须提供全局激活协调器 Guard")
	}
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if o.mailboxSize < 1 || o.updateInterval <= 0 || o.lifecycleTimeout <= 0 {
		panic("actor: 邮箱容量、更新间隔和生命周期超时必须大于 0")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &System{
		guard:   guard,
		types:   make(map[string]actorType),
		actors:  make(map[Key]*runner),
		options: o,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
}

func (s *System) Register(name string, loader Loader, opts ...TypeOption) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if name == "" || loader == nil {
		panic("actor: 必须提供 Actor 类型和 Loader")
	}
	if s.started || s.stopping {
		panic("actor: Loader 必须在 System.Start 前注册")
	}
	if _, exists := s.types[name]; exists {
		panic("actor: Actor 类型重复注册: " + name)
	}
	definition := actorType{loader: loader}
	for _, opt := range opts {
		opt(&definition)
	}
	s.types[name] = definition
}

func (s *System) Start() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.stopping {
		return ErrStopped
	}
	if s.started {
		return app.ErrStarted
	}
	s.started = true
	return nil
}

// Spawn 本节点并发调用共享一次激活，跨节点通过 Guard 抢占。
// ctx 只限制本次等待；Loader、OnStart 使用独立的生命周期上下文。
func (s *System) Spawn(ctx context.Context, key Key) (Handle, error) {
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
		definition, exists := s.types[key.Type]
		if !exists {
			s.mutex.Unlock()
			return nil, ErrTypeNotRegistered
		}
		actorCtx, cancel := context.WithCancel(s.ctx)
		r = &runner{
			key:          key,
			activationId: rand.Text(),
			options:      s.options,
			state:        stateStarting,
			ctx:          actorCtx,
			cancel:       cancel,
			mailbox:      make(chan Task, s.options.mailboxSize),
			wake:         make(chan struct{}, 1),
			done:         make(chan struct{}),
			starting:     make(map[chan error]struct{}),
		}
		s.actors[key] = r
		s.wait.Add(1)
		go s.run(r, definition)
	}
	r.mu.Lock()
	s.mutex.Unlock()
	if r.state == stateRunning {
		r.mu.Unlock()
		return r, nil
	}
	if r.state >= stateStopping {
		r.mu.Unlock()
		return nil, ErrStopping
	}
	ready := make(chan error, 1)
	r.starting[ready] = struct{}{}
	r.mu.Unlock()
	select {
	case err := <-ready:
		if err != nil {
			return nil, err
		}
		return r, nil
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.starting, ready)
		r.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Stop 请求正常退出并等待 OnStop 完成；ctx 超时不会放行同 Key 的新实例。
func (s *System) Stop(ctx context.Context, key Key) error {
	s.mutex.Lock()
	r := s.actors[key]
	s.mutex.Unlock()
	if r == nil {
		return nil
	}
	r.Stop()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *System) RequestStop() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.stopping {
		return
	}
	s.stopping = true
	for _, r := range s.actors {
		r.requestStop(StopReasonShutdown)
	}
	s.cancel()
	// stopping 与 wait.Add 由同一把锁保护，开始 Wait 后不会新增激活任务。
	go func() {
		s.wait.Wait()
		close(s.done)
	}()
}

func (s *System) Shutdown(ctx context.Context) error {
	s.RequestStop()
	select {
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *System) run(r *runner, definition actorType) {
	defer s.wait.Done()
	defer close(r.done)
	defer r.cancel()
	var err error
	defer func() {
		// System 和 runner 同时加锁时始终先锁 System。
		s.mutex.Lock()
		r.mu.Lock()
		r.state = stateStopped
		delete(s.actors, r.key)
		for ready := range r.starting {
			ready <- err
			delete(r.starting, ready)
		}
		r.mu.Unlock()
		s.mutex.Unlock()
	}()

	acquireCtx, stopAcquire := context.WithTimeout(r.ctx, s.options.lifecycleTimeout)
	lease, owner, acquired, acquireErr := s.guard.TryAcquire(acquireCtx, r.key, r.activationId)
	stopAcquire()
	if acquireErr != nil {
		err = acquireErr
		return
	}
	if !acquired {
		err = &OwnedError{
			Key:   r.key,
			Owner: owner,
		}
		return
	}
	r.lease = lease
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), s.options.lifecycleTimeout)
		defer cancel()
		if releaseErr := lease.Release(ctx); releaseErr != nil && !errors.Is(releaseErr, ErrGuardLost) {
			logx.Errorf("actor: 释放激活租约失败 type=%s id=%s error=%v", r.key.Type, r.key.Id, releaseErr)
		}
	}()
	stopLeaseWatch := context.AfterFunc(lease.ctx, func() { r.requestStop(StopReasonLeaseLost) })
	defer stopLeaseWatch()
	if !r.checkLease() {
		err = ErrGuardLost
		return
	}
	r.mu.Lock()
	starting := r.state == stateStarting
	r.mu.Unlock()
	if !starting {
		err = ErrStopping
		return
	}

	startCtx, cancel := context.WithTimeout(r.ctx, s.options.lifecycleTimeout)
	defer cancel()
	value, err := definition.loader(startCtx, r.key)
	if errors.Is(err, ErrEntityNotFound) && definition.onNotFound != nil {
		if !r.checkLease() {
			err = ErrGuardLost
			return
		}
		if err = definition.onNotFound(startCtx, r.key); err != nil {
			return
		}
		value, err = definition.loader(startCtx, r.key)
	}
	if err != nil {
		return
	}
	if value == nil {
		panic("actor: Loader 返回了空 Actor")
	}
	if !r.checkLease() {
		err = ErrGuardLost
		return
	}
	if err = startCtx.Err(); err != nil {
		return
	}
	err = value.OnStart(StartContext{
		Context: startCtx,
		Self:    r,
	})
	started := err == nil
	if started {
		select {
		case <-startCtx.Done():
			err = startCtx.Err()
		default:
		}
	}
	cancel()
	r.mu.Lock()
	if !lease.Active() {
		err = ErrGuardLost
	} else if err == nil && r.state != stateStarting {
		err = ErrStopping
	}
	if err == nil {
		r.state = stateRunning
		for ready := range r.starting {
			ready <- nil
			delete(r.starting, ready)
		}
	}
	r.mu.Unlock()
	if err == nil {
		r.loop(value)
	}
	r.mu.Lock()
	r.state = stateStopping
	if !lease.Active() {
		r.reason = StopReasonLeaseLost
	} else if r.reason == 0 {
		r.reason = StopReasonSelf
	}
	reason := r.reason
	for _, timer := range r.timers {
		timer.index = -1
		timer.canceled = true
		timer.task = nil
	}
	r.timers = nil
	// 拒绝新任务后释放尚未执行的闭包，避免旧 Handle 长期保留业务对象。
	for len(r.mailbox) > 0 {
		<-r.mailbox
	}
	r.mu.Unlock()
	if started {
		stopCtx, stop := context.WithTimeout(context.Background(), s.options.lifecycleTimeout)
		// 正常退出继续持有租约；失去租约时也取消正在执行的 OnStop。
		stopWatch := context.AfterFunc(lease.ctx, stop)
		if !lease.Active() {
			stop()
		}
		value.OnStop(StopContext{
			Context: stopCtx,
			Self:    r,
			Reason:  reason,
		})
		stopWatch()
		stop()
	}
}
