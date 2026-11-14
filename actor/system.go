package actor

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
	"time"

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
	ErrSelfAsk           = errors.New("actor: 不能在自己的回调中同步 Ask 自己")
	ErrSelfWait          = errors.New("actor: 不能在自己的回调中同步等待主循环任务")
	ErrInvalidTimer      = errors.New("actor: After 延迟不能为负数，Every 间隔必须大于 0")
)

const stateStarting = 1
const stateRunning = 2
const stateStopping = 3
const stateStopped = 4

type options struct {
	mailboxSize      int
	updateInterval   time.Duration
	lifecycleTimeout time.Duration
}

type Option func(*options)

func defaultOptions() options {
	return options{
		mailboxSize:      128,
		updateInterval:   time.Second,
		lifecycleTimeout: 5 * time.Second,
	}
}

func WithMailboxSize(size int) Option {
	return func(o *options) { o.mailboxSize = size }
}

func WithUpdateInterval(interval time.Duration) Option {
	return func(o *options) { o.updateInterval = interval }
}

// WithLifecycleTimeout 设置 Loader 与 OnStart 的共享超时，以及 OnStop 的超时
func WithLifecycleTimeout(timeout time.Duration) Option {
	return func(o *options) { o.lifecycleTimeout = timeout }
}

type actorType struct {
	loader     Loader
	onNotFound func(context.Context, Key) error
}

type TypeOption func(*actorType)

// WithOnNotFound 仅在 Loader 返回 ErrEntityNotFound 时调用，成功后重新加载一次
func WithOnNotFound(handler func(context.Context, Key) error) TypeOption {
	return func(o *actorType) { o.onNotFound = handler }
}

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

// Spawn 本地合并激活，跨节点通过 Guard 抢占
// ctx 仅限制等待，Loader 和 OnStart 使用独立上下文
func (s *System) Spawn(ctx context.Context, key Key) (Handle, error) {
	r, err := s.spawn(ctx, key)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (s *System) spawn(ctx context.Context, key Key) (*runner, error) {
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
		if err := ctx.Err(); err != nil {
			s.mutex.Unlock()
			return nil, err
		}
		actorCtx, cancel := context.WithCancel(s.ctx)
		r = &runner{
			key:          key,
			activationId: rand.Text(),
			options:      s.options,
			state:        stateStarting,
			ctx:          actorCtx,
			cancel:       cancel,
			mailbox:      make(chan *mailboxEntry, s.options.mailboxSize),
			wake:         make(chan struct{}, 1),
			done:         make(chan struct{}),
			starting:     make(map[chan error]struct{}),
		}
		r.ctx = context.WithValue(actorCtx, executionContextKey{}, r)
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
		return r, ErrStopping
	}
	ready := make(chan error, 1)
	r.starting[ready] = struct{}{}
	r.mu.Unlock()
	select {
	case err := <-ready:
		if err != nil {
			return r, err
		}
		return r, nil
	case <-ctx.Done():
		r.mu.Lock()
		delete(r.starting, ready)
		r.mu.Unlock()
		return nil, ctx.Err()
	}
}

// Stop 等待 OnStop 完成，超时不会放行新实例
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
	// 锁内禁止新增激活后才能 Wait
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
		// 锁顺序固定为 System -> runner
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

	lease, owner, acquired, acquireErr := s.guard.TryAcquire(r.ctx, r.key, r.activationId)
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
	err = r.run(definition)
}
