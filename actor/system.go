package actor

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"sync"

	"github.com/2comjie/ngame/app"
)

var (
	ErrNotStarted        = errors.New("actor: System 尚未启动")
	ErrStopped           = errors.New("actor: System 已停止")
	ErrStopping          = errors.New("actor: Actor 正在停止")
	ErrNotFound          = errors.New("actor: Actor 不存在")
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
	mutex     sync.Mutex
	factories map[string]Factory
	actors    map[Key]*runner
	options   options
	ctx       context.Context
	cancel    context.CancelFunc
	started   bool
	stopping  bool
	wait      sync.WaitGroup
	done      chan struct{}
}

var _ app.Component = (*System)(nil)

func New(opts ...Option) *System {
	o := defaultOptions()
	for _, opt := range opts {
		opt(&o)
	}
	if o.mailboxSize < 1 || o.updateInterval <= 0 || o.lifecycleTimeout <= 0 {
		panic("actor: 邮箱容量、更新间隔和生命周期超时必须大于 0")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &System{
		factories: make(map[string]Factory),
		actors:    make(map[Key]*runner),
		options:   o,
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
	}
}

func (s *System) Register(actorType string, factory Factory) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if actorType == "" || factory == nil {
		panic("actor: 必须提供 Actor 类型和工厂")
	}
	if s.started || s.stopping {
		panic("actor: 工厂必须在 System.Start 前注册")
	}
	if _, exists := s.factories[actorType]; exists {
		panic("actor: Actor 类型重复注册: " + actorType)
	}
	s.factories[actorType] = factory
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

// Spawn 并发调用时共享一次创建。ctx 仅限制本次等待，不取消其他调用方共享的 OnStart。
// args 在新建时复制并交给 OnStart。已存在的 Actor 不重复初始化，后续 args 不会覆盖已有状态。
func (s *System) Spawn(ctx context.Context, key Key, args []byte) (Handle, error) {
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
		factory := s.factories[key.Type]
		if factory == nil {
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
		go s.run(r, factory, bytes.Clone(args))
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
	// stopping 与 wait.Add 由同一把锁保护，开始 Wait 后不会增加 Actor。
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

func (s *System) run(r *runner, factory Factory, args []byte) {
	defer s.wait.Done()
	defer close(r.done)
	defer r.cancel()
	value := factory(r.key)
	if value == nil {
		panic("actor: 工厂返回了空 Actor")
	}
	startCtx, cancel := context.WithTimeout(r.ctx, s.options.lifecycleTimeout)
	err := value.OnStart(StartContext{
		Context: startCtx,
		Self:    r,
		Args:    args,
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
	if err == nil && r.state != stateStarting {
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
	if r.reason == 0 {
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
		value.OnStop(StopContext{
			Context: stopCtx,
			Self:    r,
			Reason:  reason,
		})
		stop()
	}
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
}
