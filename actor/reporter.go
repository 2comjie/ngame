package actor

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/2comjie/ngame/app"
	"github.com/2comjie/ntool/logx"
	"github.com/shirou/gopsutil/v4/process"
)

const NodeLoadMetaKey = "ngame.actor.load"

// NodeLoad 的 CPU、内存使用率以配置的准入额度为分母
type NodeLoad struct {
	ActorCount  int       `json:"actor_count"`
	ActorLimit  int       `json:"actor_limit"`
	CPUUsage    float64   `json:"cpu_usage"`
	MemoryUsage float64   `json:"memory_usage"`
	SampledAt   time.Time `json:"sampled_at"`
}

type reporterOptions struct {
	cpuCapacity    float64
	memoryCapacity uint64
	interval       time.Duration
	timeout        time.Duration
	maxAge         time.Duration
}

type ReporterOption func(*reporterOptions)

// WithResourceCapacity 设置新激活的 CPU 核数和内存字节准入额度
func WithResourceCapacity(cpuCores float64, memoryBytes uint64) ReporterOption {
	return func(o *reporterOptions) {
		o.cpuCapacity = cpuCores
		o.memoryCapacity = memoryBytes
	}
}

// WithLoadInterval 设置负载上报间隔，默认 2 秒
func WithLoadInterval(interval time.Duration) ReporterOption {
	return func(o *reporterOptions) { o.interval = interval }
}

// WithReportTimeout 设置单次采样和上报超时，默认 5 秒
func WithReportTimeout(timeout time.Duration) ReporterOption {
	return func(o *reporterOptions) { o.timeout = timeout }
}

// WithResourceMaxAge 设置本地资源采样有效期，默认 10 秒
func WithResourceMaxAge(age time.Duration) ReporterOption {
	return func(o *reporterOptions) { o.maxAge = age }
}

// Reporter 独立采样和上报，生命周期由应用管理
type Reporter struct {
	system     *System
	report     func(context.Context, map[string]string) error
	options    reporterOptions
	mutex      sync.Mutex
	started    bool
	stopping   atomic.Bool
	ready      atomic.Bool
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	process    *process.Process
	lastCPU    float64
	lastSample time.Time
	cpuUsage   float64
	sampled    bool
	latest     atomic.Pointer[NodeLoad]
}

var _ app.Component = (*Reporter)(nil)

// NewReporter 绑定本地资源准入，必须在 System.Start 前创建
func NewReporter(system *System, report func(context.Context, map[string]string) error, opts ...ReporterOption) *Reporter {
	if system == nil || report == nil {
		panic("actor: Reporter 必须提供 System 和上报入口")
	}
	o := reporterOptions{
		cpuCapacity: float64(runtime.GOMAXPROCS(0)),
		interval:    2 * time.Second,
		timeout:     5 * time.Second,
		maxAge:      10 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.cpuCapacity <= 0 || math.IsNaN(o.cpuCapacity) || math.IsInf(o.cpuCapacity, 0) || o.memoryCapacity == 0 || o.interval <= 0 || o.timeout <= 0 || o.maxAge <= 0 {
		panic("actor: Reporter 的资源额度、采样间隔、超时和有效期必须为正数")
	}
	system.mutex.Lock()
	defer system.mutex.Unlock()
	if system.started || system.stopping || system.admission != nil {
		panic("actor: 每个 System 只能在启动前绑定一个 Reporter")
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &Reporter{
		system:  system,
		report:  report,
		options: o,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
	}
	system.admission = r.admit
	return r
}

func (r *Reporter) admit() bool {
	if r.stopping.Load() || !r.ready.Load() {
		return false
	}
	load := r.latest.Load()
	return load != nil && time.Since(load.SampledAt) <= r.options.maxAge && load.CPUUsage < 1 && load.MemoryUsage < 1
}

func (r *Reporter) Start() error {
	r.mutex.Lock()
	if r.stopping.Load() {
		r.mutex.Unlock()
		return app.ErrClosed
	}
	if r.started {
		r.mutex.Unlock()
		return app.ErrStarted
	}
	r.started = true
	r.mutex.Unlock()
	p, err := process.NewProcessWithContext(r.ctx, int32(os.Getpid()))
	if err != nil {
		r.RequestStop()
		close(r.done)
		return err
	}
	r.process = p
	if err := r.publish(); err != nil {
		r.RequestStop()
		close(r.done)
		return err
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.stopping.Load() {
		close(r.done)
		return app.ErrClosed
	}
	r.ready.Store(true)
	go r.loop()
	return nil
}

func (r *Reporter) publish() error {
	ctx, cancel := context.WithTimeout(r.ctx, r.options.timeout)
	defer cancel()
	times, err := r.process.TimesWithContext(ctx)
	if err != nil {
		r.latest.Store(nil)
		return err
	}
	memory, err := r.process.MemoryInfoWithContext(ctx)
	if err != nil {
		r.latest.Store(nil)
		return err
	}
	// 部分平台读取失败时返回零值而不报错
	if memory.RSS == 0 {
		r.latest.Store(nil)
		return errors.New("actor: 进程内存采样无效")
	}
	now := time.Now()
	cpuTime := times.User + times.System
	var usage float64
	if r.sampled {
		current := max(0, (cpuTime-r.lastCPU)/now.Sub(r.lastSample).Seconds()/r.options.cpuCapacity)
		usage = 0.5*r.cpuUsage + 0.5*current
	} else {
		// 首次用进程平均 CPU，避免极短采样窗口放大误差
		percent, err := r.process.CPUPercentWithContext(ctx)
		if err != nil {
			r.latest.Store(nil)
			return err
		}
		usage = percent / 100 / r.options.cpuCapacity
	}
	stats := r.system.Stats()
	load := NodeLoad{
		ActorCount:  stats.ActorCount,
		ActorLimit:  stats.ActorLimit,
		CPUUsage:    usage,
		MemoryUsage: float64(memory.RSS) / float64(r.options.memoryCapacity),
		SampledAt:   now.UTC(),
	}
	value, err := json.Marshal(load)
	if err != nil {
		r.latest.Store(nil)
		return err
	}
	r.lastCPU = cpuTime
	r.lastSample = now
	r.cpuUsage = usage
	r.sampled = true
	r.latest.Store(&load)
	return r.report(ctx, map[string]string{NodeLoadMetaKey: string(value)})
}

func (r *Reporter) loop() {
	defer close(r.done)
	ticker := time.NewTicker(r.options.interval)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if err := r.publish(); err != nil && !r.stopping.Load() {
				logx.Errorf("actor: 上报负载失败 node=%s error=%v", r.system.guard.nodeId, err)
			}
		}
	}
}

func (r *Reporter) RequestStop() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if r.stopping.Swap(true) {
		return
	}
	r.cancel()
	if !r.started {
		close(r.done)
	}
}

func (r *Reporter) Shutdown(ctx context.Context) error {
	r.RequestStop()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
