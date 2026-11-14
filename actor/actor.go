package actor

import (
	"context"
	"time"
)

type Key struct {
	Type string
	Id   string
}

type Actor interface {
	OnStart(ctx StartContext) error
	// OnMessage 与 Update、定时任务串行执行
	OnMessage(ctx MessageContext, message Message) ([]byte, error)
	// OnUpdate 返回 0 使用默认更新间隔，返回负数停止自动 Update
	OnUpdate(ctx UpdateContext) time.Duration
	OnStop(ctx StopContext)
}

// Loader 加载已有业务实体，不创建数据
type Loader func(ctx context.Context, key Key) (Actor, error)

// Message.Body 入队时复制，调用后可复用原缓冲区
type Message struct {
	Method string
	Body   []byte
}

type MessageContext struct {
	context.Context
	Self Handle
}

// Handle 当前激活实例的本地句柄
// 重新激活会创建新 Handle
type Handle interface {
	Key() Key
	ActivationId() string

	// Stop 请求停止，不等待当前回调结束
	Stop()

	// RunOnMainLoop 投递闭包，不等待完成
	RunOnMainLoop(task Task) error

	// WaitResultOnMainLoop 投递闭包并等待结果，取消时跳过排队任务
	// 回调内传当前上下文，禁止同步等待自己
	WaitResultOnMainLoop(ctx context.Context, task func(context.Context) error) error

	// After 定时任务最终也在自己的主循环执行
	After(delay time.Duration, task Task) (Timer, error)
	Every(interval time.Duration, task Task) (Timer, error)
}

// Task 在主循环执行，可直接操作 Actor 状态
type Task func(ctx context.Context)

// Timer 可主动取消的定时任务句柄
type Timer interface {
	// Cancel 幂等，不等待正在执行的回调结束
	Cancel()
}

type StartContext struct {
	context.Context
	Self Handle
}

type UpdateContext struct {
	context.Context
	Self  Handle
	Delta time.Duration // 距离上次 Update 的时间
	Idle  time.Duration // 距离上次业务消息执行结束的时间
}

type StopContext struct {
	context.Context
	Self   Handle
	Reason StopReason
}

type StopReason uint8

const (
	StopReasonSelf      StopReason = 1
	StopReasonShutdown  StopReason = 2
	StopReasonLeaseLost StopReason = 3
)
