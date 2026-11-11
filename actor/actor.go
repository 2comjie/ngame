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
}

// 当前激活实例的句柄，由框架创建，可以保存。
// 同一个 Key 重新激活后，会获得新的 Handle。
type Handle interface {
	Key() Key
	ActivationId() string

	// 请求停止，不等待当前回调结束。
	Stop()

	// 投递到自己的执行队列，不等待执行完成。
	RunOnMainLoop(task Task) error

	// 定时任务最终也在自己的主循环执行。
	After(delay time.Duration, task Task) (Timer, error)
	Every(interval time.Duration, task Task) (Timer, error)
}

// ctx 是任务执行时的上下文。
// 回调在 Actor 主循环中执行，可以直接操作 Actor 状态。
type Task func(ctx context.Context)

// 定时任务句柄，可以保存、主动取消。
type Timer interface {
	// 幂等，不等待正在执行的回调结束。
	Cancel()
}

type StartContext struct {
	context.Context
	Self Handle
}

type UpdateContext struct {
	context.Context
	Self  Handle
	Delta time.Duration // 距离上次 Update 的时间。
	Idle  time.Duration // 距离上次业务消息执行结束的时间。
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
