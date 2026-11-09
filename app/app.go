package app

import (
	"context"
	"errors"

	"github.com/2comjie/ntool/logx"
)

var (
	ErrStarted = errors.New("app: 应用已经启动")
	ErrClosed  = errors.New("app: 应用已经停止")
)

type App struct {
	components []Component
	started    int
	running    bool
	stopping   bool
}

var _ Component = (*App)(nil)

func New(components ...Component) *App {
	a := &App{}
	for _, component := range components {
		a.AddComponent(component)
	}
	return a
}

func (a *App) AddComponent(component Component) {
	if component == nil {
		panic("app: 必须提供 Component")
	}
	if a.running || a.stopping {
		panic("app: 组件必须在启动前添加")
	}
	a.components = append(a.components, component)
}

func (a *App) GetComponent[T Component]() (T, bool) {
	for _, component := range a.components {
		if value, ok := component.(T); ok {
			return value, true
		}
	}
	var zero T
	return zero, false
}

func (a *App) Start() error {
	if a.stopping {
		return ErrClosed
	}
	if a.running {
		return ErrStarted
	}
	a.running = true
	for _, component := range a.components {
		if err := component.Start(); err != nil {
			if cleanupErr := a.Shutdown(context.Background()); cleanupErr != nil {
				logx.Errorf("app: 启动失败后清理组件失败 error=%v", cleanupErr)
			}
			return err
		}
		a.started++
	}
	return nil
}

func (a *App) RequestStop() {
	if a.stopping {
		return
	}
	a.stopping = true
	for index := a.started - 1; index >= 0; index-- {
		a.components[index].RequestStop()
	}
}

func (a *App) Shutdown(ctx context.Context) error {
	a.RequestStop()
	var shutdownErr error
	for a.started > 0 {
		a.started--
		// 一个组件释放失败也继续释放剩余组件，错误仅在本次调用链聚合返回。
		shutdownErr = errors.Join(shutdownErr, a.components[a.started].Shutdown(ctx))
	}
	return shutdownErr
}
