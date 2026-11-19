package app

import (
	"context"
	"errors"

	"github.com/2comjie/nbean"
)

var (
	ErrStarted = errors.New("app: 应用已经启动")
	ErrClosed  = errors.New("app: 应用已经停止")
)

// App 按添加顺序管理组件，生命周期方法由调用方串行调用
type App struct {
	beans             *nbean.Container
	components        []Component
	started           int
	running           bool
	stopping          bool
	stoppingComponent bool
}

var _ Component = (*App)(nil)

func New(components ...Component) *App {
	a := &App{beans: nbean.New()}
	for _, component := range components {
		a.AddComponent(component)
	}
	return a
}

func (a *App) Beans() *nbean.Container {
	return a.beans
}

// AddComponent 先添加依赖，再添加使用它的组件
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
			if rollbackErr := a.Shutdown(context.Background()); rollbackErr != nil {
				return errors.Join(err, rollbackErr)
			}
			return err
		}
		a.started++
	}
	return nil
}

// RequestStop 标记应用停止，组件由 Shutdown 逐个关闭
func (a *App) RequestStop() { a.stopping = true }

func (a *App) Shutdown(ctx context.Context) error {
	a.RequestStop()
	var shutdownErr error
	for a.started > 0 {
		if err := ctx.Err(); err != nil {
			return errors.Join(shutdownErr, err)
		}
		component := a.components[a.started-1]
		if !a.stoppingComponent {
			component.RequestStop()
			a.stoppingComponent = true
		}
		if err := component.Shutdown(ctx); err != nil {
			shutdownErr = errors.Join(shutdownErr, err)
			if ctx.Err() != nil {
				return shutdownErr
			}
		}
		a.started--
		a.stoppingComponent = false
	}
	return shutdownErr
}
