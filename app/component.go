package app

import "context"

type Component interface {
	// Start 失败时组件自行清理这次启动申请的资源
	Start() error
	RequestStop()
	// Shutdown 等待组件退出，调用超时后可再次等待
	Shutdown(context.Context) error
}

type CommonComponent struct {
	MStart       func() error
	MRequestStop func()
	MShutdown    func(context.Context) error
}

func (c *CommonComponent) Start() error {
	if c.MStart != nil {
		return c.MStart()
	}
	return nil
}

func (c *CommonComponent) RequestStop() {
	if c.MRequestStop != nil {
		c.MRequestStop()
	}
}

func (c *CommonComponent) Shutdown(ctx context.Context) error {
	if c.MShutdown != nil {
		return c.MShutdown(ctx)
	}
	return nil
}
