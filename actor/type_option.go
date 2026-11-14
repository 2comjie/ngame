package actor

import "context"

type actorType struct {
	loader     Loader
	onNotFound func(context.Context, Key) error
}

type TypeOption func(*actorType)

// WithOnNotFound 仅在 Loader 返回 ErrEntityNotFound 时调用，成功后重新加载一次。
func WithOnNotFound(handler func(context.Context, Key) error) TypeOption {
	return func(o *actorType) { o.onNotFound = handler }
}
