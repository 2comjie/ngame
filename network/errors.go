package network

import "errors"

var (
	ErrClosed          = errors.New("network: 已关闭")
	ErrStarted         = errors.New("network: 已经启动")
	ErrAutherRequired  = errors.New("network: 必须提供Auther")
	ErrListenerMissing = errors.New("network: 必须提供Listener")
	ErrUnauthorized    = errors.New("network: 认证失败")
	ErrNotBound        = errors.New("network: Session尚未绑定")
	ErrResponseWritten = errors.New("network: 请求已经返回响应")
	ErrBodyTooLarge    = errors.New("network: Body超过上限")
)
