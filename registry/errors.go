package registry

import "errors"

var (
	ErrAlreadyRegistered  = errors.New("registry: 实例 Id 已被注册")
	ErrRegistrationClosed = errors.New("registry: 注册已结束")
	ErrIdentityChanged    = errors.New("registry: 不可更改实例 Id 或服务名")
	ErrDiscoveryClosed    = errors.New("registry: 服务发现已关闭")
)
