package diff

// Scope 选择允许导出的字段分类；具体观察者的权限由调用方决定。
type Scope uint8

const (
	Public  Scope = 1
	Private Scope = 2
	Server  Scope = 4

	All Scope = Public | Private | Server
)
