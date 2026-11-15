//go:build diff_fast

package model

import "example.com/scopes/child"

type Box[T any] struct {
	Value T `diff:"1" sync:"public"`
}

type ChildBox Box[*child.Data]
type NumberBox Box[int64]

type Player struct {
	Id          int64                 `diff:"1" sync:"public"`
	Secret      int64                 `diff:"2" sync:"private"`
	Audit       int64                 `diff:"3" sync:"server"`
	Default     int64                 `diff:"4"`
	Child       *child.Data           `diff:"5" sync:"public"`
	Replacement *child.Data           `diff:"6" sync:"public"`
	Items       map[int64]*child.Data `diff:"7" sync:"public"`
	History     []*child.Data         `diff:"8" sync:"public"`
	Stats       map[int64]int64       `diff:"9" sync:"public"`
	Numbers     []int64               `diff:"10" sync:"public"`
	PrivateData *child.Data           `diff:"11" sync:"private"`
	ServerData  *child.Data           `diff:"12" sync:"server"`
	Box         *ChildBox             `diff:"13" sync:"public"`
	NumberBox   *NumberBox            `diff:"14" sync:"public"`
	PrivateMap  map[int64]int64       `diff:"15" sync:"private"`
	ServerList  []int64               `diff:"16" sync:"server"`
}
