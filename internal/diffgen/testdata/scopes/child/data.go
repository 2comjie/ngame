//go:build diff_fast

package child

type Data struct {
	Public  int64 `diff:"1" sync:"public"`
	Private int64 `diff:"2" sync:"private"`
	Server  int64 `diff:"3" sync:"server"`
	Default int64 `diff:"4"`
}
