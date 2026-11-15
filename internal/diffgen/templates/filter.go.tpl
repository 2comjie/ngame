{{define "filter"}}
{{$typeName := .Name}}
// Filter{{.Name}} 按分类复制全量或增量 Proto，不修改输入，也不保留未知字段。
// 父字段不可见时整条子树不可见；嵌套更新过滤为空时不再输出该更新。
// 无可见数据时返回空 Proto，输入为 nil 时返回 nil。
func Filter{{.Name}}(data *pbData.{{.Name}}, scope diff.Scope) *pbData.{{.Name}} {
    if data == nil {
        return nil
    }
    filtered := new(pbData.{{.Name}})
{{range .Fields}}
    if scope & {{.Scope}} != 0 {
{{- if eq .Kind "primitive"}}
        filtered.{{.ProtoGoName}} = data.{{.ProtoGoName}}
        if data.{{.ProtoGoName}}Update != nil {
            fieldValue := *data.{{.ProtoGoName}}Update
            filtered.{{.ProtoGoName}}Update = &fieldValue
        }
{{- else if eq .Kind "pointer"}}
        filtered.{{.ProtoGoName}} = {{.FilterFunc}}(data.{{.ProtoGoName}}, scope)
        switch change := data.{{.ProtoGoName}}Diff.(type) {
        case *pbData.{{$typeName}}_{{.ProtoGoName}}Set:
            filtered.{{.ProtoGoName}}Diff = &pbData.{{$typeName}}_{{.ProtoGoName}}Set{
                {{.ProtoGoName}}Set: {{.FilterFunc}}(change.{{.ProtoGoName}}Set, scope),
            }
        case *pbData.{{$typeName}}_{{.ProtoGoName}}Clear:
            filtered.{{.ProtoGoName}}Diff = &pbData.{{$typeName}}_{{.ProtoGoName}}Clear{
                {{.ProtoGoName}}Clear: change.{{.ProtoGoName}}Clear,
            }
        case *pbData.{{$typeName}}_{{.ProtoGoName}}Update:
            childUpdate := {{.FilterFunc}}(change.{{.ProtoGoName}}Update, scope)
            if childUpdate != nil && proto.Size(childUpdate) != 0 {
                filtered.{{.ProtoGoName}}Diff = &pbData.{{$typeName}}_{{.ProtoGoName}}Update{
                    {{.ProtoGoName}}Update: childUpdate,
                }
            }
        }
{{- else if eq .Kind "primitiveMap"}}
        filtered.{{.ProtoGoName}} = maps.Clone(data.{{.ProtoGoName}})
        filtered.{{.ProtoGoName}}Clear = data.{{.ProtoGoName}}Clear
        filtered.{{.ProtoGoName}}Set = maps.Clone(data.{{.ProtoGoName}}Set)
        filtered.{{.ProtoGoName}}Delete = slices.Clone(data.{{.ProtoGoName}}Delete)
{{- else if eq .Kind "pointerMap"}}
        if data.{{.ProtoGoName}} != nil {
            filtered.{{.ProtoGoName}} = make(map[{{.ProtoKeyType}}]{{.ProtoGoType}}, len(data.{{.ProtoGoName}}))
            for key, child := range data.{{.ProtoGoName}} {
                filtered.{{.ProtoGoName}}[key] = {{.FilterFunc}}(child, scope)
            }
        }
        filtered.{{.ProtoGoName}}Clear = data.{{.ProtoGoName}}Clear
        if data.{{.ProtoGoName}}Set != nil {
            filtered.{{.ProtoGoName}}Set = make(map[{{.ProtoKeyType}}]{{.ProtoGoType}}, len(data.{{.ProtoGoName}}Set))
            for key, child := range data.{{.ProtoGoName}}Set {
                filtered.{{.ProtoGoName}}Set[key] = {{.FilterFunc}}(child, scope)
            }
        }
        filtered.{{.ProtoGoName}}Delete = slices.Clone(data.{{.ProtoGoName}}Delete)
        for key, child := range data.{{.ProtoGoName}}Update {
            childUpdate := {{.FilterFunc}}(child, scope)
            if childUpdate != nil && proto.Size(childUpdate) != 0 {
                diff.SetMap(&filtered.{{.ProtoGoName}}Update, key, childUpdate)
            }
        }
{{- else if eq .Kind "primitiveSlice"}}
        filtered.{{.ProtoGoName}} = slices.Clone(data.{{.ProtoGoName}})
        filtered.{{.ProtoGoName}}Updated = data.{{.ProtoGoName}}Updated
        filtered.{{.ProtoGoName}}Update = slices.Clone(data.{{.ProtoGoName}}Update)
{{- else}}
        if data.{{.ProtoGoName}} != nil {
            filtered.{{.ProtoGoName}} = make([]{{.ProtoGoType}}, len(data.{{.ProtoGoName}}))
            for index, child := range data.{{.ProtoGoName}} {
                filtered.{{.ProtoGoName}}[index] = {{.FilterFunc}}(child, scope)
            }
        }
        filtered.{{.ProtoGoName}}Updated = data.{{.ProtoGoName}}Updated
        if data.{{.ProtoGoName}}Update != nil {
            filtered.{{.ProtoGoName}}Update = make([]{{.ProtoGoType}}, len(data.{{.ProtoGoName}}Update))
            for index, child := range data.{{.ProtoGoName}}Update {
                filtered.{{.ProtoGoName}}Update[index] = {{.FilterFunc}}(child, scope)
            }
        }
{{- end}}
    }
{{end}}
    return filtered
}
{{end}}
