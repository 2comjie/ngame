package diffgen

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestScopeFiltering(t *testing.T) {
	protoc, err := exec.LookPath("protoc")
	if err != nil {
		t.Skip("scope integration test requires protoc")
	}
	dir := t.TempDir()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("GOWORK", "off")
	t.Setenv("GOFLAGS", "-mod=mod")
	if err := os.CopyFS(dir, os.DirFS("testdata/scopes")); err != nil {
		t.Fatal(err)
	}
	module := fmt.Sprintf("module example.com/scopes\n\ngo 1.27.3\n\nrequire github.com/2comjie/ngame v0.0.0\nreplace github.com/2comjie/ngame => %q\n", root)
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(module), 0644); err != nil {
		t.Fatal(err)
	}
	if err := Generate(filepath.Join(dir, "diffgen.yaml")); err != nil {
		t.Fatal(err)
	}
	plugin := filepath.Join(dir, "protoc-gen-go")
	build := exec.Command("go", "build", "-o", plugin, "google.golang.org/protobuf/cmd/protoc-gen-go")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build protoc-gen-go: %v\n%s", err, output)
	}
	compile := exec.Command(protoc, "-I", "proto", "--plugin=protoc-gen-go="+plugin, "--go_out=.", "--go_opt=module=example.com/scopes", "proto/model/data.proto", "proto/child/data.proto")
	compile.Dir = dir
	if output, err := compile.CombinedOutput(); err != nil {
		t.Fatalf("compile proto: %v\n%s", err, output)
	}
	verify := exec.Command("go", "test", "-v", "./model")
	verify.Dir = dir
	if output, err := verify.CombinedOutput(); err != nil {
		t.Fatalf("verify generated filters: %v\n%s", err, output)
	}
}
