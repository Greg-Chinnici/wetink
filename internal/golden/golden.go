// Package golden loads testdata/golden: outputs of the original implementation that the Go
// code must reproduce exactly.
package golden

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Root is the repository root.
func Root() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// Load decodes testdata/golden/<name>.json into v.
func Load(t testing.TB, name string, v any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(Root(), "testdata", "golden", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatal(err)
	}
}
