package ezghflags

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/open-feature/go-sdk/openfeature"
)

// vector is a flags-stack protocol/vectors/eval file (flags-stack protocol/README.md, "Vector
// format"), synced verbatim into testdata/vectors by scripts/sync-vectors.sh.
type vector struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Snapshot    json.RawMessage `json:"snapshot"`
	Cases       []vectorCase    `json:"cases"`
}

type vectorCase struct {
	Flag    string         `json:"flag"`
	Type    string         `json:"type"`
	Default any            `json:"default"`
	Context map[string]any `json:"context"`
	Expect  struct {
		Value       any    `json:"value"`
		Variant     string `json:"variant"`
		Reason      string `json:"reason"`
		FlagVersion *int64 `json:"flagVersion"`
		ErrorCode   string `json:"errorCode"`
	} `json:"expect"`
}

// result is what the provider answered, in the vectors' five fields.
type result struct {
	value     any
	variant   string
	reason    string
	meta      openfeature.FlagMetadata
	errorCode string
}

func resultOf[T any](r openfeature.GenericResolutionDetail[T]) result {
	d := r.ResolutionDetail()
	return result{value: r.Value, variant: r.Variant, reason: string(r.Reason), meta: r.FlagMetadata, errorCode: string(d.ErrorCode)}
}

// evaluateCase calls the provider's typed method for the case's type, as an application would
// through OpenFeature: number is FloatEvaluation.
func evaluateCase(t *testing.T, p *Provider, c vectorCase) result {
	t.Helper()
	ctx := context.Background()
	fc := openfeature.FlattenedContext(c.Context)
	switch c.Type {
	case "boolean":
		return resultOf(p.BooleanEvaluation(ctx, c.Flag, c.Default.(bool), fc))
	case "string":
		return resultOf(p.StringEvaluation(ctx, c.Flag, c.Default.(string), fc))
	case "number":
		return resultOf(p.FloatEvaluation(ctx, c.Flag, c.Default.(float64), fc))
	case "object":
		return resultOf(p.ObjectEvaluation(ctx, c.Flag, c.Default, fc))
	}
	t.Fatalf("unknown case type %q", c.Type)
	return result{}
}

// TestVectors evaluates every conformance vector through the provider (offline, bootstrapped
// from the vector's snapshot) and compares value, variant, reason, flagVersion and error code.
// A failure is a cross-core disagreement to report to flags-stack, never a vector to edit.
func TestVectors(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "vectors", "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no vectors in testdata/vectors (run scripts/sync-vectors.sh): %v", err)
	}
	total := 0
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var v vector
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&v); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		t.Run(v.Name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "snapshot.json")
			if err := os.WriteFile(path, v.Snapshot, 0o600); err != nil {
				t.Fatal(err)
			}
			p := New("", WithBootstrapFile(path), WithOffline())
			if err := p.InitWithContext(context.Background(), openfeature.EvaluationContext{}); err != nil {
				t.Fatalf("init: %v", err)
			}
			defer p.Shutdown()
			for i, c := range v.Cases {
				ctxJSON, _ := json.Marshal(c.Context)
				t.Run(fmt.Sprintf("%d %s %s", i, c.Flag, ctxJSON), func(t *testing.T) {
					r := evaluateCase(t, p, c)
					if !reflect.DeepEqual(r.value, c.Expect.Value) {
						t.Errorf("value = %#v, want %#v", r.value, c.Expect.Value)
					}
					if c.Expect.ErrorCode != "" {
						if r.errorCode != c.Expect.ErrorCode {
							t.Errorf("error code = %q, want %q", r.errorCode, c.Expect.ErrorCode)
						}
						return
					}
					if r.errorCode != "" {
						t.Errorf("unexpected error code %q", r.errorCode)
					}
					if r.variant != c.Expect.Variant {
						t.Errorf("variant = %q, want %q", r.variant, c.Expect.Variant)
					}
					if r.reason != c.Expect.Reason {
						t.Errorf("reason = %q, want %q", r.reason, c.Expect.Reason)
					}
					if c.Expect.FlagVersion == nil {
						t.Fatal("vector case has no flagVersion")
					}
					if got, ok := r.meta["flagVersion"].(int64); !ok || got != *c.Expect.FlagVersion {
						t.Errorf("flagVersion = %#v, want %d", r.meta["flagVersion"], *c.Expect.FlagVersion)
					}
				})
			}
		})
		total += len(v.Cases)
	}
	t.Logf("%d cases in %d files", total, len(files))
}
