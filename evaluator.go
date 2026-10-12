package ezghflags

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	"github.com/open-feature/flagd/core/pkg/evaluator"
	"github.com/open-feature/flagd/core/pkg/logger"
	"github.com/open-feature/flagd/core/pkg/model"
	"github.com/open-feature/flagd/core/pkg/store"
	fsync "github.com/open-feature/flagd/core/pkg/sync"
	"go.uber.org/zap"
)

// This file follows flags-stack's internal/eval (the data plane's evaluator) so the provider and
// flags-data answer alike; the conformance vectors pin both. It's a copy, not an import:
// internal/eval is internal to a private repository.

// source names the snapshot's flags in flagd's store; every evaluator has its own store.
const source = "ezgh"

type flagMeta struct {
	version int64
	enabled bool
}

// snapshot is one loaded snapshot (the flagd core's evaluator over its own store). It is
// immutable once loaded and safe for concurrent use.
type snapshot struct {
	ev      *evaluator.JSON
	version int64
	flags   map[string]flagMeta
}

// anyResult is one flag's evaluation before typing. On an error, value and variant are empty and
// errorCode is FLAG_NOT_FOUND, PARSE_ERROR, INVALID_CONTEXT or GENERAL.
type anyResult struct {
	value       any
	variant     string
	reason      string
	flagVersion int64
	errorCode   string
}

// quiet is flagd's logger with nothing logged: evaluation contexts are never logged, and
// evaluation errors are answered, not logged per call.
var quiet = logger.NewLogger(zap.NewNop(), false)

// loadSnapshot parses a compiled snapshot (flagd JSON with EZGH's metadata). Invalid JSON, a
// document flagd rejects, or a flag whose defaultVariant isn't one of its variants is an error.
func loadSnapshot(data []byte) (*snapshot, error) {
	var head struct {
		Metadata struct {
			SnapshotVersion int64 `json:"snapshotVersion"`
		} `json:"metadata"`
		Flags map[string]struct {
			Metadata struct {
				FlagVersion int64 `json:"flagVersion"`
				Enabled     *bool `json:"enabled"`
			} `json:"metadata"`
			DefaultVariant *string                    `json:"defaultVariant"`
			Variants       map[string]json.RawMessage `json:"variants"`
		} `json:"flags"`
	}
	if err := json.Unmarshal(data, &head); err != nil {
		return nil, fmt.Errorf("ezghflags: snapshot isn't JSON: %w", err)
	}
	if head.Flags == nil {
		return nil, errors.New("ezghflags: snapshot has no flags object")
	}
	for k, f := range head.Flags {
		if f.DefaultVariant == nil {
			return nil, fmt.Errorf("ezghflags: flag %q has no defaultVariant", k)
		}
		if _, ok := f.Variants[*f.DefaultVariant]; !ok {
			return nil, fmt.Errorf("ezghflags: flag %q: defaultVariant %q isn't a variant", k, *f.DefaultVariant)
		}
	}
	st, err := store.NewStore(quiet, []string{source})
	if err != nil {
		return nil, err
	}
	ev := evaluator.NewJSON(quiet, st)
	if err := ev.SetState(fsync.DataSync{FlagData: string(data), Source: source}); err != nil {
		return nil, fmt.Errorf("ezghflags: flagd rejected the snapshot: %w", err)
	}
	s := &snapshot{ev: ev, version: head.Metadata.SnapshotVersion, flags: map[string]flagMeta{}}
	for k, f := range head.Flags {
		s.flags[k] = flagMeta{version: f.Metadata.FlagVersion, enabled: f.Metadata.Enabled == nil || *f.Metadata.Enabled}
	}
	return s, nil
}

// evaluate evaluates one flag for evalCtx, which it never stores or logs. An off config
// (metadata enabled: false) answers its off value with reason DISABLED (plan PD7); otherwise the
// reason is the core's (STATIC, DEFAULT, TARGETING_MATCH).
func (s *snapshot) evaluate(ctx context.Context, key string, evalCtx map[string]any) anyResult {
	m, ok := s.flags[key]
	if !ok {
		return anyResult{reason: model.ErrorReason, errorCode: model.FlagNotFoundErrorCode}
	}
	if evalCtx == nil {
		evalCtx = map[string]any{}
	}
	r := s.ev.ResolveAsAnyValue(ctx, "", key, evalCtx)
	if r.Error != nil {
		code := r.Error.Error()
		switch code {
		case model.FlagNotFoundErrorCode, model.ParseErrorCode, model.InvalidContextCode:
		default:
			code = model.GeneralErrorCode
		}
		return anyResult{reason: model.ErrorReason, errorCode: code}
	}
	reason := r.Reason
	if !m.enabled {
		reason = model.DisabledReason
	}
	return anyResult{value: r.Value, variant: r.Variant, reason: reason, flagVersion: m.version}
}

// flagVersions maps each flag key to its config version (metadata.flagVersion).
func (s *snapshot) flagVersions() map[string]int64 {
	out := make(map[string]int64, len(s.flags))
	for k, m := range s.flags {
		out[k] = m.version
	}
	return out
}

// changedKeys are the keys whose flagVersion differs between before and after, or that are in
// only one of them, sorted.
func changedKeys(before, after map[string]int64) []string {
	var out []string
	for k, v := range after {
		if old, ok := before[k]; !ok || old != v {
			out = append(out, k)
		}
	}
	for k := range before {
		if _, ok := after[k]; !ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}
