package ezghflags

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// testFlag is a boolean flag in a test snapshot, serving value statically.
type testFlag struct {
	key         string
	value       bool
	flagVersion int64
}

// snapshotJSON is a compact compiled snapshot (flagd JSON with EZGH's metadata, plan PD13).
func snapshotJSON(version int64, flags ...testFlag) []byte {
	fs := map[string]any{}
	for _, f := range flags {
		fs[f.key] = map[string]any{
			"state":          "ENABLED",
			"variants":       map[string]any{"false": false, "true": true},
			"defaultVariant": strconv.FormatBool(f.value),
			"metadata":       map[string]any{"flagVersion": f.flagVersion, "enabled": true},
		}
	}
	b, err := json.Marshal(map[string]any{
		"$schema":  "https://flagd.dev/schema/v0/flags.json",
		"metadata": map[string]any{"environmentId": "0199a3c2-8d31-7f02-a4b5-6c7d8e9f0a1b", "snapshotVersion": version, "compiledAt": "2026-10-12T09:00:00Z"},
		"flags":    fs,
	})
	if err != nil {
		panic(err)
	}
	return b
}

// recorded is one request the fake data plane received.
type recorded struct {
	method, path, auth, ifNoneMatch, body string
	at                                    time.Time
}

// failure is a canned error answer.
type failure struct {
	status     int
	retryAfter string
}

// fakeStream is one open /v1/sync/stream.
type fakeStream struct {
	frames chan string
	drop   chan struct{}
}

// fakeDataPlane is an httptest server with flags-data's server-key surface: GET /v1/sync/snapshot
// (ETag, If-None-Match → 304) and POST /v1/sync/stream (SSE: ": open", a snapshot event unless the
// body names the current version, then pushed snapshots and end events).
type fakeDataPlane struct {
	t   *testing.T
	srv *httptest.Server
	key string

	mu       sync.Mutex
	snapshot []byte
	version  int64
	fail     map[string]failure // by path; absent answers normally
	failN    map[string]int     // how many more requests fail (0: until cleared)
	streams  map[*fakeStream]struct{}
	requests []recorded
	notify   chan struct{}
	// quirk misbehaves on /v1/sync/stream: "silent" answers 200 text/event-stream and closes
	// without a byte; "json" answers 200 with Content-Type application/json.
	quirk string
}

const (
	snapshotPath = "/v1/sync/snapshot"
	streamPath   = "/v1/sync/stream"
)

func newFakeDataPlane(t *testing.T, key string, snapshot []byte, version int64) *fakeDataPlane {
	t.Helper()
	f := &fakeDataPlane{
		t: t, key: key, snapshot: snapshot, version: version,
		fail: map[string]failure{}, failN: map[string]int{},
		streams: map[*fakeStream]struct{}{}, notify: make(chan struct{}),
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(func() {
		f.dropStreams()
		f.srv.Close()
	})
	return f
}

func (f *fakeDataPlane) url() string { return f.srv.URL }

// setSnapshot replaces the current snapshot and pushes it to every open stream.
func (f *fakeDataPlane) setSnapshot(snapshot []byte, version int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snapshot, f.version = snapshot, version
	for s := range f.streams {
		s.frames <- snapshotFrame(snapshot, version)
	}
}

// endStreams ends every open stream with an end event naming reason.
func (f *fakeDataPlane) endStreams(reason string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for s := range f.streams {
		s.frames <- fmt.Sprintf("event: end\ndata: {\"reason\":%q}\n\n", reason)
		delete(f.streams, s)
	}
}

// dropStreams closes every open stream without an end event.
func (f *fakeDataPlane) dropStreams() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for s := range f.streams {
		close(s.drop)
		delete(f.streams, s)
	}
}

// failPath answers status (with Retry-After when set) on path: for the next n requests, or until
// clearFailures when n is 0.
func (f *fakeDataPlane) failPath(path string, status int, retryAfter string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail[path] = failure{status: status, retryAfter: retryAfter}
	f.failN[path] = n
}

func (f *fakeDataPlane) clearFailures() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = map[string]failure{}
	f.failN = map[string]int{}
}

// requestsTo answers the requests made to path so far.
func (f *fakeDataPlane) requestsTo(path string) []recorded {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recorded
	for _, r := range f.requests {
		if r.path == path {
			out = append(out, r)
		}
	}
	return out
}

func (f *fakeDataPlane) openStreams() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.streams)
}

// waitFor waits until cond holds, re-checking on every request and every 5 ms.
func (f *fakeDataPlane) waitFor(what string, timeout time.Duration, cond func() bool) {
	f.t.Helper()
	deadline := time.After(timeout)
	for !cond() {
		f.mu.Lock()
		ch := f.notify
		f.mu.Unlock()
		select {
		case <-ch:
		case <-time.After(5 * time.Millisecond):
		case <-deadline:
			f.t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func snapshotFrame(snapshot []byte, version int64) string {
	return fmt.Sprintf("event: snapshot\nid: %d\ndata: %s\n\n", version, snapshot)
}

func (f *fakeDataPlane) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	f.mu.Lock()
	f.requests = append(f.requests, recorded{
		method: r.Method, path: r.URL.Path, auth: r.Header.Get("Authorization"),
		ifNoneMatch: r.Header.Get("If-None-Match"), body: string(body), at: time.Now(),
	})
	close(f.notify)
	f.notify = make(chan struct{})
	fl, failing := f.fail[r.URL.Path]
	if failing {
		if n := f.failN[r.URL.Path]; n > 0 {
			if n == 1 {
				delete(f.fail, r.URL.Path)
				delete(f.failN, r.URL.Path)
			} else {
				f.failN[r.URL.Path] = n - 1
			}
		}
	}
	snapshot, version := f.snapshot, f.version
	f.mu.Unlock()

	if r.Header.Get("Authorization") != "Bearer "+f.key {
		writeError(w, http.StatusUnauthorized, "")
		return
	}
	if failing {
		writeError(w, fl.status, fl.retryAfter)
		return
	}
	etag := fmt.Sprintf("%q", strconv.FormatInt(version, 10))
	switch {
	case r.Method == http.MethodGet && r.URL.Path == snapshotPath:
		w.Header().Set("ETag", etag)
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(snapshot)
	case r.Method == http.MethodPost && r.URL.Path == streamPath:
		var req struct {
			Version int64 `json:"version"`
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(w, http.StatusBadRequest, "")
				return
			}
		}
		f.stream(w, r, req.Version)
	default:
		writeError(w, http.StatusNotFound, "")
	}
}

func (f *fakeDataPlane) setQuirk(q string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.quirk = q
}

func (f *fakeDataPlane) stream(w http.ResponseWriter, r *http.Request, known int64) {
	f.mu.Lock()
	quirk := f.quirk
	f.mu.Unlock()
	switch quirk {
	case "silent":
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		return
	case "json":
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, ": open\n\n")
		return
	}
	s := &fakeStream{frames: make(chan string, 64), drop: make(chan struct{})}
	f.mu.Lock()
	f.streams[s] = struct{}{}
	if known != f.version {
		s.frames <- snapshotFrame(f.snapshot, f.version)
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		delete(f.streams, s)
		f.mu.Unlock()
	}()
	w.Header().Set("Content-Type", "text/event-stream; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	write := func(frame string) bool {
		if _, err := io.WriteString(w, frame); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !write(": open\n\n") {
		return
	}
	for {
		select {
		case frame := <-s.frames:
			if !write(frame) {
				return
			}
			if len(frame) > 10 && frame[:10] == "event: end" {
				return
			}
		case <-s.drop:
			return
		case <-r.Context().Done():
			return
		}
	}
}

func writeError(w http.ResponseWriter, status int, retryAfter string) {
	if retryAfter != "" {
		w.Header().Set("Retry-After", retryAfter)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"error":{"code":"status_%d","message":"fake"}}`, status)
}
