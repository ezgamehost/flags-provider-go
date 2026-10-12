package ezghflags

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/isolated"
)

const testKey = "ezgh_fs_0123456789abcdefghijABCDEFGHIJ0123456789a"

// fast shrinks the provider's timings for tests.
func fast(c *config) {
	c.minBackoff = 10 * time.Millisecond
	c.maxBackoff = 40 * time.Millisecond
	c.pollEvery = 20 * time.Millisecond
	c.staleAfter = 150 * time.Millisecond
	c.idleTimeout = 5 * time.Second
	c.initWait = 300 * time.Millisecond
	c.fetchTimeout = 5 * time.Second
	c.logger = slog.New(slog.DiscardHandler)
}

type gotEvent struct {
	typ openfeature.EventType
	openfeature.EventDetails
}

// eventLog records the provider's events as OpenFeature delivers them to handlers.
type eventLog struct {
	ch chan gotEvent
}

func watchEvents(api *openfeature.EvaluationAPI) *eventLog {
	l := &eventLog{ch: make(chan gotEvent, 256)}
	for _, typ := range []openfeature.EventType{openfeature.ProviderReady, openfeature.ProviderConfigChange, openfeature.ProviderStale, openfeature.ProviderError} {
		cb := func(d openfeature.EventDetails) {
			if d.ProviderName == Name {
				l.ch <- gotEvent{typ: typ, EventDetails: d}
			}
		}
		api.AddHandler(typ, &cb)
	}
	return l
}

// next waits for the next event of type typ, skipping others.
func (l *eventLog) next(t *testing.T, typ openfeature.EventType) gotEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case e := <-l.ch:
			if e.typ == typ {
				return e
			}
		case <-deadline:
			t.Fatalf("no %s event", typ)
		}
	}
}

// none checks that no event of type typ arrives within d.
func (l *eventLog) none(t *testing.T, typ openfeature.EventType, d time.Duration) {
	t.Helper()
	deadline := time.After(d)
	for {
		select {
		case e := <-l.ch:
			if e.typ == typ {
				t.Fatalf("unexpected %s event: %+v", typ, e)
			}
		case <-deadline:
			return
		}
	}
}

// setup starts a provider against fake through an isolated OpenFeature API and waits for it.
func setup(t *testing.T, fake *fakeDataPlane, key string, opts ...Option) (*Provider, *openfeature.Client, *eventLog, error) {
	t.Helper()
	api := isolated.NewAPI()
	events := watchEvents(api)
	p := New(key, append([]Option{WithEndpoint(fake.url() + "/"), fast}, opts...)...)
	t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := api.SetProviderAndWait(ctx, p)
	return p, api.NewClient(), events, err
}

func boolValue(t *testing.T, c *openfeature.Client, flag string) bool {
	t.Helper()
	v, err := c.BooleanValue(context.Background(), flag, false, openfeature.NewEvaluationContext("user-1", nil))
	if err != nil {
		t.Fatalf("evaluate %s: %v", flag, err)
	}
	return v
}

func waitStreams(t *testing.T, fake *fakeDataPlane, n int) {
	t.Helper()
	fake.waitFor("open streams", 5*time.Second, func() bool { return fake.openStreams() == n })
}

func TestReadyAfterInitialize(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"new-checkout", true, 3}), 1)
	_, client, events, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	events.next(t, openfeature.ProviderReady)
	events.none(t, openfeature.ProviderReady, 50*time.Millisecond)
	if client.State() != openfeature.ReadyState {
		t.Fatalf("state = %s", client.State())
	}
	snaps := fake.requestsTo(snapshotPath)
	if len(snaps) != 1 || snaps[0].method != http.MethodGet || snaps[0].auth != "Bearer "+testKey {
		t.Fatalf("snapshot requests = %+v", snaps)
	}
	waitStreams(t, fake, 1)
	streams := fake.requestsTo(streamPath)
	if len(streams) != 1 || streams[0].auth != "Bearer "+testKey || streams[0].body != `{"version":1}` {
		t.Fatalf("stream requests = %+v", streams)
	}

	d, err := client.BooleanValueDetails(context.Background(), "new-checkout", false, openfeature.NewEvaluationContext("user-1", nil))
	if err != nil || !d.Value || d.Variant != "true" || d.Reason != openfeature.StaticReason || d.FlagMetadata["flagVersion"] != int64(3) {
		t.Fatalf("details = %+v, %v", d, err)
	}
	for range 100 {
		if !boolValue(t, client, "new-checkout") {
			t.Fatal("want true")
		}
	}
	if n := len(fake.requestsTo(snapshotPath)) + len(fake.requestsTo(streamPath)); n != 2 {
		t.Fatalf("evaluations made requests: %d in all", n)
	}
}

func TestConfigurationChanged(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}, testFlag{"b", false, 1}), 1)
	_, client, events, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 1)
	fake.setSnapshot(snapshotJSON(2, testFlag{"a", true, 1}, testFlag{"b", true, 2}, testFlag{"c", true, 1}), 2)
	e := events.next(t, openfeature.ProviderConfigChange)
	if !reflect.DeepEqual(e.FlagChanges, []string{"b", "c"}) {
		t.Fatalf("flags changed = %v", e.FlagChanges)
	}
	if !boolValue(t, client, "b") || !boolValue(t, client, "c") {
		t.Fatal("the new snapshot isn't evaluated")
	}

	// A flag that disappears is a change too.
	fake.setSnapshot(snapshotJSON(3, testFlag{"a", true, 1}, testFlag{"b", true, 2}), 3)
	if e := events.next(t, openfeature.ProviderConfigChange); !reflect.DeepEqual(e.FlagChanges, []string{"c"}) {
		t.Fatalf("flags changed = %v", e.FlagChanges)
	}
	if d, _ := client.BooleanValueDetails(context.Background(), "c", false, openfeature.EvaluationContext{}); d.ErrorCode != openfeature.FlagNotFoundCode {
		t.Fatalf("removed flag: %+v", d)
	}
}

func TestBadSnapshotKeepsTheOldOne(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	_, client, events, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 1)
	fake.setSnapshot([]byte(`{"flags":{"a":{"state":"ENABLED","variants":{"x":1},"defaultVariant":"nope"}}}`), 2)
	events.none(t, openfeature.ProviderConfigChange, 100*time.Millisecond)
	if !boolValue(t, client, "a") {
		t.Fatal("the old snapshot was dropped")
	}
}

func TestReconnectsAfterEndRotate(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	_, _, events, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 1)
	fake.setSnapshot(snapshotJSON(2, testFlag{"a", false, 2}), 2)
	events.next(t, openfeature.ProviderConfigChange)
	fake.endStreams("rotate")
	fake.waitFor("a second stream", 5*time.Second, func() bool { return len(fake.requestsTo(streamPath)) == 2 })
	if got := fake.requestsTo(streamPath)[1].body; got != `{"version":2}` {
		t.Fatalf("reconnect body = %s", got)
	}
	waitStreams(t, fake, 1)
	if n := len(fake.requestsTo(snapshotPath)); n != 1 {
		t.Fatalf("a rotation fetched the snapshot again (%d fetches)", n)
	}
	events.none(t, openfeature.ProviderConfigChange, 50*time.Millisecond)
}

func TestStaleWhenDownThenReady(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	p, client, events, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 1)
	fake.failPath(snapshotPath, http.StatusServiceUnavailable, "", 0)
	fake.failPath(streamPath, http.StatusServiceUnavailable, "", 0)
	fake.dropStreams()
	events.next(t, openfeature.ProviderStale)
	if client.State() != openfeature.StaleState {
		t.Fatalf("state = %s", client.State())
	}
	if !boolValue(t, client, "a") {
		t.Fatal("stale provider stopped answering from its last snapshot")
	}
	if r := p.BooleanEvaluation(context.Background(), "a", false, nil); !r.Value || r.Reason != openfeature.StaticReason {
		t.Fatalf("direct evaluation = %+v", r)
	}
	fake.clearFailures()
	events.next(t, openfeature.ProviderReady)
	if client.State() != openfeature.ReadyState {
		t.Fatalf("state = %s", client.State())
	}
}

func TestFallsBackToPolling(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", false, 1}), 1)
	_, client, events, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 1)
	fake.failPath(streamPath, http.StatusServiceUnavailable, "", 0)
	fake.dropStreams()
	fake.waitFor("a poll", 5*time.Second, func() bool { return len(fake.requestsTo(snapshotPath)) >= 2 })
	polls := fake.requestsTo(snapshotPath)[1:]
	if polls[0].ifNoneMatch != `"1"` {
		t.Fatalf("poll If-None-Match = %q", polls[0].ifNoneMatch)
	}
	// The stream failed three times in a row (the drop and two 503s) before the first poll.
	failed := 0
	for _, r := range fake.requestsTo(streamPath)[1:] {
		if r.at.Before(polls[0].at) {
			failed++
		}
	}
	if failed != 2 {
		t.Fatalf("%d failed stream attempts after the drop before polling, want 2", failed)
	}
	fake.waitFor("polls repeat", 5*time.Second, func() bool { return len(fake.requestsTo(snapshotPath)) >= 4 })

	fake.setSnapshot(snapshotJSON(2, testFlag{"a", true, 2}), 2)
	if e := events.next(t, openfeature.ProviderConfigChange); !reflect.DeepEqual(e.FlagChanges, []string{"a"}) {
		t.Fatalf("flags changed = %v", e.FlagChanges)
	}
	if !boolValue(t, client, "a") {
		t.Fatal("the polled snapshot isn't evaluated")
	}
	if client.State() != openfeature.ReadyState {
		t.Fatalf("polling successfully should stay ready, state = %s", client.State())
	}

	// Streaming again stops the polling.
	fake.clearFailures()
	waitStreams(t, fake, 1)
	n := len(fake.requestsTo(snapshotPath))
	time.Sleep(100 * time.Millisecond)
	if m := len(fake.requestsTo(snapshotPath)); m != n {
		t.Fatalf("still polling with a stream open (%d → %d)", n, m)
	}
}

func TestFatalOn401(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	_, client, events, err := setup(t, fake, "ezgh_fs_wrong")
	if err == nil {
		t.Fatal("init succeeded with a refused key")
	}
	// OpenFeature doesn't pass an event's error code to handlers; FATAL state says it was
	// PROVIDER_FATAL.
	events.next(t, openfeature.ProviderError)
	if client.State() != openfeature.FatalState {
		t.Fatalf("state = %s", client.State())
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(fake.requestsTo(snapshotPath)) + len(fake.requestsTo(streamPath)); n != 1 {
		t.Fatalf("a fatal provider kept calling (%d requests)", n)
	}
}

func TestFatalWhenRevoked(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	p, client, events, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 1)
	fake.endStreams("revoked")
	// OpenFeature doesn't pass an event's error code to handlers; FATAL state says it was
	// PROVIDER_FATAL.
	events.next(t, openfeature.ProviderError)
	if client.State() != openfeature.FatalState {
		t.Fatalf("state = %s", client.State())
	}
	// Fatal at once: no reconnect.
	time.Sleep(100 * time.Millisecond)
	if n := len(fake.requestsTo(streamPath)); n != 1 {
		t.Fatalf("a revoked provider reconnected (%d stream requests)", n)
	}
	if r := p.BooleanEvaluation(context.Background(), "a", false, nil); !r.Value {
		t.Fatalf("direct evaluation after fatal = %+v", r)
	}
}

func TestHonoursRetryAfter(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	fake.failPath(snapshotPath, http.StatusTooManyRequests, "1", 1)
	_, _, _, err := setup(t, fake, testKey, func(c *config) { c.initWait = 5 * time.Second })
	if err != nil {
		t.Fatal(err)
	}
	snaps := fake.requestsTo(snapshotPath)
	if len(snaps) != 2 {
		t.Fatalf("%d snapshot requests, want 2", len(snaps))
	}
	if gap := snaps[1].at.Sub(snaps[0].at); gap < 950*time.Millisecond {
		t.Fatalf("retried the snapshot after %s despite Retry-After: 1", gap)
	}

	// The stream honours it too.
	waitStreams(t, fake, 1)
	fake.failPath(streamPath, http.StatusServiceUnavailable, "1", 1)
	fake.dropStreams()
	fake.waitFor("the stream back", 5*time.Second, func() bool { return len(fake.requestsTo(streamPath)) == 3 })
	streams := fake.requestsTo(streamPath)
	if gap := streams[2].at.Sub(streams[1].at); gap < 950*time.Millisecond {
		t.Fatalf("reconnected after %s despite Retry-After: 1", gap)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 10, 12, 9, 0, 0, 0, time.UTC)
	for in, want := range map[string]time.Duration{
		"":                              0,
		"3":                             3 * time.Second,
		"-1":                            0,
		"soon":                          0,
		"Mon, 12 Oct 2026 09:00:05 GMT": 5 * time.Second,
		"Mon, 12 Oct 2026 08:59:00 GMT": 0,
	} {
		if got := parseRetryAfter(in, now); got != want {
			t.Errorf("parseRetryAfter(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestInitFailureThenRecovery(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	fake.failPath(snapshotPath, http.StatusServiceUnavailable, "", 0)
	fake.failPath(streamPath, http.StatusServiceUnavailable, "", 0)
	_, client, events, err := setup(t, fake, testKey)
	if err == nil {
		t.Fatal("init succeeded without a snapshot")
	}
	if client.State() != openfeature.ErrorState {
		t.Fatalf("state = %s", client.State())
	}
	if d, _ := client.BooleanValueDetails(context.Background(), "a", false, openfeature.EvaluationContext{}); d.Value {
		t.Fatalf("evaluated without a snapshot: %+v", d)
	}
	fake.clearFailures()
	events.next(t, openfeature.ProviderReady)
	if !boolValue(t, client, "a") {
		t.Fatal("not evaluating after recovery")
	}
}

func writeSnapshotFile(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "flags.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBootstrapWithoutNetwork(t *testing.T) {
	down := httptest.NewServer(http.NotFoundHandler())
	down.Close()
	api := isolated.NewAPI()
	t.Cleanup(func() { _ = api.Shutdown(context.Background()) })
	p := New(testKey, WithEndpoint(down.URL), WithBootstrapFile(writeSnapshotFile(t, snapshotJSON(7, testFlag{"a", true, 1}))), fast)
	start := time.Now()
	if err := api.SetProviderAndWait(context.Background(), p); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("bootstrap waited for the network (%s)", time.Since(start))
	}
	if !boolValue(t, api.NewClient(), "a") {
		t.Fatal("bootstrap snapshot not evaluated")
	}
}

func TestBootstrapThenFollowsTheDataPlane(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(8, testFlag{"a", true, 2}), 8)
	path := writeSnapshotFile(t, snapshotJSON(7, testFlag{"a", false, 1}))
	_, client, events, err := setup(t, fake, testKey, WithBootstrapFile(path))
	if err != nil {
		t.Fatal(err)
	}
	if e := events.next(t, openfeature.ProviderConfigChange); !reflect.DeepEqual(e.FlagChanges, []string{"a"}) {
		t.Fatalf("flags changed = %v", e.FlagChanges)
	}
	if !boolValue(t, client, "a") {
		t.Fatal("the data plane's snapshot isn't evaluated")
	}
	// A bootstrap file's version is never sent: the data plane sends its snapshot.
	if got := fake.requestsTo(streamPath)[0].body; got != `{}` {
		t.Fatalf("stream body = %s", got)
	}
}

func TestOffline(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", false, 1}), 1)
	path := writeSnapshotFile(t, snapshotJSON(7, testFlag{"a", true, 1}))
	_, client, _, err := setup(t, fake, testKey, WithBootstrapFile(path), WithOffline())
	if err != nil {
		t.Fatal(err)
	}
	if !boolValue(t, client, "a") {
		t.Fatal("offline snapshot not evaluated")
	}
	time.Sleep(100 * time.Millisecond)
	if n := len(fake.requestsTo(snapshotPath)) + len(fake.requestsTo(streamPath)); n != 0 {
		t.Fatalf("offline provider made %d requests", n)
	}
}

func TestOfflineNeedsABootstrapFile(t *testing.T) {
	p := New(testKey, WithOffline())
	err := p.InitWithContext(context.Background(), openfeature.EvaluationContext{})
	var ie *openfeature.ProviderInitError
	if !errors.As(err, &ie) || ie.ErrorCode != openfeature.ProviderFatalCode {
		t.Fatalf("err = %v", err)
	}
	p = New(testKey, WithBootstrapFile(filepath.Join(t.TempDir(), "missing.json")), WithOffline())
	if err := p.InitWithContext(context.Background(), openfeature.EvaluationContext{}); err == nil || !strings.Contains(err.Error(), "bootstrap") {
		t.Fatalf("missing file: %v", err)
	}
}

func TestBrokenBootstrapFallsBackToTheNetwork(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	path := writeSnapshotFile(t, []byte("not json"))
	_, client, _, err := setup(t, fake, testKey, WithBootstrapFile(path))
	if err != nil {
		t.Fatal(err)
	}
	if !boolValue(t, client, "a") {
		t.Fatal("the data plane's snapshot isn't evaluated")
	}
}

func TestTypeMismatch(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	_, client, _, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	d, err := client.StringValueDetails(context.Background(), "a", "fallback", openfeature.EvaluationContext{})
	if err == nil || d.Value != "fallback" || d.ErrorCode != openfeature.TypeMismatchCode || d.Reason != openfeature.ErrorReason {
		t.Fatalf("details = %+v, err = %v", d, err)
	}
	f, err := client.FloatValueDetails(context.Background(), "a", 1.5, openfeature.EvaluationContext{})
	if err == nil || f.Value != 1.5 || f.ErrorCode != openfeature.TypeMismatchCode {
		t.Fatalf("details = %+v, err = %v", f, err)
	}
}

func TestShutdownStops(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	p := New(testKey, WithEndpoint(fake.url()), fast)
	if err := p.InitWithContext(context.Background(), openfeature.EvaluationContext{}); err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := p.ShutdownWithContext(ctx); err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 0)
	n := len(fake.requestsTo(streamPath))
	time.Sleep(100 * time.Millisecond)
	if m := len(fake.requestsTo(streamPath)); m != n {
		t.Fatalf("reconnected after shutdown (%d → %d)", n, m)
	}
	if err := p.ShutdownWithContext(ctx); err != nil {
		t.Fatalf("second shutdown: %v", err)
	}
}

func TestChangedKeys(t *testing.T) {
	got := changedKeys(map[string]int64{"a": 1, "b": 2, "gone": 1}, map[string]int64{"a": 1, "b": 3, "new": 1})
	if want := []string{"b", "gone", "new"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("changedKeys = %v, want %v", got, want)
	}
	if got := changedKeys(map[string]int64{"a": 1}, map[string]int64{"a": 1}); len(got) != 0 {
		t.Fatalf("no change = %v", got)
	}
}

func TestEventsNeverBlockWithoutAListener(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", false, 1}), 1)
	p := New(testKey, WithEndpoint(fake.url()), fast)
	if err := p.InitWithContext(context.Background(), openfeature.EvaluationContext{}); err != nil {
		t.Fatal(err)
	}
	defer p.Shutdown()
	waitStreams(t, fake, 1)
	for v := int64(2); v <= 60; v++ {
		fake.setSnapshot(snapshotJSON(v, testFlag{"a", v%2 == 0, v}), v)
	}
	fake.waitFor("the last snapshot applied", 5*time.Second, func() bool {
		r := p.BooleanEvaluation(context.Background(), "a", false, nil)
		return r.FlagMetadata["flagVersion"] == int64(60)
	})
	if n := len(p.EventChannel()); n != cap(p.events) {
		t.Fatalf("%d events queued, want a full buffer (%d)", n, cap(p.events))
	}
}

func TestSegmentOnlyChangeStillNotifies(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	_, _, events, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 1)
	fake.setSnapshot(snapshotJSON(2, testFlag{"a", true, 1}), 2)
	e := events.next(t, openfeature.ProviderConfigChange)
	if len(e.FlagChanges) != 0 || e.EventMetadata["snapshotVersion"] != int64(2) {
		t.Fatalf("event = %+v", e)
	}
}

func TestEndEnvironmentDeletedThen401IsFatal(t *testing.T) {
	fake := newFakeDataPlane(t, testKey, snapshotJSON(1, testFlag{"a", true, 1}), 1)
	_, client, events, err := setup(t, fake, testKey)
	if err != nil {
		t.Fatal(err)
	}
	waitStreams(t, fake, 1)
	fake.mu.Lock()
	fake.key = "gone"
	fake.mu.Unlock()
	fake.endStreams("environmentDeleted")
	events.next(t, openfeature.ProviderError)
	if client.State() != openfeature.FatalState {
		t.Fatalf("state = %s", client.State())
	}
	if n := len(fake.requestsTo(streamPath)); n != 2 {
		t.Fatalf("%d stream requests, want 2", n)
	}
}
