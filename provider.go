// Package ezghflags is the EZGH Feature Flags provider for OpenFeature's Go SDK.
//
// The provider keeps the environment's current snapshot (flagd's flag-definition JSON, compiled
// by EZGH) and evaluates every flag locally with flagd's Go core: no network call per
// evaluation. It loads the snapshot from the data plane at Init, follows its snapshot stream,
// and falls back to polling while the stream is failing.
//
//	p := ezghflags.New(os.Getenv("EZGH_FLAGS_SERVER_KEY"))
//	if err := openfeature.SetProviderAndWait(p); err != nil { … }
//	client := openfeature.NewDefaultClient()
//	on, _ := client.BooleanValue(ctx, "new-checkout", false, openfeature.NewEvaluationContext("user-1842", nil))
package ezghflags

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-feature/flagd/core/pkg/model"
	"github.com/open-feature/go-sdk/openfeature"
)

// Name is the provider's OpenFeature metadata name.
const Name = "ezgh-flags"

// Version is this module's release.
const Version = "0.1.0"

// Provider is an OpenFeature provider for one EZGH Feature Flags environment, identified by a
// server SDK key (ezgh_fs_…). Create it with New.
type Provider struct {
	sdkKey string
	cfg    config
	snap   atomic.Pointer[snapshot]
	// netVersion is the version of the last snapshot loaded from the data plane (0 before
	// one): what the stream's {version} and If-None-Match name. A bootstrap file's version is
	// never sent, since the file may be stale or from elsewhere.
	netVersion atomic.Int64
	events     chan openfeature.Event

	// life guards the background loop's lifecycle (Init and Shutdown).
	life   sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}

	// mu guards the connection state below and orders the events the provider emits.
	mu         sync.Mutex
	connected  bool
	stale      bool
	fatal      bool
	staleTimer *time.Timer
}

var (
	_ openfeature.FeatureProvider          = (*Provider)(nil)
	_ openfeature.ContextAwareStateHandler = (*Provider)(nil)
	_ openfeature.EventHandler             = (*Provider)(nil)
)

// New makes a provider for the environment of sdkKey, a server key. It does nothing until
// OpenFeature initializes it (SetProviderAndWait).
func New(sdkKey string, opts ...Option) *Provider {
	cfg := defaultConfig()
	for _, o := range opts {
		o(&cfg)
	}
	return &Provider{sdkKey: sdkKey, cfg: cfg, events: make(chan openfeature.Event, 16)}
}

// Metadata names the provider.
func (p *Provider) Metadata() openfeature.Metadata { return openfeature.Metadata{Name: Name} }

// Hooks are none.
func (p *Provider) Hooks() []openfeature.Hook { return nil }

// EventChannel carries the provider's events: PROVIDER_CONFIGURATION_CHANGED on a new snapshot,
// PROVIDER_STALE when disconnected, PROVIDER_READY on recovery and PROVIDER_ERROR (fatal) when
// the key is refused.
func (p *Provider) EventChannel() <-chan openfeature.Event { return p.events }

// BooleanEvaluation evaluates a boolean flag.
func (p *Provider) BooleanEvaluation(ctx context.Context, flag string, defaultValue bool, fc openfeature.FlattenedContext) openfeature.BoolResolutionDetail {
	return evaluateAs(ctx, p, flag, defaultValue, fc, func(v any) (bool, bool) {
		b, ok := v.(bool)
		return b, ok
	})
}

// StringEvaluation evaluates a string flag.
func (p *Provider) StringEvaluation(ctx context.Context, flag string, defaultValue string, fc openfeature.FlattenedContext) openfeature.StringResolutionDetail {
	return evaluateAs(ctx, p, flag, defaultValue, fc, func(v any) (string, bool) {
		s, ok := v.(string)
		return s, ok
	})
}

// FloatEvaluation evaluates a number flag.
func (p *Provider) FloatEvaluation(ctx context.Context, flag string, defaultValue float64, fc openfeature.FlattenedContext) openfeature.FloatResolutionDetail {
	return evaluateAs(ctx, p, flag, defaultValue, fc, func(v any) (float64, bool) {
		f, ok := v.(float64)
		return f, ok
	})
}

// IntEvaluation evaluates a number flag whose value is an integer; a fraction is a type mismatch.
func (p *Provider) IntEvaluation(ctx context.Context, flag string, defaultValue int64, fc openfeature.FlattenedContext) openfeature.IntResolutionDetail {
	return evaluateAs(ctx, p, flag, defaultValue, fc, func(v any) (int64, bool) {
		f, ok := v.(float64)
		if !ok || f != math.Trunc(f) || f < math.MinInt64 || f >= math.MaxInt64 {
			return 0, false
		}
		return int64(f), true
	})
}

// ObjectEvaluation evaluates an object flag. Values are JSON-decoded: map[string]any (or []any),
// with numbers as float64.
func (p *Provider) ObjectEvaluation(ctx context.Context, flag string, defaultValue any, fc openfeature.FlattenedContext) openfeature.InterfaceResolutionDetail {
	return evaluateAs(ctx, p, flag, defaultValue, fc, func(v any) (any, bool) {
		switch v.(type) {
		case map[string]any, []any:
			return v, true
		}
		return nil, false
	})
}

// evaluateAs evaluates flag against the current snapshot and types its value. Any error answers
// the caller's default with reason ERROR.
func evaluateAs[T any](ctx context.Context, p *Provider, flag string, def T, fc openfeature.FlattenedContext, as func(any) (T, bool)) openfeature.GenericResolutionDetail[T] {
	fail := func(e openfeature.ResolutionError) openfeature.GenericResolutionDetail[T] {
		return openfeature.GenericResolutionDetail[T]{Value: def, ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
			ResolutionError: e,
			Reason:          openfeature.ErrorReason,
		}}
	}
	s := p.snap.Load()
	if s == nil {
		return fail(openfeature.NewProviderNotReadyResolutionError("ezghflags: no snapshot loaded yet"))
	}
	r := s.evaluate(ctx, flag, fc)
	switch r.errorCode {
	case "":
	case model.FlagNotFoundErrorCode:
		return fail(openfeature.NewFlagNotFoundResolutionError(fmt.Sprintf("flag %q not found", flag)))
	case model.ParseErrorCode:
		return fail(openfeature.NewParseErrorResolutionError(fmt.Sprintf("flag %q: the targeting rules failed to evaluate", flag)))
	case model.InvalidContextCode:
		return fail(openfeature.NewInvalidContextResolutionError(fmt.Sprintf("flag %q: invalid evaluation context", flag)))
	default:
		return fail(openfeature.NewGeneralResolutionError(fmt.Sprintf("flag %q: evaluation failed", flag)))
	}
	v, ok := as(r.value)
	if !ok {
		return fail(openfeature.NewTypeMismatchResolutionError(fmt.Sprintf("flag %q: the value is %T, not %T", flag, r.value, def)))
	}
	return openfeature.GenericResolutionDetail[T]{Value: v, ProviderResolutionDetail: openfeature.ProviderResolutionDetail{
		Reason:       openfeature.Reason(r.reason),
		Variant:      r.variant,
		FlagMetadata: openfeature.FlagMetadata{"flagVersion": r.flagVersion},
	}}
}

// loadBootstrap reads and loads the bootstrap file.
func (p *Provider) loadBootstrap() error {
	data, err := os.ReadFile(p.cfg.bootstrapPath)
	if err != nil {
		return fmt.Errorf("ezghflags: bootstrap file: %w", err)
	}
	s, err := loadSnapshot(data)
	if err != nil {
		return fmt.Errorf("ezghflags: bootstrap file %s: %w", p.cfg.bootstrapPath, err)
	}
	p.snap.Store(s)
	return nil
}

// Init initializes the provider with a background context (see InitWithContext).
func (p *Provider) Init(evalCtx openfeature.EvaluationContext) error {
	return p.InitWithContext(context.Background(), evalCtx)
}

// Shutdown stops the provider (see ShutdownWithContext).
func (p *Provider) Shutdown() { _ = p.ShutdownWithContext(context.Background()) }

// InitWithContext loads the first snapshot and starts following the data plane.
//
//   - With WithOffline, it loads the bootstrap file and nothing else.
//   - With WithBootstrapFile, it loads the file and is ready at once; the data plane's newer
//     snapshot arrives through the stream.
//   - Otherwise it fetches /v1/sync/snapshot, retrying 429, 503 and network errors (honouring
//     Retry-After) for up to 10 seconds or until ctx ends. If that fails, it answers the error
//     (OpenFeature's ERROR state) and keeps trying in the background, emitting PROVIDER_READY
//     when a snapshot arrives.
//
// A refused key (401 or 403) is fatal (PROVIDER_FATAL): the provider stops calling.
func (p *Provider) InitWithContext(ctx context.Context, _ openfeature.EvaluationContext) error {
	p.life.Lock()
	defer p.life.Unlock()
	if p.cancel != nil {
		return p.currentState()
	}
	warnUnpinnedCores(p.cfg.logger)
	if p.cfg.offline {
		if p.cfg.bootstrapPath == "" {
			return &openfeature.ProviderInitError{ErrorCode: openfeature.ProviderFatalCode, Message: "ezghflags: WithOffline needs WithBootstrapFile"}
		}
		if err := p.loadBootstrap(); err != nil {
			return &openfeature.ProviderInitError{ErrorCode: openfeature.ProviderFatalCode, Message: err.Error()}
		}
		return nil
	}
	if p.cfg.bootstrapPath != "" {
		if err := p.loadBootstrap(); err != nil {
			p.cfg.logger.Error("ezghflags: ignoring the bootstrap file; loading from the data plane", "error", err)
		}
	}

	var initErr error
	if p.snap.Load() == nil {
		initErr = p.firstFetch(ctx)
		var fe *fatalError
		if errors.As(initErr, &fe) {
			p.mu.Lock()
			p.fatal = true
			p.mu.Unlock()
			return &openfeature.ProviderInitError{ErrorCode: openfeature.ProviderFatalCode, Message: fe.Error()}
		}
	}
	p.mu.Lock()
	p.connected = initErr == nil
	p.stale, p.fatal = false, false
	p.mu.Unlock()

	runCtx, cancel := context.WithCancel(context.Background())
	p.cancel, p.done = cancel, make(chan struct{})
	go p.run(runCtx, p.done)
	if initErr != nil {
		return &openfeature.ProviderInitError{ErrorCode: openfeature.GeneralCode, Message: initErr.Error()}
	}
	return nil
}

// ShutdownWithContext stops following the data plane and waits for the background loop, or for
// ctx. Evaluations keep answering from the last snapshot.
func (p *Provider) ShutdownWithContext(ctx context.Context) error {
	p.life.Lock()
	cancel, done := p.cancel, p.done
	p.cancel, p.done = nil, nil
	p.life.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	p.mu.Lock()
	if p.staleTimer != nil {
		p.staleTimer.Stop()
		p.staleTimer = nil
	}
	p.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// fatalError is a refused key: the provider stops.
type fatalError struct{ status int }

func (e *fatalError) Error() string {
	return fmt.Sprintf("ezghflags: the data plane refused the SDK key (HTTP %d); check that it's a live server key", e.status)
}

// retryError is a failure worth retrying, after at least retryAfter.
type retryError struct {
	err        error
	retryAfter time.Duration
}

func (e *retryError) Error() string { return e.err.Error() }
func (e *retryError) Unwrap() error { return e.err }

// statusError classifies a non-success answer.
func statusError(resp *http.Response, what string) error {
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return &fatalError{status: resp.StatusCode}
	}
	return &retryError{
		err:        fmt.Errorf("ezghflags: %s: HTTP %d", what, resp.StatusCode),
		retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
	}
}

// parseRetryAfter reads Retry-After as seconds or an HTTP date; anything else is zero.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if s, err := strconv.ParseInt(v, 10, 64); err == nil {
		if s <= 0 {
			return 0
		}
		return time.Duration(min(s, 3600)) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return min(t.Sub(now), time.Hour)
	}
	return 0
}

// backoff is the delay before retry number n (1-based): exponential from minBackoff to
// maxBackoff, with jitter (half fixed, half random), and at least retryAfter plus 0–20 %.
func (p *Provider) backoff(n int, retryAfter time.Duration) time.Duration {
	d := p.cfg.minBackoff
	for i := 1; i < n && d < p.cfg.maxBackoff; i++ {
		d *= 2
	}
	d = min(d, p.cfg.maxBackoff)
	d = d/2 + rand.N(d/2+1)
	return max(d, jitterUp(retryAfter))
}

// jitterUp adds 0–20 % to a Retry-After wait, so clients told the same moment don't return
// together.
func jitterUp(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d + rand.N(d/5+1)
}

// sleep waits d or until ctx ends, answering false when it ended.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// firstFetch fetches the first snapshot, retrying for up to initWait.
func (p *Provider) firstFetch(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.initWait)
	defer cancel()
	for n := 1; ; n++ {
		_, err := p.fetch(ctx, true)
		if err == nil {
			return nil
		}
		var fe *fatalError
		if errors.As(err, &fe) {
			return err
		}
		var re *retryError
		retryAfter := time.Duration(0)
		if errors.As(err, &re) {
			retryAfter = re.retryAfter
		}
		if !sleep(ctx, p.backoff(n, retryAfter)) {
			return fmt.Errorf("ezghflags: no snapshot within %s: %w", p.cfg.initWait, err)
		}
	}
}

// fetch GETs /v1/sync/snapshot with If-None-Match and applies a new snapshot. It answers whether
// the snapshot changed. During Init (initial), the first snapshot emits no event: OpenFeature
// emits PROVIDER_READY when Init returns. (Accepted race: a stream event emitted while OpenFeature
// is still finishing Init can be delivered before its PROVIDER_READY.)
func (p *Provider) fetch(ctx context.Context, initial bool) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.fetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.cfg.endpoint+"/v1/sync/snapshot", nil)
	if err != nil {
		return false, err
	}
	p.authorize(req)
	if v := p.netVersion.Load(); v > 0 {
		req.Header.Set("If-None-Match", strconv.Quote(strconv.FormatInt(v, 10)))
	}
	resp, err := p.cfg.client.Do(req)
	if err != nil {
		return false, &retryError{err: fmt.Errorf("ezghflags: snapshot: %w", err)}
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotModified:
		return false, nil
	default:
		return false, statusError(resp, "snapshot")
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxSnapshotBytes+1))
	if err != nil {
		return false, &retryError{err: fmt.Errorf("ezghflags: snapshot: %w", err)}
	}
	if len(data) > maxSnapshotBytes {
		return false, &retryError{err: errors.New("ezghflags: snapshot: too large")}
	}
	return p.apply(data, !initial)
}

// maxSnapshotBytes bounds a snapshot read (the data plane compiles at most 8 MiB).
const maxSnapshotBytes = 16 << 20

func (p *Provider) authorize(req *http.Request) {
	req.Header.Set("Authorization", "Bearer "+p.sdkKey)
	req.Header.Set("User-Agent", "ezgh-flags-provider-go/"+Version)
}

// apply loads data and swaps it in. A snapshot that doesn't load is logged and dropped, keeping
// the current one. The first snapshot after a failed Init emits PROVIDER_READY; later ones emit
// PROVIDER_CONFIGURATION_CHANGED on every new version, with the keys whose compiled definition
// changed or that appeared or disappeared (possibly none) and the snapshotVersion.
func (p *Provider) apply(data []byte, readyEvent bool) (bool, error) {
	s, err := loadSnapshot(data)
	if err != nil {
		p.cfg.logger.Error("ezghflags: ignoring a snapshot that doesn't load; keeping the current one", "error", err)
		return false, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	old := p.snap.Load()
	// Never go back: a snapshot older than the one held (a lagging pod or replica) is ignored.
	// A bootstrap file's version doesn't count, since it may be from elsewhere.
	if cur := p.netVersion.Load(); cur > 0 && s.version <= cur {
		return false, nil
	}
	p.snap.Store(s)
	p.netVersion.Store(s.version)
	if old == nil {
		if readyEvent {
			p.emit(openfeature.ProviderReady, openfeature.ProviderEventDetails{Message: "snapshot loaded"})
		}
		return true, nil
	}
	p.emit(openfeature.ProviderConfigChange, openfeature.ProviderEventDetails{
		Message:       fmt.Sprintf("snapshot %d", s.version),
		FlagChanges:   changedKeys(old.definitions(), s.definitions()),
		EventMetadata: map[string]any{"snapshotVersion": s.version},
	})
	return true, nil
}

// emit queues an event; the caller holds p.mu, which orders events. It never blocks: when nobody
// drains EventChannel (a provider used without OpenFeature) and the buffer is full, the oldest
// event is dropped, so the provider keeps following the data plane and the newest state wins.
func (p *Provider) emit(typ openfeature.EventType, d openfeature.ProviderEventDetails) {
	ev := openfeature.Event{ProviderName: Name, EventType: typ, ProviderEventDetails: d}
	for {
		select {
		case p.events <- ev:
			return
		default:
		}
		select {
		case <-p.events:
		default:
		}
	}
}

// markUp records a connection: a stream opened or a poll answered. A stale provider is ready
// again.
func (p *Provider) markUp() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connected = true
	if p.staleTimer != nil {
		p.staleTimer.Stop()
		p.staleTimer = nil
	}
	if p.stale && p.snap.Load() != nil {
		p.stale = false
		p.emit(openfeature.ProviderReady, openfeature.ProviderEventDetails{Message: "reconnected"})
	}
}

// markDown records a lost connection. Without one for staleAfter, the provider emits
// PROVIDER_STALE and keeps evaluating its last snapshot.
func (p *Provider) markDown(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.connected {
		return
	}
	p.connected = false
	p.staleTimer = time.AfterFunc(p.cfg.staleAfter, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		if p.connected || p.stale || p.fatal || ctx.Err() != nil || p.snap.Load() == nil {
			return
		}
		p.stale = true
		p.emit(openfeature.ProviderStale, openfeature.ProviderEventDetails{Message: "disconnected from the data plane; evaluating the last snapshot"})
	})
}

// setFatal stops the provider on a refused key.
func (p *Provider) setFatal(err error) {
	p.cfg.logger.Error("ezghflags: stopping", "error", err)
	p.mu.Lock()
	defer p.mu.Unlock()
	p.fatal = true
	if p.staleTimer != nil {
		p.staleTimer.Stop()
		p.staleTimer = nil
	}
	p.emit(openfeature.ProviderError, openfeature.ProviderEventDetails{Message: err.Error(), ErrorCode: openfeature.ProviderFatalCode})
}

// run follows the snapshot stream until ctx ends or the key is refused. A stream that fails (or
// can't open) is retried with backoff; after fallbackAfter failures in a row, the snapshot is
// also polled every pollEvery while waiting to retry.
func (p *Provider) run(ctx context.Context, done chan struct{}) {
	defer close(done)
	failures := 0
	pollOK := false
	var nextPoll time.Time
	for ctx.Err() == nil {
		end, connected, err := p.stream(ctx)
		if ctx.Err() != nil {
			return
		}
		var fe *fatalError
		if errors.As(err, &fe) {
			p.setFatal(err)
			return
		}
		if err == nil {
			// An end event. revoked and environmentDeleted are fatal at once; rotate reconnects at
			// once; anything else (shutdown) waits a moment, then reconnects to another pod.
			failures = 0
			switch end {
			case "revoked":
				p.setFatal(errors.New("ezghflags: the SDK key was revoked"))
				return
			case "environmentDeleted":
				p.setFatal(errors.New("ezghflags: the SDK key's environment was deleted"))
				return
			}
			if end != "rotate" && !sleep(ctx, p.backoff(1, 0)) {
				return
			}
			continue
		}
		if connected {
			failures = 0
		}
		failures++
		polling := failures >= p.cfg.fallbackAfter
		if !polling || !pollOK {
			p.markDown(ctx)
		}
		if failures == 1 || !errors.As(err, new(*retryError)) {
			p.cfg.logger.Warn("ezghflags: snapshot stream failed; retrying", "error", err, "failures", failures)
		}
		var re *retryError
		retryAfter := time.Duration(0)
		if errors.As(err, &re) {
			retryAfter = re.retryAfter
		}
		retryAt := time.Now().Add(p.backoff(failures, retryAfter))
		for {
			now := time.Now()
			if polling && !now.Before(nextPoll) {
				_, perr := p.fetch(ctx, false)
				if ctx.Err() != nil {
					return
				}
				if errors.As(perr, &fe) {
					p.setFatal(perr)
					return
				}
				nextPoll = time.Now().Add(p.cfg.pollEvery)
				pollOK = perr == nil
				if pollOK {
					p.markUp()
				} else {
					p.markDown(ctx)
					if errors.As(perr, &re) && re.retryAfter > 0 {
						nextPoll = time.Now().Add(max(p.cfg.pollEvery, jitterUp(re.retryAfter)))
					}
				}
				now = time.Now()
			}
			if !now.Before(retryAt) {
				break
			}
			wake := retryAt
			if polling && nextPoll.Before(wake) {
				wake = nextPoll
			}
			if !sleep(ctx, wake.Sub(now)) {
				return
			}
		}
	}
}

// stream opens POST /v1/sync/stream with the data plane's last version and applies its snapshot events
// until it ends. It answers the end event's reason (err nil), or the failure and whether the
// stream had opened.
func (p *Provider) stream(ctx context.Context) (end string, connected bool, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	version := p.netVersion.Load()
	body := "{}"
	if version > 0 {
		body = fmt.Sprintf(`{"version":%d}`, version)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.cfg.endpoint+"/v1/sync/stream", strings.NewReader(body))
	if err != nil {
		return "", false, err
	}
	p.authorize(req)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := p.cfg.client.Do(req)
	if err != nil {
		return "", false, &retryError{err: fmt.Errorf("ezghflags: stream: %w", err)}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", false, statusError(resp, "stream")
	}
	if mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mt != "text/event-stream" {
		return "", false, &retryError{err: fmt.Errorf("ezghflags: stream: Content-Type %q isn't text/event-stream", resp.Header.Get("Content-Type"))}
	}

	// The data plane sends a heartbeat comment every 15 s; a stream silent for idleTimeout is
	// dead (a half-open connection), so it's dropped.
	idle := time.AfterFunc(p.cfg.idleTimeout, cancel)
	defer idle.Stop()
	sr := newSSEReader(&idleReader{r: resp.Body, t: idle, d: p.cfg.idleTimeout}, maxSnapshotBytes+(1<<10))
	// The stream counts as connected (failures reset, STALE cleared) from its first frame or
	// comment: the data plane opens with ": open".
	sr.onLine = func() {
		if !connected {
			connected = true
			p.markUp()
		}
	}
	for {
		ev, err := sr.next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			return "", connected, &retryError{err: fmt.Errorf("ezghflags: stream: %w", err)}
		}
		switch ev.event {
		case "snapshot":
			_, _ = p.apply([]byte(ev.data), true)
		case "end":
			var e struct {
				Reason string `json:"reason"`
			}
			_ = json.Unmarshal([]byte(ev.data), &e)
			return e.Reason, connected, nil
		}
	}
}

// idleReader resets an idle timer on every read.
type idleReader struct {
	r io.Reader
	t *time.Timer
	d time.Duration
}

func (r *idleReader) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	if n > 0 {
		r.t.Reset(r.d)
	}
	return n, err
}

// currentState answers a repeated Init while the provider runs, so OpenFeature records its real
// state: fatal stays FATAL, no snapshot yet is an error, and a stale provider answers nil (the SDK
// then marks it READY) and re-emits PROVIDER_STALE just after.
func (p *Provider) currentState() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.fatal:
		return &openfeature.ProviderInitError{ErrorCode: openfeature.ProviderFatalCode, Message: "ezghflags: the data plane refused the SDK key"}
	case p.snap.Load() == nil:
		return &openfeature.ProviderInitError{ErrorCode: openfeature.GeneralCode, Message: "ezghflags: no snapshot loaded yet; still trying"}
	case p.stale:
		// OpenFeature emits PROVIDER_READY when Init returns; emitting STALE now could be
		// overtaken by it, so it's sent a moment later (if still stale).
		time.AfterFunc(restaleDelay, func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.stale && !p.fatal {
				p.emit(openfeature.ProviderStale, openfeature.ProviderEventDetails{Message: "disconnected from the data plane; evaluating the last snapshot"})
			}
		})
	}
	return nil
}

// restaleDelay is how long after a repeated Init a stale provider re-emits PROVIDER_STALE.
const restaleDelay = 50 * time.Millisecond
