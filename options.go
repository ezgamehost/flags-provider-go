package ezghflags

import (
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// DefaultEndpoint is the EZGH Feature Flags data plane.
const DefaultEndpoint = "https://flags.ezghcloud.com"

// Option configures a Provider.
type Option func(*config)

type config struct {
	endpoint      string
	client        *http.Client
	bootstrapPath string
	offline       bool
	logger        *slog.Logger

	// Timing. The defaults are the design's; tests shrink them.
	minBackoff    time.Duration // first stream retry delay (jittered, doubling)
	maxBackoff    time.Duration // largest stream retry delay
	pollEvery     time.Duration // snapshot polling while the stream is failing
	fallbackAfter int           // consecutive stream failures before polling starts
	staleAfter    time.Duration // without a connection this long, the provider is STALE
	idleTimeout   time.Duration // a stream silent this long (no heartbeat) is dropped
	initWait      time.Duration // how long Init retries a failing first fetch
	fetchTimeout  time.Duration // one snapshot request
}

func defaultConfig() config {
	return config{
		endpoint:      DefaultEndpoint,
		client:        &http.Client{},
		logger:        slog.Default(),
		minBackoff:    time.Second,
		maxBackoff:    time.Minute,
		pollEvery:     30 * time.Second,
		fallbackAfter: 3,
		staleAfter:    30 * time.Second,
		idleTimeout:   45 * time.Second,
		initWait:      10 * time.Second,
		fetchTimeout:  30 * time.Second,
	}
}

// WithEndpoint sets the data plane's base URL (default https://flags.ezghcloud.com).
func WithEndpoint(url string) Option {
	return func(c *config) { c.endpoint = strings.TrimRight(url, "/") }
}

// WithHTTPClient sets the HTTP client for the data plane. Its Timeout must be zero (or longer
// than a stream's life, 10 minutes): the snapshot stream is one long request.
func WithHTTPClient(client *http.Client) Option {
	return func(c *config) {
		if client != nil {
			c.client = client
		}
	}
}

// WithBootstrapFile loads a snapshot file (from `ezgh flags export`) at Init, so the provider is
// ready without the network. Unless WithOffline is set, it then follows the data plane as usual.
func WithBootstrapFile(path string) Option {
	return func(c *config) { c.bootstrapPath = path }
}

// WithOffline evaluates only the bootstrap file and never calls the network (tests, CI and
// air-gapped runs). It requires WithBootstrapFile.
func WithOffline() Option {
	return func(c *config) { c.offline = true }
}

// WithLogger sets the logger for connection problems and rejected snapshots (default
// slog.Default()). Evaluation contexts are never logged.
func WithLogger(l *slog.Logger) Option {
	return func(c *config) {
		if l != nil {
			c.logger = l
		}
	}
}
