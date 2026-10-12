# EZGH Feature Flags provider for Go

An [OpenFeature](https://openfeature.dev) provider for EZGH Feature Flags. You write against
OpenFeature's Go SDK; the provider keeps your environment's flags in memory and evaluates them
locally with [flagd](https://flagd.dev)'s open-source Go core, so an evaluation never makes a
network call.

```sh
go get github.com/ezgamehost/flags-provider-go@v0.1.0
```

**Status:** in development (EZGH Feature Flags isn't launched yet). Every conformance vector in
flags-stack's `protocol/vectors/eval` (1,210 cases at `.flags-stack-rev`) passes through this
provider.

## Usage

Create a **server key** for the environment (`ezgh_fs_…`, in the console or with
`ezgh flags sdk-keys create`). Server keys are secret: use them in backends only.

```go
package main

import (
	"context"
	"log"
	"os"

	ezghflags "github.com/ezgamehost/flags-provider-go"
	"github.com/open-feature/go-sdk/openfeature"
)

func main() {
	provider := ezghflags.New(os.Getenv("EZGH_FLAGS_SERVER_KEY"))
	if err := openfeature.SetProviderAndWait(provider); err != nil {
		log.Fatal(err)
	}
	defer openfeature.Shutdown()

	client := openfeature.NewDefaultClient()
	evalCtx := openfeature.NewEvaluationContext("user-1842", map[string]any{
		"plan":    "pro",
		"country": "US",
	})
	if on, _ := client.BooleanValue(context.Background(), "new-checkout", false, evalCtx); on {
		// …
	}
}
```

Flag types map to OpenFeature's typed methods: boolean flags to `BooleanValue`, string flags to
`StringValue`, number flags to `FloatValue` (or `IntValue` for whole numbers) and object flags to
`ObjectValue` (`map[string]any`, numbers as `float64`). Asking for the wrong type answers your
default with `TYPE_MISMATCH`.

Reasons are EZGH's: `DISABLED` for a flag that's off in the environment (with its off value),
`STATIC` for a flag on with no rules, `TARGETING_MATCH` when a target, rule or rollout chose the
value, and `DEFAULT` when rules exist and none matched. `FlagMetadata["flagVersion"]` (an
`int64`) is the flag's config version in the environment.

### Options

| Option | |
| --- | --- |
| `WithEndpoint(url)` | The data plane (default `https://flags.ezghcloud.com`). |
| `WithHTTPClient(c)` | The HTTP client. Leave its `Timeout` at zero: the snapshot stream is one long request. |
| `WithBootstrapFile(path)` | A snapshot file from `ezgh flags export --env <env> > flags.json`. The provider is ready from it at once, without the network, then follows the data plane. A file that doesn't load is logged and skipped. |
| `WithOffline()` | Evaluate only the bootstrap file and never call the network (tests, CI, air-gapped runs). Needs `WithBootstrapFile`. |
| `WithLogger(l)` | A `*slog.Logger` for connection problems (default `slog.Default()`). Evaluation contexts are never logged. |

## How it stays current

- **Init** fetches the environment's snapshot (`GET /v1/sync/snapshot`); the provider is
  `READY` once it's loaded. A failing first fetch (`429`, `503`, network errors) is retried for
  up to 10 seconds, honouring `Retry-After`. If it still fails, `SetProviderAndWait` answers
  the error (state `ERROR`), the provider keeps trying in the background, and it emits
  `PROVIDER_READY` when a snapshot arrives.
- It then follows the snapshot stream (`POST /v1/sync/stream`), which sends the whole snapshot
  on every change. Each new snapshot version emits `PROVIDER_CONFIGURATION_CHANGED`, with
  `snapshotVersion` in `EventMetadata` and, in `FlagChanges`, every flag whose compiled definition
  changed (a segment change lists the flags that use the segment) or that appeared or
  disappeared. `FlagChanges` can be empty. A snapshot older than the one held is ignored.
- The stream is retried with exponential backoff and jitter (1 s to 60 s, and never sooner than
  `Retry-After` plus 0–20 %). A stream counts as connected once its first frame or comment
  arrives (and only with `Content-Type: text/event-stream`). After three failures in a row, the
  provider also polls the snapshot every 30 seconds with `If-None-Match` (also honouring
  `Retry-After`) until the stream is back; a successful poll counts as connected.
- **Disconnected** for 30 seconds, it emits `PROVIDER_STALE` and keeps evaluating its last
  snapshot; it emits `PROVIDER_READY` when it reconnects.
- **A refused key** (`401` or `403`: a revoked key, a deleted environment, a client key) or a
  stream ending with `revoked` or `environmentDeleted` is fatal: `PROVIDER_ERROR` with `PROVIDER_FATAL`, and the provider
  stops calling. OpenFeature then answers defaults.
- A bootstrap file's version is never sent to the data plane (as the stream's `version` or
  `If-None-Match`), so the first connection always loads the data plane's own snapshot.

- Calling `Init` again on a running provider (binding it to another domain) answers its current
  state: fatal stays fatal, no snapshot yet is an error, and a stale provider re-emits
  `PROVIDER_STALE`.

### Evaluator versions

The provider is tested against `github.com/open-feature/flagd/core` v0.18.0 and
`github.com/diegoholiveira/jsonlogic/v3` v3.9.1, the versions EZGH's data plane runs, and its
`go.mod` requires exactly those. **Don't upgrade them independently** in your module: another
version can evaluate some rules differently from the data plane. If your build resolves other
versions (Go's minimal version selection lets another dependency raise them), the provider logs a
warning at Init. Upgrade this provider instead when a release moves them.

The provider holds one thing in memory: the environment's current snapshot. It never stores or
logs evaluation contexts.

## Development

```sh
gofmt -l . && go vet ./... && staticcheck ./... && go test -race ./...
```

`testdata/vectors` are flags-stack's conformance vectors, copied verbatim. To move to a newer
flags-stack:

```sh
scripts/sync-vectors.sh ~/code/flags-stack origin/main   # writes testdata/vectors and .flags-stack-rev
go test ./... -run Vectors
```

A failing vector is a disagreement between evaluator cores: report it to flags-stack rather than
changing the vector. CI checks that `testdata/vectors` matches flags-stack at `.flags-stack-rev`.

## License

Apache-2.0
