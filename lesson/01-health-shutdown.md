# Module 1: Health, shutdown, config, JSON logs

Goal: a Go service that behaves well under Kubernetes. It says when it's ready, and on SIGTERM it finishes its work before exiting.

## Lessons

| # | Lesson | Status | Commit |
|---|---|---|---|
| 1.1 | Slow `/` endpoint as a test tool | ✅ | `5f3b060` |
| 1.2 | Own `http.Server` instead of `router.Run()`; `/livez`, `/readyz` | ✅ | `70af1eb` |
| 1.3 | Catch SIGINT/SIGTERM; wire the readiness flag | ✅ | `141e6e7` |
| 1.4 | Drain in-flight requests with `server.Shutdown` + timeout | ✅ | `bb5c5d3` |
| 1.5 | Fix: don't log "complete" after a timeout | ✅ | `0d867cf` |
| 1.6 | Config from environment variables (PORT, SHUTDOWN_TIMEOUT) | ✅ | see `git log -- config.go` |
| 1.7 | JSON logs with slog (our own lines) | ✅ | `0d867cf` |
| 1.8 | DRAIN_DELAY: see `/readyz` answer 503 during shutdown | ✅ | see `git log -- config.go` |
| 1.9 | Gin request logs as JSON (own middleware) | ⬜ | |

## 1.2 Own `http.Server`

`router.Run()` creates an `http.Server` internally and never gives it back, so you can't call `Shutdown` on it. Building the server yourself keeps a handle:

```go
server := &http.Server{Addr: ":8080", Handler: router}
```

The Gin router is just the `Handler`; `net/http` does the serving.

## 1.2 Liveness vs readiness

| Probe | Question | On failure, Kubernetes… | Checks |
|---|---|---|---|
| `/livez` | Is the process stuck? | restarts the container | nothing; stays 200 even while shutting down |
| `/readyz` | Should I get traffic now? | removes the pod from the Service | a flag that main sets |

- A probe is an **instant read** of a flag. It never waits.
- `atomic.Bool`: main writes the flag, and every request runs in its own goroutine (created by `net/http`) and reads it. Atomic makes that safe.
- `c.JSON` doesn't stop the handler. Without `return`, both responses get written.

## 1.3 Signals and the main goroutine

```
go ListenAndServe()   ← server runs in the background
SetReady(true)        ← main reaches this immediately
<-appCtx.Done()       ← main blocks here until Ctrl+C / SIGTERM
```

- `ListenAndServe` blocks while the server is up, so anything after it in the same goroutine runs only after the server dies.
- When `main()` returns, the whole program exits, including all goroutines.
- `signal.NotifyContext` returns a context that is cancelled on the signal. `stop()` unhooks the signal listener (a second Ctrl+C then kills immediately). It does **not** stop the server.
- Kubernetes sends SIGTERM, waits `terminationGracePeriodSeconds` (default 30 s), then sends SIGKILL.

## 1.4 Graceful drain

`server.Shutdown(ctx)`:
1. **Immediately** stops accepting new connections.
2. **Waits** for in-flight requests, up to the context's deadline.

The timeout is a **maximum**, not a fixed wait:

| Situation | Result |
|---|---|
| Nothing in flight | returns at once, `err == nil` |
| Request finishes in time | returns when it finishes, `err == nil` |
| Request outlives the deadline | `err == context.DeadlineExceeded`, request cut off |
| Anything else (rare) | some other error |

- Use a **fresh** `context.Background()` for the shutdown timeout. `appCtx` is already cancelled, so Shutdown would give up at once.
- The shutdown timeout must fit inside Kubernetes' grace period, or SIGKILL arrives mid-drain.

Observed: Ctrl+C ~0.8 s into a 5 s request → `[GIN] 200 | 5s`, shutdown in 4.19 s (the remaining time). A 15 s request is dropped at 10 s.

## 1.5 Only claim success on success

After a failed `Shutdown`, `return` so "complete" isn't logged too. `return` in `main` still runs the `defer`s.

## 1.6 Config from environment variables

The same image runs in every environment; Kubernetes passes settings in as env vars (Deployment or ConfigMap).

- Start from defaults, let each env var override, and **fail fast** on a value that is set but invalid.
- `os.LookupEnv` says whether a variable is set at all: unset → default, `PORT=` → error. `os.Getenv` returns `""` for both. Either is a valid choice; LookupEnv is stricter.
- `LoadConfig` returns `(Config, error)` and doesn't exit; main logs and calls `os.Exit(1)`. Load config after the logger so the error is JSON.
- `Addr` needs the colon: `":" + strconv.Itoa(port)`. `"8080"` fails with `missing port in address`.

Observed: `SHUTDOWN_TIMEOUT=abc` → JSON error at startup. `PORT=9097 SHUTDOWN_TIMEOUT=2s` → 5 s request cut off at 2 s.

## 1.7 JSON logs with slog

```go
slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil))) // in loggers.Setup()
```

- One JSON object per line, so a log tool (Cloud Logging later) can filter by field.
- slog takes **key/value pairs**, not format strings. The message stays fixed; data goes in named fields:
  `slog.Warn("shutdown timed out", "after", d.String())`. `go vet` catches `%s`-style mistakes.
- Levels: Info for normal, Warn for degraded but handled, Error for failed.
- `slog.Error` only logs. `log.Fatal` used to exit too, so add `os.Exit(1)` yourself.
- `time.Duration` is written as nanoseconds in JSON; use `.String()` for `"4.19s"`.
- Gin's `[GIN]` request lines are still plain text (1.9).

## 1.8 DRAIN_DELAY

When a pod is deleted, Kubernetes sends SIGTERM **and** removes the pod from the Service endpoints at the same time. The removal takes a few seconds to reach kube-proxy and load balancers, so traffic keeps arriving. Closing the listener at once means those requests get "connection refused".

```
SIGTERM → stop() → SetReady(false) → log → sleep DRAIN_DELAY → Shutdown(SHUTDOWN_TIMEOUT)
```

- Announce first (`/readyz` 503), then wait while **still serving**, then close.
- `stop()` before the sleep, so a second Ctrl+C still force-quits.
- Log **before** sleeping, or the drain looks like a hang.
- `time.Duration` is nanoseconds: a bare `5` means 5 ns. Write `5*time.Second`.
- "At least 5s" is `d < 5*time.Second` → error. `<=` would reject 5s itself.
- Budget: DRAIN_DELAY + SHUTDOWN_TIMEOUT < terminationGracePeriodSeconds (30 s).
- Comments say what/why, not values: "sets a context of 10s" went stale once the value became configurable.

Observed: 1 s after Ctrl+C, `/readyz` 503 and `/livez` 200. After 5 s the listener closed (curl `000`) and shutdown completed at once (nothing in flight).

## Concepts carried over from the first run

- **defer**: runs when the function returns, last in first out; arguments are evaluated immediately; skipped by `os.Exit` / `log.Fatal`.
- **App vs request context**: the app context lives as long as the process (signals). The request context, `c.Request.Context()`, lives as long as one request. `gin.Context` is neither, so use `c.Request.Context()` inside handlers.
- **`go run .`** builds every file in the package. `go run main.go` builds only that file.
