# Design

## Context

Two goroutines are started per SSE-mode BMC by `ConnectionManager.Ensure` — one with `StreamKind=Metrics`, one with `StreamKind=Alerts`. Both goroutines share the same `sseConn.stream()` code path. `stream()` unconditionally calls `c.cfg.Vendor.SSEURL()`, which returns a MetricReport-filtered URL. `AlertSSEURL()` was added to `VendorConfig` in the dual-stream change but is never called anywhere.

For MetricReport-mode BMCs, `push.AddTo` constructs a `Receiver` with `ReceiverConfig{StatusWriter: nil}`. `BMCReconciler` implements `push.StatusWriter` and has `RecordMetricReport`/`RecordAlertEvent`, but `push.Options` has no field for it, so the reconciler is never passed in.

## Goals / Non-Goals

**Goals:**
- Fix `stream()` so the metrics goroutine uses `SSEURL()` and the alerts goroutine uses `AlertSSEURL()`
- Wire `BMCReconciler` as `StatusWriter` on the push receiver so `LastEventTime` is updated on MetricReport and alert POSTs
- Keep all changes minimal and surgical — no refactors, no new abstractions

**Non-Goals:**
- Changing the rate-limiting logic on `SetLastEventTime` in SSE mode (once per minute)
- Adding retry logic or exponential backoff changes
- Modifying the `ConnectionState` FSM beyond what the spec requires

## Decisions

### 1. Branch on `StreamKind` inside `stream()`, not at the call site

`stream()` is a private method; the call sites are `run()` which already has `c.cfg.StreamKind`. The branch belongs where the URL is resolved — inside `stream()` — so all future callers automatically get the correct URL:

```go
var url string
if c.cfg.StreamKind == telemetryv1alpha1.StreamKindAlerts {
    url = c.cfg.Vendor.AlertSSEURL()
} else {
    url = c.cfg.Vendor.SSEURL()
}
```

Alternative considered: add a `URL()` method to `VendorConfig` that takes a `StreamKind`. Rejected — more code for a two-branch switch that is already local to `stream()`.

### 2. Add `StatusWriter push.StatusWriter` to `push.Options`

`push.AddTo` is the public constructor path for the push pipeline. Adding `StatusWriter` to `push.Options` keeps wiring explicit at the call site (`main.go`) with zero change to `Receiver` internals. `SetStatusWriter` already exists on `Receiver` for tests; `AddTo` calls it when the option is non-nil.

Alternative considered: pass `BMCReconciler` directly to `push.AddTo`. Rejected — would couple the push package to the controller package through a concrete type. `push.StatusWriter` interface is the right boundary.

### 3. Wire in `main.go` after both components are constructed

`bmcReconciler` is created before `push.AddTo` is called in `main.go`. Pass it as `push.Options.StatusWriter`. No ordering changes needed.

## Risks / Trade-offs

- **Alert stream URL change is live after restart**: BMCs that have their alerts SSE connected to the MetricReport URL will see that connection dropped and a new one opened to the correct alerts URL on next restart. Existing MetricReport data is unaffected; there is a brief gap in alerts coverage during reconnect. Backoff means the reconnect happens within seconds. Acceptable.

- **Push `StatusWriter` nil guard stays**: The nil check in `handleMetricReport` and `handleAlerts` remains. This is intentional — test setups that construct `Receiver` directly without wiring a `StatusWriter` continue to work.

- **Duplicate metric publishing eliminated**: Previously both SSE goroutines published MetricReport samples (since both connected to the MetricReport URL). After the fix only the metrics goroutine does. This is a silent correctness improvement — Prometheus `gauge` semantics mean duplicate samples were being overwritten, not doubled, but the extra load is removed.
