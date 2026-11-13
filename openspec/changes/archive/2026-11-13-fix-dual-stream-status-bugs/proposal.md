# Proposal

## Why

Two bugs introduced during `dual-stream-bmc-telemetry` cause BMCTelemetry status to be silently incorrect in production: MetricReport-mode BMCs never populate `LastEventTime` (the `StatusWriter` is not wired into the push receiver), and SSE-mode BMCs show `Reconnecting` instead of `Connected` because both the metrics and alerts SSE goroutines connect to the same MetricReport-filtered URL.

## What Changes

- **SSE stream URL routing**: `conn.go`'s `stream()` function always calls `c.cfg.Vendor.SSEURL()` regardless of `StreamKind`. `AlertSSEURL()` exists in `vendor.go` but is never called. Fix: branch on `cfg.StreamKind` in `stream()` so the alerts goroutine uses `AlertSSEURL()` and the metrics goroutine uses `SSEURL()`.

- **Push `StatusWriter` wiring**: `push.AddTo` creates the `Receiver` without a `StatusWriter`, so `handleMetricReport`'s `r.cfg.StatusWriter.RecordMetricReport()` call is a no-op guarded by a nil check. `BMCReconciler` correctly implements `push.StatusWriter` but is never passed to the receiver. Fix: add `StatusWriter push.StatusWriter` to `push.Options` and wire `bmcReconciler` from `main.go`.

## Capabilities

### New Capabilities

- `sse-stream-connection`: SSE dual-stream connection behavior — which URL each stream kind must use, and when `ConnectionState` transitions occur.

### Modified Capabilities

- `push-telemetry`: Clarify that `LastEventTime` update (requirement: "BMCTelemetry status updated on push activity") requires the `StatusWriter` to be wired to the push receiver. The receiver MUST have a `StatusWriter`; without it the requirement cannot be satisfied.

## Impact

- `internal/sse/conn.go`: `stream()` — branch on `StreamKind` to select the correct SSE URL
- `internal/push/push.go`: `push.Options` — add `StatusWriter` field; `AddTo` — wire it to `NewReceiver`
- `cmd/main.go`: pass `bmcReconciler` as `push.Options.StatusWriter`
- No API surface changes; no CRD changes; no breaking changes
