# Tasks

## 1. Fix SSE alerts stream URL routing

- [x] 1.1 In `internal/sse/conn.go` `stream()` (line 102), replace `url := c.cfg.Vendor.SSEURL()` with a branch: `AlertSSEURL()` when `c.cfg.StreamKind == telemetryv1alpha1.StreamKindAlerts`, `SSEURL()` otherwise. Verify by adding a test to `internal/sse/manager_test.go` that runs a TLS `httptest` server recording request URLs, calls `Ensure` for both `StreamKindMetrics` and `StreamKindAlerts`, and asserts the two URLs differ (one contains `MetricReport`, the other contains `Event`).

## 2. Wire push StatusWriter

- [x] 2.1 Add `StatusWriter StatusWriter` field to `push.Options` in `internal/push/push.go`. In `AddTo`, after `NewReceiver`, call `receiver.SetStatusWriter(opts.StatusWriter)` when the field is non-nil. Verify by adding a test to `internal/push/push_test.go` using the existing `fakeStatusWriter` pattern: pass it via `push.Options.StatusWriter`, POST a MetricReport to the receiver, and assert `RecordMetricReport` was called.

- [x] 2.2 In `cmd/main.go`, pass `bmcReconciler` as `push.Options.StatusWriter` when calling `push.AddTo`. Verify `go build ./cmd/...` succeeds and the existing compile-time assertion `var _ push.StatusWriter = (*BMCReconciler)(nil)` in `bmc_reconciler.go` remains satisfied.

## Workflow follow-up

- Run `openspec validate --change fix-dual-stream-status-bugs` after all tasks are done.
- Archive the change once the PR is merged.
