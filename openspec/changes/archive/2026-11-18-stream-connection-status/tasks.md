# Tasks

## 1. Replace SubscriptionStatusSink with StreamConnectionSink

- [x] 1.1 In `internal/sink/prometheus/subscriptionsink.go`: replace `SubscriptionStatusSink`,
  `SubscriptionStatusRecorder`, `NewSubscriptionStatusSink`, `RecordSubscriptionStatus`, and
  `ForgetSubscriptionStatus` with `StreamConnectionSink`, `NewStreamConnectionSink`,
  `RecordStreamStatus(bmcName, streamType, reason string)`, and `ForgetStreamStatus(bmcName string)`.
  Track series by `streamKey{bmcName, streamType}` pair. Drop `vendor` / `model` from labels.
  Value is `1` when `reason == "connected"`, `0` otherwise. Atomic-swap per pair (delete old
  labels before writing new). Verify: `go build ./internal/sink/prometheus/...` succeeds.

- [x] 1.2 Add `internal/sink/prometheus/subscriptionsink_test.go` covering: initial record writes
  two series, reason change deletes old and writes new (per pair), ForgetStreamStatus removes
  both stream_type series, independent per-stream updates do not affect each other.
  Verify: `go test ./internal/sink/prometheus/...` passes.

## 2. Add StreamConnectionRecorder to SSE connection layer

- [x] 2.1 In `internal/sse/conn.go`: define `StreamConnectionRecorder` interface with
  `RecordStreamStatus(bmcName, streamType, reason string)`. Add `recorder StreamConnectionRecorder`
  field to `sseConn` struct and to `ConnConfig`. Add `streamTypeLabel(StreamKind) string` helper
  that returns `"metrics"` for `StreamKindMetrics` and `"alerts"` for `StreamKindAlerts`.
  Verify: `go build ./internal/sse/...` succeeds.

- [x] 2.2 In `internal/sse/conn.go` `run()`: alongside the `sw.SetConnected(false)` call at the
  reconnect point (line ~102), add `c.recorder.RecordStreamStatus(c.cfg.BMCName, streamTypeLabel(c.cfg.StreamKind), "disconnected")`.
  In `stream()`: alongside the `sw.SetConnected(true)` call on HTTP 200 (line ~147), add
  `c.recorder.RecordStreamStatus(c.cfg.BMCName, streamTypeLabel(c.cfg.StreamKind), "connected")`.
  Guard both with a nil check or use a no-op recorder. Verify: `go build ./internal/sse/...` succeeds.

- [x] 2.3 In `internal/sse/manager.go`: add `recorder StreamConnectionRecorder` field to
  `ConnectionManager`. Add it as a parameter to `NewConnectionManager` (after the existing params)
  and pass it through to `startConn` and into the `sseConn` struct.
  Verify: `go build ./internal/sse/...` succeeds.

## 3. Update BMC reconciler

- [x] 3.1 In `internal/controller/bmc_reconciler.go`: change the `statusSink` field type from
  `*prometheusSink.SubscriptionStatusSink` to `*prometheusSink.StreamConnectionSink`. Update
  the constructor call on line ~150 to `NewStreamConnectionSink`. Update the `StatusSink()`
  accessor return type. Pass `r.statusSink` as the `recorder StreamConnectionRecorder` argument
  when constructing the `ConnectionManager`.
  Verify: `go build ./internal/controller/...` succeeds.

- [x] 3.2 In `internal/controller/bmc_reconciler.go`: update the `probe_failed` call site (line ~266)
  to call `r.statusSink.RecordStreamStatus(bmc.Name, "metrics", "probe_failed")` and
  `r.statusSink.RecordStreamStatus(bmc.Name, "alerts", "probe_failed")`. Remove the `sse_mode`
  call site (line ~310) entirely. Update the `excluded` call site (line ~356) to call
  `RecordStreamStatus` twice, once for `"metrics"` and once for `"alerts"`. Update the
  `forgetBMC` call site to `r.statusSink.ForgetStreamStatus(name)`.
  Verify: `go build ./internal/controller/...` succeeds.

## 4. Update push reconciler

- [x] 4.1 In `internal/push/reconciler.go`: replace the `SubscriptionStatusRecorder` interface
  definition and field (`StatusRecorder SubscriptionStatusRecorder`) with
  `StreamConnectionRecorder` (method: `RecordStreamStatus(bmcName, streamType, reason string)`)
  and field `StatusRecorder StreamConnectionRecorder`.
  Verify: `go build ./internal/push/...` succeeds.

- [x] 4.2 In `internal/push/reconciler.go`: update the `slots_full` call site for MetricReport
  subscriptions (line ~255) to `r.StatusRecorder.RecordStreamStatus(bmc.Name, "metrics", "slots_full")`.
  Update the `slots_full` call site for alerts subscriptions (line ~271) to
  `r.StatusRecorder.RecordStreamStatus(bmc.Name, "alerts", "slots_full")`.
  Update the `subscribed` call site (line ~280) to two calls:
  `RecordStreamStatus(bmc.Name, "metrics", "connected")` and
  `RecordStreamStatus(bmc.Name, "alerts", "connected")`.
  Verify: `go build ./internal/push/...` succeeds.

- [x] 4.3 In `internal/push/push.go`: update the `StatusRecorder` field declaration to use the
  new `StreamConnectionRecorder` type (not the old `SubscriptionStatusRecorder`).
  Verify: `go build ./internal/push/...` succeeds.

## 5. Wire up in main.go

- [x] 5.1 In `cmd/main.go`: verify `bmcReconciler.StatusSink()` (line ~183) still satisfies the
  `push.StreamConnectionRecorder` interface after the type change. The type now returns
  `*prometheusSink.StreamConnectionSink`; confirm the call compiles without changes. If the
  return type reference changed, update the variable assignment.
  Verify: `go build ./cmd/...` succeeds.

## 6. Full build and tests

- [x] 6.1 Run `make build` from the repo root and confirm it exits 0 with no errors.

- [x] 6.2 Run `make test` (or `go test ./...`) and confirm all tests pass. Pay attention to
  any existing controller tests that may reference `SubscriptionStatusSink`,
  `RecordSubscriptionStatus`, or `ForgetSubscriptionStatus`.

## 7. Update README and dashboard

- [x] 7.1 In `README.md`: replace the `### redfish_bmc_event_subscription_status` section
  (lines ~137–172) with a new `### redfish_stream_connection_status` section documenting the
  new labels (`hostname`, `stream_type`, `reason`), the two-series-per-BMC semantics, all
  reason values and their descriptions, and example PromQL queries.
  Verify: section renders correctly in Markdown.

- [x] 7.2 In `dashboards/bmc-alerts/main.go`: update all references from
  `redfish_bmc_event_subscription_status` to `redfish_stream_connection_status`, adjusting
  label references (`vendor`, `model` removed; `stream_type` added). Update any
  `reason="subscribed"` or `reason="sse_mode"` filter values to use the new reason vocabulary.
  Verify: `go build ./dashboards/bmc-alerts/...` (or `make generate-dashboard`) succeeds.

## Workflow follow-up

- Archive the change after the implementation is reviewed and merged.
