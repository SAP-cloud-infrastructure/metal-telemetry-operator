# Tasks

## 1. Port TestEventSink to MTO sink package

- [x] 1.1 Create `internal/sink/prometheus/testsink.go` by porting `TestEventSink` from MMO (two-gauge struct: `redfish_event_pipeline_test_last_result` GaugeVec and `redfish_event_pipeline_test_timestamp` GaugeVec, `RecordTestResult`, `Forget`); the constants `metricNamespace` and `labelHostname` already exist in `metricsink.go` — do not redefine them; verify `go test ./internal/sink/prometheus/` passes

## 2. Wire TestResultRecorder interface and recorder into SSE conn

- [x] 2.1 Define `TestResultRecorder` interface in `internal/sse/alert_wire.go` (`RecordTestResult(bmcName, result string)` only) alongside the existing `ConsumeIfTest` interface; add `TestRecorder TestResultRecorder` field to `ConnConfig` and `sseConn`; in `startConn`, assign `c.testRecorder = cfg.TestRecorder`; verify `go vet ./internal/sse/...`
- [x] 2.2 In `sseConn.handleAlertEvent` (the ConsumeIfTest loop), when `ConsumeIfTest` returns true and `c.testRecorder` is non-nil, call `c.testRecorder.RecordTestResult(c.cfg.BMCName, "success")`; add a test case to `conn_alert_test.go` that wires a mock `TestResultRecorder`, fires a test-event payload, and asserts `RecordTestResult("success")` was called; verify `go test ./internal/sse/` passes

## 3. Thread TestResultRecorder and late-wire TestFilter through ConnectionManager

- [x] 3.1 Add `testRecorder TestResultRecorder` field to `ConnectionManager`; update `NewConnectionManager` to accept `tr TestResultRecorder` as a new parameter after `tf ConsumeIfTest`; in `Ensure`, set `cfg.TestRecorder = m.testRecorder` alongside `cfg.TestFilter = m.testFilter`; add `SetTestFilter(tf ConsumeIfTest)` setter method (mutex-guarded) to `ConnectionManager` so the push reconciler can be wired after construction; update all callers of `NewConnectionManager` to pass the new parameter; verify `go build ./...` passes

## 4. Create TestEventSink in BMCReconciler and call Forget on deletion

- [x] 4.1 In `bmc_reconciler.go`, add a `testSink *prometheusSink.TestEventSink` field to `BMCReconciler`; in `SetupWithManager`, create the sink (`prometheusSink.NewTestEventSink(ctrlmetrics.Registry)`) alongside `AlertEventSink` creation; pass `r.testSink` to `NewConnectionManager` as the new `tr TestResultRecorder` parameter; in both deletion paths (IsNotFound and DeletionTimestamp/StateDisabled branches) call `r.testSink.Forget(...)` alongside the existing `r.sinkInst.Forget(...)` calls; add `SetSSETestFilter(tf sse.ConsumeIfTest)` method to `BMCReconciler` that calls `r.connMgr.SetTestFilter(tf)`; verify `go build ./...` passes

## 5. Wire TestRecorder and SSE TestFilter in push.AddTo and main.go

- [x] 5.1 Add `TestRecorder push.TestResultRecorder` input field and `ConsumeIfTestOut *push.ConsumeIfTest` output field to `push.Options` in `push.go`; in `AddTo`, set `reconcilerCfg.TestRecorder = opts.TestRecorder`; if `opts.ConsumeIfTestOut != nil`, set `*opts.ConsumeIfTestOut = reconciler` after creating it; verify `go vet ./internal/push/...` passes
- [x] 5.2 In `cmd/main.go`, pass `bmcReconciler.testSink` as `push.Options.TestRecorder`; capture the push reconciler via `ConsumeIfTestOut`; after `push.AddTo` returns, call `bmcReconciler.SetSSETestFilter(*opts.ConsumeIfTestOut)` to wire the SSE TestFilter; verify `go build ./...` and `make test` pass

## Workflow follow-up

- Run `openspec validate --change port-test-event-sink` before archiving.
- Archive the change after the project's review requirements are satisfied.
