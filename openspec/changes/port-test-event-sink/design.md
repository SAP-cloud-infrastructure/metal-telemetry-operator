# Design

## Context

MTO's push reconciler already sends `SubmitTestEvent` heartbeats and implements `ConsumeIfTest` to match returns. The `ReconcilerConfig.TestRecorder` field is defined but wired as `nil` — so `RecordTestResult` is never called and the two pipeline health metrics (`redfish_event_pipeline_test_timestamp`, `redfish_event_pipeline_test_last_result`) are never emitted. The SSE path has a matching gap: `ConnConfig.TestFilter` and `ConsumeIfTest` are wired, but no recorder is passed through. The BMC deletion path calls `EventSink.Forget` but not any test recorder Forget.

Additionally, the `SubmitTestEvent` payload is incomplete: only `{"MessageId": ...}` is sent, which HPE iLO 7 rejects with 400 (`ActionParameterMissing`). This fix was already applied in the bmc-alert-processing task 9.4 investigation.

## Goals / Non-Goals

**Goals:** Make `redfish_event_pipeline_test_timestamp` and `redfish_event_pipeline_test_last_result` appear in metrics output so the Pipeline Health dashboard section becomes functional.

**Non-goals:** Changing the heartbeat cadence, timeout, or the messageId scheme; changing the dashboard code; changing the `SubmitTestEvent` action URL.

## Decisions

### Port TestEventSink directly from MMO rather than rebuilding

MMO's `TestEventSink` is a simple two-gauge struct (`RecordTestResult`, `Forget`). Porting it avoids design drift and makes the metric names identical, so existing Grafana queries (which reference `redfish_event_pipeline_test_timestamp` and `redfish_event_pipeline_test_last_result`) work without dashboard changes.

Alternative: build a generic `PipelineHealthRecorder` interface and defer the Prometheus implementation. Rejected — the dashboard already has specific metric names, so a different naming scheme only adds churn.

### Wire `TestEventSink` at `push.AddTo` time, not at reconciler init

`push.AddTo` in `push.go` is where the shared `EventSink` is already wired. Creating the `TestEventSink` in the same `AddTo` call keeps construction co-located and avoids threading another field through `BMCReconciler`.

### Use a `TestResultRecorder` interface in sse/conn.go

The SSE package should not import `internal/sink/prometheus`. A minimal interface (`RecordTestResult(bmcName, result string)`) is defined in `internal/sse/conn.go` (or `internal/sink/sink.go`) so the SSE package depends on behavior, not the concrete type.

## Risks / Trade-offs

- **TestEventSink Forget not called on SSE-only BMCs** — BMCs in SSE mode never enter the push reconciler's `Forget` path. The `bmc_reconciler.go` deletion handler must call `TestEventSink.Forget` directly alongside `EventSink.Forget`. Low risk: the deletion handler already has this pattern.

## Migration Plan

No data migration required. The new gauge series appear the first time a round-trip completes after deployment. The Pipeline Health dashboard panels use `or vector(0)` guards so they display zero rather than "No data" before any round-trips complete.

## Open Questions

None — all decisions are resolved.
