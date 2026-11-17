# Proposal

## Why

The Pipeline Health section of the `bmc-alerts` dashboard is completely dark because `redfish_event_pipeline_test_timestamp` and `redfish_event_pipeline_test_last_result` — the two metrics it depends on — do not exist in MTO. MMO produced these via a `TestEventSink` struct and wired it to its subscription reconciler; that component was never ported when alert processing moved to MTO. The `ReconcilerConfig.TestRecorder` field and the call site (`RecordTestResult`) are already in MTO's push reconciler, but `TestRecorder` is wired as `nil`.

## What Changes

- Port `TestEventSink` from MMO (`internal/telemetry/sink/prometheus/testsink.go`) into MTO as `internal/sink/prometheus/testsink.go`, producing `redfish_event_pipeline_test_timestamp{hostname,result="success"}` and `redfish_event_pipeline_test_last_result{hostname}`.
- Create and wire a `TestEventSink` instance as `ReconcilerConfig.TestRecorder` in `internal/push/push.go` so the push reconciler's existing `RecordTestResult` call site becomes active.
- Wire `RecordTestResult` on the SSE alerts path: when `ConsumeIfTest` returns true in `internal/sse/conn.go`, call `RecordTestResult(bmcName, "success")` so SSE-mode BMCs also contribute to pipeline health. The comment at `bmc_reconciler.go:138` (`TestFilter: SSE heartbeat not yet implemented`) tracks this gap.
- Register `TestEventSink.Forget` wherever `EventSink.Forget` is called so both sinks stay in sync on BMC deletion.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `prometheus-metric-sink`: Two new Prometheus metrics emitted — `redfish_event_pipeline_test_timestamp` (Unix timestamp of last successful health-check per BMC) and `redfish_event_pipeline_test_last_result` (1 = success, 0 = failure). Both use label set `{hostname}`.
- `push-telemetry`: The system now records a pipeline health-check result (success or timeout) when a round-trip test event completes on the push path.

## Impact

- `internal/sink/prometheus/testsink.go` — new file
- `internal/push/push.go` — wire `TestRecorder`
- `internal/sse/conn.go` and `internal/sse/manager.go` — pass `TestResultRecorder` through and call `RecordTestResult` on test event consumption
- `internal/controller/bmc_reconciler.go` — pass `TestEventSink` to SSE connection manager; update the `nil` comment at line 138
- Dashboard Pipeline Health section becomes functional with no dashboard code changes
