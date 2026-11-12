# Proposal

## Why

When both SSE-mode and MetricReport-mode BMCs are present, the operator crashes at startup with `"duplicate metrics collector registration attempted"`. Both `BMCReconciler.SetupWithManager` and `push.AddTo` independently call `prometheusSink.New(ctrlmetrics.Registry)`, each trying to register the same `redfish_telemetry_reading` GaugeVec against the global registry. The second registration fails and the process exits.

## What Changes

- Create a single `MetricReportSink` instance in `main.go` before wiring any components.
- Add an injectable `Sink sink.MetricReportSink` field to `BMCReconciler`; `SetupWithManager` uses it when set instead of calling `prometheusSink.New` itself.
- Add a `Sink sink.MetricReportSink` field to `push.Options`; `push.AddTo` uses it when set instead of calling `prometheusSink.New` itself.
- Wire both components to the shared instance in `main.go`.

Sharing the instance also corrects a latent cleanup bug: when a BMC transitions between MetricReport and SSE modes, `Forget(bmcName)` called on one path now removes series written by the other path, preventing stale gauge series accumulation.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `push-telemetry`: Add a requirement that the push pipeline operates without Prometheus registration conflicts when SSE mode is also active (i.e., both collection paths use a shared metric sink).

## Impact

- `internal/controller/bmc_reconciler.go`: add `Sink` field; guard the `prometheusSink.New` call in `SetupWithManager` behind nil-check.
- `internal/push/push.go`: add `Sink` field to `Options`; guard the `prometheusSink.New` call in `AddTo` behind nil-check.
- `cmd/main.go`: create sink once, pass to both.
- All existing unit and integration tests remain valid; no API or CRD changes.
