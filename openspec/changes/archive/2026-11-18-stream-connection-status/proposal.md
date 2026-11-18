# Proposal

## Why

The existing `redfish_bmc_event_subscription_status` gauge treats every BMC as a single unit,
recording one series per host with reasons like `sse_mode` (not actionable) and `subscribed` (conflates
two independent streams). Operators cannot tell whether the metrics stream, the alerts stream, or both are
live — information that is essential when diagnosing dropped telemetry or missing alert notifications.
The gauge also carries `vendor` and `model` labels whose values are available via `kube_ironcore_bmc_info`
joins, adding cardinality with no benefit.

## What Changes

- **Replace** `redfish_bmc_event_subscription_status{hostname, vendor, model, reason}` with
  `redfish_stream_connection_status{hostname, stream_type, reason}`.
- **Two series per BMC**: one for `stream_type="metrics"`, one for `stream_type="alerts"`.
- **New reason vocabulary**: `connected` (value=1), `disconnected` (value=0),
  `slots_full` (value=0), `excluded` (value=0), `probe_failed` (value=0).
  The `subscribed` and `sse_mode` reasons are removed.
- **SSE integration**: hook into the SSE `StatusWriter.SetConnected` call path so each stream reports its
  live connection state independently. Previously only push mode had representation in this metric.
- **Push integration**: the push reconciler records per-stream-type status for `slots_full` (separate
  series for MetricReport vs alerts subscriptions) and `connected` (both streams when both subs active).
- Remove `vendor` and `model` labels; topology joins use `kube_ironcore_bmc_info` instead.
- Update README to document `redfish_stream_connection_status` and remove `redfish_bmc_event_subscription_status`.
- Update `dashboards/bmc-alerts/main.go` to query the new metric.

## Capabilities

### New Capabilities

_(none — all changes are modifications to an existing capability)_

### Modified Capabilities

- `bmc-subscription-status`: Replace `redfish_bmc_event_subscription_status` with
  `redfish_stream_connection_status`. Emit two series per BMC (one per stream type). Add SSE
  mode stream-connection tracking. Remove `vendor`/`model` labels and the `sse_mode`/`subscribed`
  reasons.

## Impact

- `internal/sink/prometheus/subscriptionsink.go` — replace `SubscriptionStatusSink` with `StreamConnectionSink`
- `internal/controller/bmc_reconciler.go` — update `statusSink` field and all call sites
- `internal/sse/manager.go` + `internal/sse/conn.go` — add `StreamConnectionRecorder` injection
- `internal/push/reconciler.go` — update `SubscriptionStatusRecorder` interface and call sites
- `README.md` — replace metric documentation section
- `dashboards/bmc-alerts/main.go` — update queries
- `openspec/specs/bmc-subscription-status/spec.md` — updated by delta spec sync
