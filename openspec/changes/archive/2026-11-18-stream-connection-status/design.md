# Design

## Context

See `proposal.md — Why` for motivation.

The codebase has two stream-connection event sources that need Prometheus recording:

1. **SSE connections** (`internal/sse/conn.go`): The `sseConn.run()` loop already calls
   `sw.SetConnected(ctx, bmcName, streamKind, true|false)` at the two critical moments —
   line 147 (HTTP 200 received) and line 102 (before reconnect backoff). This is the
   `StatusWriter` interface, currently injected into `ConnectionManager`.

2. **Push reconciler** (`internal/push/reconciler.go`): The `SubscriptionReconciler` calls
   `StatusRecorder.RecordSubscriptionStatus(bmcName, vendor, model, reason)` at three points:
   `slots_full` per subscription type (lines 255/271) and `subscribed` (line 280).

The existing `SubscriptionStatusSink` in `internal/sink/prometheus/subscriptionsink.go` handles
only the push-mode call path. There is no Prometheus recording in the SSE call path today.

## Goals / Non-Goals

**Goals:**
- Emit `redfish_stream_connection_status{hostname, stream_type, reason}` from both SSE and push code paths
- Two independent series per BMC (one per stream_type)
- Drop `vendor` / `model` labels and the `sse_mode` / `subscribed` reason vocabulary
- Remove `redfish_bmc_event_subscription_status` and all its set points

**Non-Goals:**
- Adding K8s CRD status fields for Prometheus-visible stream state (covered by `sse-stream-connection` spec)
- Changing SSE reconnect backoff behaviour or push subscription logic

## Decisions

### 1. Inject `StreamConnectionRecorder` into `ConnectionManager` alongside `StatusWriter`

`StatusWriter` targets the K8s BMCTelemetry resource. Prometheus recording is a separate concern
that should not be added to that interface. Instead, add a second injectable field
`streamRecorder StreamConnectionRecorder` to `ConnectionManager`, and pass it through to each
`sseConn` via `ConnConfig`.

In `conn.go`, alongside each `sw.SetConnected` call, add a matching recorder call:
```go
// line 102 (disconnect):
sw.SetConnected(ctx, bmcName, streamKind, false)
streamRecorder.RecordStreamStatus(bmcName, streamTypeLabel(streamKind), "disconnected")

// line 147 (connect):
sw.SetConnected(ctx, bmcName, streamKind, true)
streamRecorder.RecordStreamStatus(bmcName, streamTypeLabel(streamKind), "connected")
```

**Alternative considered**: Extend `StatusWriter` with a Prometheus method. Rejected: it couples
K8s and Prometheus concerns in one interface and pollutes the `BMCReconciler` which already
implements `StatusWriter`.

### 2. Define `StreamConnectionRecorder` interface in the `sse` package

The interface is small and only needs one method for the SSE call path:
```go
type StreamConnectionRecorder interface {
    RecordStreamStatus(bmcName, streamType, reason string)
}
```

Define it in `internal/sse/conn.go` (alongside `StatusWriter`). The `push` package defines
its own same-shaped interface independently; `StreamConnectionSink` satisfies both via Go's
structural typing without an import cycle.

**Alternative considered**: Define in a shared `sink` sub-package. Unnecessary indirection for a
two-method interface; Go's duck typing means no shared definition is needed.

### 3. Replace `SubscriptionStatusSink` with `StreamConnectionSink` in-place

Rename/replace the sink file. New implementation:

```go
type streamKey struct {
    bmcName    string
    streamType string  // "metrics" or "alerts"
}

type StreamConnectionSink struct {
    gauge   *prometheus.GaugeVec
    mu      sync.Mutex
    tracked map[streamKey]prometheus.Labels
}
```

Labels: `{hostname, stream_type, reason}` — drop `vendor` / `model`.

`RecordStreamStatus(bmcName, streamType, reason string)`:
- Look up old labels under `streamKey{bmcName, streamType}`
- If old labels differ from new → delete old series first
- Write new series with `value = 1` if `reason == "connected"`, else `0`
- Store new labels

`ForgetStreamStatus(bmcName string)`:
- Delete both `streamKey{bmcName, "metrics"}` and `streamKey{bmcName, "alerts"}`

### 4. BMC reconciler: set both streams for excluded / probe_failed

The BMC reconciler records `excluded` and `probe_failed` states independently of the SSE or push
code paths. Update both call sites to call `RecordStreamStatus` twice — once for `"metrics"`,
once for `"alerts"`.

Remove the `sse_mode` call site (line 310 of `bmc_reconciler.go`). In SSE mode, each stream now
self-reports its state when the connection goroutine starts. There is a brief window between
`connMgr.Ensure` and the first HTTP 200 where no series exists for that stream, which is
acceptable (absence means "not yet connected").

### 5. Push reconciler: per-stream recorder calls

Replace the single `SubscriptionStatusRecorder` interface with the new `StreamConnectionRecorder`
interface. Update call sites:
- `slots_full` for MetricReport → `RecordStreamStatus(bmcName, "metrics", "slots_full")`
- `slots_full` for alerts → `RecordStreamStatus(bmcName, "alerts", "slots_full")`
- Both subscriptions confirmed → `RecordStreamStatus(bmcName, "metrics", "connected")` + `RecordStreamStatus(bmcName, "alerts", "connected")`

### 6. Helper: `streamTypeLabel(StreamKind) string`

Define a package-level function in `subscriptionsink.go` (or a new `util.go` in the sink pkg):
```go
func streamTypeLabel(k telemetryv1alpha1.StreamKind) string {
    switch k {
    case telemetryv1alpha1.StreamKindMetrics: return "metrics"
    case telemetryv1alpha1.StreamKindAlerts:  return "alerts"
    default: return strings.ToLower(string(k))
    }
}
```

## Risks / Trade-offs

- **Brief gap for SSE streams**: Between `connMgr.Ensure` and the first HTTP 200, no series exists
  for that BMC's SSE streams. Dashboard panels using `absent()` would falsely trigger during this
  window (typically < 5s). Mitigation: document in README; do not add `absent()` alert rules
  without an appropriate wait window (e.g. `for: 1m`).

- **Atomic-swap correctness**: The `StreamConnectionSink` mutex must be held across both the
  delete and the write. The existing `SubscriptionStatusSink` already does this correctly; the
  new implementation inherits the same pattern.

- **Dashboard breakage during rollout**: Grafana panels querying `redfish_bmc_event_subscription_status`
  will show no data immediately after the operator upgrade. Mitigation: update `dashboards/bmc-alerts/main.go`
  in the same change so the dashboard is deployed alongside the operator.

## Migration Plan

1. Deploy updated operator image (drops `redfish_bmc_event_subscription_status`, emits `redfish_stream_connection_status`).
2. Deploy updated dashboard (queries `redfish_stream_connection_status`).
3. Any alerting rules that reference `redfish_bmc_event_subscription_status` must be updated
   before or alongside the operator upgrade. The `bmc-subscription-alerting` change was archived
   without deploying such rules, so there are no existing alert rules to migrate.
