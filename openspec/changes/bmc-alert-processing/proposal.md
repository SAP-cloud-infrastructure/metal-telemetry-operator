# Proposal

## Why

MTO currently discards all Redfish alert events: push mode stamps a timestamp on `{bmc}-alerts` BMCTelemetry but throws away event content; SSE mode silently drops every `#Event.*` frame. MMO has a full alert pipeline (disabled in QA production), making MTO the only operator receiving alert events and doing nothing with them. This change builds the complete alert processing pipeline in MTO so hardware faults surface as Server conditions, Kubernetes Events, and Prometheus metrics — and lays the foundation for eventually removing the duplicate pipeline from MMO.

## What Changes

- **Full alert event decode in push receiver**: replace the stub `RecordAlertEvent` call with a proper `wireEvent` decode (vendor variants `Events[]` / `Alerts[]`, Redfish v1.5 field renames `EventId`→`MessageId` / `Severity`→`MessageSeverity`, Dell `OriginOfCondition` plain-string quirk) and route decoded events to a new `EventSink`.
- **New `EventSink` interface** in `internal/sink/sink.go`: `PublishEvents(ctx, bmcName, []Event) error` + `Forget(bmcName)`, mirroring the existing `MetricReportSink`.
- **New Prometheus event sink** (`internal/sink/prometheus/eventsink.go`): `redfish_event_alert_total` counter `{hostname, severity, message_id, component}`, bounded FIFO dedup set per BMC (4096 entries), deferred-commit pattern to prevent double-counting on handler retry.
- **New `internal/alert/criticalevent/` package**: sets `CriticalEventReceived=True` condition on Servers matching `spec.bmcRef.name`, emits Kubernetes Warning Event, drives `redfish_server_critical_alert` gauge.
- **New `internal/alert/warningevent/` package**: emits Kubernetes Warning Event on Servers for Warning-severity alerts; no condition change.
- **Server field indexer**: register `spec.bmcRef.name` index on startup (MTO currently has no field indexers).
- **RBAC additions**: `servers` (get;list;watch), `servers/status` (get;patch), `core/v1 events` (create;patch).
- **Alert gauge prewarm**: on startup after cache sync, set `redfish_server_critical_alert` gauge=1 for any Server already carrying `CriticalEventReceived=True`, so the metric is correct immediately after a pod restart.
- **Test-event heartbeat**: push subscription reconciler fires `SubmitTestEvent` periodically (same cadence as MMO); a new `ConsumeIfTest` interface shared by the push receiver and SSE conn identifies in-flight test events and routes them to a health metric only — they must NOT update `LastEventTime`, increment `redfish_event_alert_total`, or reach the `EventSink`.
- **Spike task (auto-run after Phase 1)**: probe real QA BMCs (Dell, HPE, Lenovo) for SSE alert stream wire format to inform Phase 2 SSE design.
- **SSE alert stream routing (Phase 2, design deferred)**: update `internal/sse/conn.go` to route `#Event.*` frames to `EventSink` when `StreamKind == StreamKindAlerts`; apply `ConsumeIfTest` before routing; `LastEventTime` updated only for real hardware events.
- **`push-telemetry` spec updated**: alert event full decode + `EventSink` routing + test-event heartbeat + `LastEventTime` exclusion for test events.
- **`bmc-sse-collection` spec updated**: replace "discard non-MetricReport events" requirement with conditional routing of `#Event.*` frames to `EventSink` when `StreamKindAlerts`.

## Capabilities

### New Capabilities

- `bmc-alert-processing`: Full alert event pipeline — counter metrics, Server conditions, Kubernetes Events, severity routing (Critical/Warning/Clear), EventID deduplication, test-event heartbeat with `LastEventTime` exclusion.

### Modified Capabilities

- `push-telemetry`: Alert event decode is no longer a stub; `EventSink` routing, severity-based handler dispatch, and test-event filtering are new required behaviors.
- `bmc-sse-collection`: SSE alert stream processing is new required behavior; the existing "discard non-MetricReport events" requirement is replaced.

## Impact

- **`internal/push/receiver.go`**: replace stub alert handling with full `wireEvent` decode + `EventSink.PublishEvents` + `ConsumeIfTest` check.
- **`internal/sink/sink.go`**: add `EventSink` interface and `Event` struct.
- **`internal/sink/prometheus/eventsink.go`**: new file.
- **`internal/alert/criticalevent/`**: new package.
- **`internal/alert/warningevent/`**: new package.
- **`internal/push/reconciler.go`**: add `SubmitTestEvent` heartbeat + `ConsumeIfTest` tracking.
- **`internal/controller/bmc_reconciler.go`**: RBAC markers, Server field indexer registration, alert gauge prewarm, wire `EventSink` and `ConsumeIfTest` into push receiver and SSE conn.
- **`internal/sse/conn.go`**: Phase 2 — add `EventSink` and `ConsumeIfTest` params to `startConn`; route `#Event.*` frames.
- **Kubernetes RBAC**: new ClusterRole rules for `servers`, `servers/status`, `events` (regenerated from markers by `make generate`).
- No API (CRD) changes.
