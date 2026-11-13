# Proposal

## Why

Two production bugs are causing MTO to silently fail telemetry setup on HPE iLO 6 BMCs: HTTP keep-alive exhaustion causes a POST to return EOF before the subscription is created, and when BMC subscription slots are already full a 409 is returned but neither surfaced to the operator nor visible to the user. Beyond the immediate fixes, the single `BMCTelemetry` resource per BMC makes it impossible for operators to distinguish metrics stream health from alerts stream health at a glance; both streams are collapsed into one status field with no per-stream visibility.

## What Changes

- **Fix keep-alive exhaustion on POST**: add `DisableKeepAlives: true` to the Redfish HTTP transport so each request gets a fresh TCP connection, eliminating EOF on the 5th request in a reconcile cycle.
- **Detect and surface slot exhaustion**: detect HTTP 409 `ResourceInUse` / `EventSubscriptionLimitExceeded` in `CreateEventSubscription`, return a typed sentinel error, and set a new `SlotsFull` connection state on the affected stream resource with a 10-minute requeue backoff instead of failing hard.
- **Dual-stream BMCTelemetry layout**: replace the single `{bmc-name}` resource with two resources per BMC — `{bmc-name}-metrics` and `{bmc-name}-alerts` — each carrying its own `StreamKind`, `ConnectionState`, and `LastEventTime`.
- **StreamKind field**: add `StreamKind: Metrics | Alerts` to `BMCTelemetrySpec` so each resource identifies which stream it tracks.
- **Extended ConnectionState enum**: add `SubscriptionActive` (push mode: subscription present and receiving) and `SlotsFull` (BMC has no free subscription slots) alongside the existing `Connected`, `Reconnecting`, and `Failed` states.
- **Alerts SSE stream**: for SSE-mode BMCs, open a second SSE connection to the same URI filtered by `EventFormatType eq Event` (alongside the existing MetricReport-filtered connection), updating `{bmc-name}-alerts` on activity.
- **RecordAlertEvent status path**: extend the push `StatusWriter` interface with `RecordAlertEvent` so incoming alert POSTs update `{bmc-name}-alerts.LastEventTime`.
- **Docs**: add iLO 6 subscription slot limit (3, same as iLO 7) and its 409 `ResourceInUse` response to `baremetal-workspace/redfish/vendors/hpe-ilo.md`.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `push-telemetry`: new requirements for dual-stream subscription management, slot-exhaustion handling, alert-event status recording, and `RecordAlertEvent` interface method.
- `telemetry-mode`: new requirements for `StreamKind` field in `BMCTelemetrySpec`, dual-resource layout per BMC, extended `ConnectionState` enum values, and per-stream `LastEventTime` and `ConnectionState` tracking.

## Impact

- **API**: `BMCTelemetrySpec` gains `StreamKind`; `ConnectionState` enum gains `SubscriptionActive` and `SlotsFull`. **BREAKING** for existing BMCTelemetry resources: the controller will delete the old single resource and create two new ones on first reconcile after upgrade; any external tooling reading BMCTelemetry by the bare BMC name will need to be updated to `{bmc-name}-metrics` / `{bmc-name}-alerts`.
- **CRD**: `+kubebuilder:printcolumn` updated to show Stream column.
- **internal/push/reconciler.go**: `defaultClientFactory`, `CreateEventSubscription`, `ensureSubscriptions` (ErrSubscriptionSlotsFull, per-stream status updates).
- **internal/push/receiver.go**: `StatusWriter` interface gains `RecordAlertEvent`.
- **internal/sse/vendor.go**: new `AlertSSEURL()` method.
- **internal/sse/conn.go + manager**: `ConnConfig.StreamKind`, `StatusWriter` interface gains stream kind param, `ConnectionManager` tracks two independent connections per BMC.
- **internal/controller/bmc_reconciler.go**: `ensureTelemetry`, `deleteOwnedTelemetry`, `SetConnected`, `IncrReconnectCount`, `SetLastEventTime` all updated for dual-resource model.
- **baremetal-workspace/redfish/vendors/hpe-ilo.md**: documentation update (no code impact).
