# Proposal

## Why

Metal Maintenance Operator (MMO) currently collects BMC telemetry via Redfish push subscriptions, but this architecture is wrong for the task: telemetry data floods MMO logs, conflicts with its maintenance-focused purpose, and push subscriptions require a receiver endpoint that complicates multi-pod deployments. A dedicated telemetry operator using Redfish SSE (Server-Sent Events) eliminates these problems — the operator pulls the stream rather than receiving pushes, removing the receiver endpoint entirely and keeping MMO's logs clean.

## What Changes

- **New operator**: `metal-telemetry-operator` — a standalone kubebuilder operator that watches `BMC` resources, detects SSE capability per BMC, maintains persistent SSE connections, and exposes Prometheus metrics.
- **New CRD**: `BMCTelemetry` — per-BMC status resource tracking connection mode, health, last event time, filter syntax variant, and reconnect count.
- **New Helm chart**: deployable via `foundation-charts`, following the same pattern as the MMO chart.
- **SSE connection manager**: goroutine-per-BMC connection pool with bounded concurrency (semaphore) and startup jitter to prevent thundering herd at scale.
- **Prometheus metrics**: `GaugeVec` exposing BMC sensor readings with labels `hostname`, `metric_id`, `type`, `unit`, `origin_context`. Sentinel value 255 (Lenovo "not available") is filtered before publication.
- **Vendor-adaptive filter syntax**: operator detects the correct SSE filter syntax at first reconcile and stores it in `BMCTelemetry.status.filterSyntax`.

## Capabilities

### New Capabilities

- `bmc-sse-collection`: Operator connects to BMC SSE streams, detects vendor-specific filter syntax, and delivers `#MetricReport` events to an internal sink. Covers the full lifecycle: detect → connect → stream → reconnect on failure.
- `bmc-telemetry-status`: `BMCTelemetry` CRD tracks per-BMC connection state, SSE URI, detected filter syntax, last event time, and reconnect count. One resource per BMC, owned by the operator.
- `prometheus-metric-sink`: MetricReport events are normalized and published as Prometheus gauges. Batched historical samples are deduplicated to the latest value per metric key. Stale metrics are removed when a BMC is deleted.

### Modified Capabilities

*(none — this is a new operator)*

## Impact

- **New repository**: `metal-telemetry-operator` (already exists as a stub at `/workspaces/foundation/metal-telemetry-operator`)
- **Dependency on `metal-operator`**: watches `metal.ironcore.dev/v1alpha1 BMC` and `BMCSecret` resources; reads BMC endpoint and credentials from them
- **Prometheus**: new metric series `redfish_telemetry_reading{hostname, metric_id, type, unit, origin_context}` — distinct name from MMO's `redfish_monitor_reading` to allow both to coexist during rollout
- **No MMO changes in v1**: MMO push subscriptions continue unchanged; telemetry operator runs alongside MMO with no conflict (SSE is a client-initiated pull, not a registered subscriber)
- **Fleet coverage (verified in m-qa-de-100)**:
  - Dell iDRAC10 (17G): SSE MetricReport ✓
  - Dell iDRAC9 (16G): SSE MetricReport ✓
  - Lenovo XCC3: SSE MetricReport ✓
  - HPE iLO 6/7, Fsas iRMC S6, BULL OpenBMC: no SSE URI — operator marks these `Unsupported`, no action taken (fallback modes out of scope for v1)
