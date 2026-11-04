# Design

## Context

The `metal-telemetry-operator` repo exists as a stub (LICENSE only). `metal-maintenance-operator` already ships a working `MetricReportSink` (`internal/telemetry/sink/prometheus/metricsink.go`) that tracks series by `(hostname, metric_id, type, unit, origin_context)` labels and implements a `Forget(bmcName)` method for stale-series cleanup. We adapt that pattern rather than reimplementing it.

The operator watches `metal.ironcore.dev/v1alpha1 BMC` resources managed by `metal-operator`. Credentials are read from `BMCSecret` resources in the same namespace (same name as the BMC). See proposal.md for motivation.

## Goals / Non-Goals

**Goals:**
- One SSE goroutine per SSE-capable BMC, bounded at startup by a semaphore
- `BMCTelemetry` CRD reflects per-BMC connection mode and health
- Prometheus gauge `redfish_telemetry_reading` (distinct from MMO's `redfish_monitor_reading`)
- Correct vendor-specific filter syntax applied at connect time
- Sentinel value 255 and empty `MetricValues` arrays handled before publishing

**Non-Goals:**
- Fallback polling or push-subscription for BMCs with no SSE URI (v1 deferred)
- Replacing or modifying MMO's existing telemetry pipeline
- Persisting historical metric time-series (Prometheus is the single series store)

## Decisions

### Decision 1: ConnectionManager owns all SSE goroutines

A single `ConnectionManager` struct (package `internal/sse`) owns a `sync.Map[bmcName → *sseConn]`. The reconciler calls `manager.Ensure(ctx, bmcName, config)` — a no-op when a healthy connection already exists; otherwise it cancels the old one and starts a new goroutine.

**Why:** Keeps the reconciler stateless and short. Multiple reconcile calls for the same BMC are idempotent.

**Alternative considered:** Store SSE goroutine lifecycle in the reconciler's Manager. Rejected — `Reconcile()` is called by controller-runtime from a work queue and must not block on I/O.

---

### Decision 2: Short reconciler loop — probe EventService, then delegate

`BMCReconciler.Reconcile()`:
1. Fetch `BMC` + `BMCSecret`. If deleted, call `manager.Stop(bmcName)` and `sink.Forget(bmcName)`.
2. If `BMCTelemetry` already has `status.mode == Unsupported`, skip EventService probe (no requeue).
3. If `BMCTelemetry.status.mode` is unset (first reconcile), probe `EventService` → write mode and SSE params to `BMCTelemetry.status`, set `SchemaDetected` condition.
4. Call `manager.Ensure(bmcName, sseConfig)` with the detected config.

The reconciler never blocks on SSE I/O. SSE goroutines update `BMCTelemetry.status` directly (via a status writer injected by the manager).

**Why:** controller-runtime reconcilers should be fast and re-entrant. Keeping SSE I/O in background goroutines matches how the boot-operator and other ironcore operators handle long-lived connections.

---

### Decision 3: Vendor detection at first reconcile only, cached in BMCTelemetry

`EventService` is probed once per BMC. The result (`mode`, `sseURI`, `filterSyntax`) is stored in `BMCTelemetry.status`. Subsequent reconciles skip the probe if `SchemaDetected` condition is `True`.

**Why:** EventService probe is a synchronous Redfish GET — unnecessary to repeat on every reconcile cycle. Storing in CRD status makes the detected config observable and survives pod restarts (no in-memory state needed).

**Alternative considered:** Cache probe result in-memory only. Rejected — cache is lost on pod restart and probing the entire fleet on startup amplifies load.

---

### Decision 4: SSE connection uses raw `net/http` with `bufio.Scanner`, no SSE library

SSE parsing is ~30 lines (scan lines, accumulate `data:` fields between blank lines, unmarshal JSON). Using a third-party SSE client library adds a dependency for trivial parsing.

Filter URL is constructed as:
- Unquoted: `<sseURI>?$filter=EventFormatType%20eq%20MetricReport`
- Quoted: `<sseURI>?$filter=EventFormatType%20eq%20%27MetricReport%27`

TLS: `InsecureSkipVerify: true` for v1 (matching MMO and boot-operator practice on internal BMC networks). Can be hardened later.

---

### Decision 5: Deduplication and sentinel filtering in the SSE consumer

The SSE consumer (`internal/sse/consumer.go`) applies these transformations before calling `sink.PublishSamples`:

1. **Latest-timestamp dedup**: iterate `MetricValues`, keep the entry with the most recent `Timestamp` per `(MetricId, MetricProperty)` key.
2. **Sentinel filter**: discard any `MetricValue` where the parsed `float64` equals `255.0`.
3. **Empty array guard**: if `MetricValues` is empty after filtering, skip the `PublishSamples` call.

**Why here rather than in the sink:** The sink interface (`sink.MetricReportSink`) is a shared abstraction used by MMO. Putting SSE-specific filtering in the consumer keeps the sink generic and reusable.

---

### Decision 6: Metric sink adapted from MMO with a different metric name

`internal/sink/prometheus/metricsink.go` is a near-copy of MMO's implementation, with `redfish_monitor_reading` renamed to `redfish_telemetry_reading`. The `MetricId` normalization (use `MetricId` if present, else extract fragment after `#/` from `MetricProperty`) lives in the consumer, which sets `Sample.MetricID` before calling `PublishSamples`. The sink itself requires `MetricID` to be non-empty (matching MMO's existing guard).

---

### Decision 7: BMCTelemetry status updates from goroutines via status writer

SSE goroutines receive a `statusWriter` interface (wrapping `client.Status().Update()`) injected at start time. Goroutines call it to update `lastEventTime` and `connectionState` without re-running the full reconcile loop.

**Why:** `Reconcile()` cannot be called directly from goroutines (no mechanism to enqueue with specific data). A thin status writer avoids the overhead of triggering a full reconcile just to bump a timestamp.

---

### Decision 8: Package layout

```
metal-telemetry-operator/
  cmd/
    main.go                    — kubebuilder entrypoint, wires manager + reconciler
  api/
    v1alpha1/
      bmctelemetry_types.go    — BMCTelemetry CRD spec/status structs
      groupversion_info.go
      zz_generated.deepcopy.go
  internal/
    controller/
      bmc_reconciler.go        — watches BMC, probes EventService, calls ConnectionManager
    sse/
      manager.go               — ConnectionManager (sync.Map + semaphore)
      conn.go                  — sseConn goroutine (HTTP GET, SSE parse loop, backoff)
      consumer.go              — MetricValues dedup, sentinel filter, sink.Sample conversion
      vendor.go                — EventService probe, vendor detection logic
    sink/
      sink.go                  — MetricReportSink interface (same as MMO)
      prometheus/
        metricsink.go          — redfish_telemetry_reading GaugeVec
  config/
    crd/bases/                 — generated BMCTelemetry CRD YAML
    rbac/                      — generated ClusterRole for BMC/BMCSecret/BMCTelemetry access
  Makefile
  Dockerfile
```

## Risks / Trade-offs

**Goroutine leak on pod shutdown → Mitigation:** `ConnectionManager` holds a root `context.Context` derived from the controller-manager's `context`. When the manager shuts down, all SSE goroutines receive cancellation via that context.

**BMCTelemetry status write contention → Mitigation:** `lastEventTime` updates are rate-limited (update only when time delta > 1 minute) to avoid continuous API server traffic on active BMCs.

**Lenovo 15-minute batch interval → Trade-off:** Prometheus scrapes may see stale values between batches. Acceptable — the gauge reflects the latest available reading from the BMC, not a real-time sensor poll. Alert rules should use `time() - timestamp(redfish_telemetry_reading{...})` to detect stale data.

**`InsecureSkipVerify: true` → Trade-off:** Required for the internal BMC network where BMC TLS certificates are self-signed. Acceptable in v1 on a closed management network; tracked for later hardening with a configurable CA bundle.

**EventService probe timeout → Mitigation:** Probe uses a short timeout (10s). If the probe fails, the BMC is requeued with exponential backoff rather than marked `Unsupported`.

## Migration Plan

1. Deploy `metal-telemetry-operator` alongside MMO (both can run simultaneously — SSE is a client-initiated pull, no conflict with push subscriptions).
2. Verify `redfish_telemetry_reading` metrics appear in Prometheus for the QA fleet (`m-qa-de-100`).
3. Once coverage is confirmed, MMO push subscriptions can be disabled per-cluster (separate MMO change, out of scope here).

Rollback: delete the `metal-telemetry-operator` Helm release. No CRD changes to other operators are required.

## Open Questions

- **Lenovo sentinel value scope**: Is `255` the only sentinel for Lenovo XCC3, or are there other "not available" magic values for specific metric types? Deferrable — add new sentinel values to the filter list as discovered in production.
- **Rate limit for `lastEventTime` status updates**: 1-minute threshold is an assumption. Can be tuned post-deployment based on observed API server load.
