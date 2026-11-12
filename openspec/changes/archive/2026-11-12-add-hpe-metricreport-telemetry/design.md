# Design

## Context

MTO already has:
- `internal/sse/` — SSE connection manager (`connMgr.Ensure`) and `ProbeEventService` / `detectVendor`
- `internal/controller/bmc_reconciler.go` — probes EventService, assigns `ModeSSE` or `ModeUnsupported`, starts SSE connections
- `internal/sink/` — `MetricReportSink` interface and Prometheus implementation

HPE iLO never sets `ServerSentEventUri`; the current `detectVendor` returns `ModeUnsupported` for all HPE hardware (see `vendor.go` Case 1). HPE does support Redfish EventDestination push subscriptions — the same mechanism MMO already uses in production (`internal/telemetry/runtime/`).

See proposal.md for motivation.

## Goals / Non-Goals

**Goals:**
- Add a plain HTTP push receiver and subscription reconciler for MetricReport-mode BMCs
- Extend mode selection in `bmc_reconciler` with ConfigMap rules, exclusion annotation, and per-BMC spec override
- Surface `MetricReport` and `Excluded` as first-class `TelemetryMode` values
- Adapt (not rewrite) the MMO push pipeline pattern to keep maintenance familiar

**Non-Goals:**
- Sharing code between MMO and MTO as a library (copy first, share later if the duplication proves painful)
- Multi-replica receiver HA (single-replica, observe first)
- HTTPS receiver (plain HTTP avoids iLO 7 TLS validation; revisit when needed)
- Modifying MMO itself

## Decisions

### New package: `internal/push/`

Adapt the MMO push pipeline into a new `internal/push/` package. The top-level entry point mirrors `runtime.AddTo(mgr, opts)`:

```
internal/push/
  push.go           // AddTo wiring function + Options struct
  receiver.go       // HTTP server; NeedLeaderElection() = false
  reconciler.go     // subscription BMCReconciler; standard leader election
  config.go         // ConfigMap types + live-reload loader
  exclude.go        // exclusion annotation + ConfigMap excludeHardware matcher
```

**Alternative considered:** Extract MMO's `internal/telemetry/` into a shared library. Rejected: it would couple two operator release cycles and the interfaces are still evolving. Copy now; share when both operators stabilize.

### Extend `sse.VendorConfig` with `ServiceEnabled`

Add `ServiceEnabled bool` to `VendorConfig` and update `eventServiceResponse` to decode `ServiceEnabled` from the EventService JSON. Update `detectVendor` Case 1 (no SSE URI): if `ServiceEnabled` is true, return `Mode: ModeMetricReport` (new constant in `internal/sse/vendor.go`); if false, return `ModeUnsupported` as today.

`ProbeEventService` already fetches `ServiceEnabled` implicitly; only the struct and the case logic need changing. Existing SSE tests stay unmodified — they don't set `ServiceEnabled`, which defaults to false, so Case 1 still returns `ModeUnsupported` for those fixtures.

**Alternative considered:** New probe function separate from `ProbeEventService`. Rejected: the information is already in the same HTTP response; a separate probe doubles the BMC API call.

### Mode selection priority chain in `bmc_reconciler`

Replace the current two-branch switch (`ModeSSE` → connMgr, `ModeUnsupported` → stop) with a four-step resolver:

```
1. annotation check  → Excluded (skip everything)
2. spec.ModeOverride → use as-is
3. ConfigMap rules   → vendor/model match
4. VendorConfig.Mode → probe result (SSE / MetricReport / Unsupported)
```

Only step 4 requires an outbound BMC call; steps 1–3 are in-memory lookups. The ConfigMap is live-reloaded via a `configloader.Loader` watch (same pattern as MMO's `runtime.AddTo`).

### Plain HTTP receiver

The receiver calls `http.ListenAndServe` (no TLS). iLO 7's `SecureStandard` policy validates the TLS certificate chain only when the EventDestination context is an HTTPS URL. An `http://` destination URL bypasses that check entirely, and MMO uses this in production today.

**Alternative considered:** HTTPS receiver with self-signed cert, passing the CA to the BMC during subscription creation. Rejected: iLO 7 has no API to register a custom CA for push subscriptions; the cert must chain to a public or pre-installed root. Plain HTTP is the correct solution, not a workaround.

### Receiver runs without leader election

The receiver's `Runnable` returns `NeedLeaderElection() = false` so it starts on all pods. The subscription reconciler (`push.BMCReconciler`) uses standard leader election. This matches MMO's design: POSTs are never lost because the receiving port is always bound; duplicate subscription creation on non-leaders is prevented because only the leader runs the reconciler.

**Alternative considered:** Fan-out from a single leader pod to all replicas. Rejected: it adds a hop and coordination layer for a single-replica deployment. Revisit if multi-replica is ever needed.

### ResourceRemoved fast re-subscription

When the receiver processes an alert event matching `ResourceRemoved`, it enqueues the BMC's `reconcile.Request` directly on the subscription reconciler's work queue via an internal channel or a `ctrl.Request` trigger (same mechanism MMO uses in `events/wire.go`). This avoids waiting for the default reconcile interval (typically 60s), which is longer than HPE's retry window.

### Single-replica HA config in Helm

Helm values add:
- `strategy: type: RollingUpdate, maxSurge: 1, maxUnavailable: 0` — new pod ready before old is removed
- `lifecycle.preStop: exec sleep 5` — drains in-flight requests before SIGTERM
- Readiness probe on receiver port — gates traffic routing
- `PodDisruptionBudget: minAvailable: 1`

This keeps mean outage to a few seconds during rolling updates. Node failure reschedule (60–120s) is the worst-case; the ResourceRemoved alert path provides fast re-subscription once the pod is back.

## Risks / Trade-offs

- **Subscription orphan on crash** — if the pod exits without deleting subscriptions, old destinations accumulate on the BMC. Mitigation: the subscription reconciler reconciles all existing subscriptions on startup, deleting ones with IDs it doesn't own.
- **HPE TerminateAfterRetries (90s window)** — if MTO is down longer than 90s, HPE kills subscriptions. Mitigation: the ResourceRemoved alert path re-subscribes immediately once the pod is back; the startup reconcile also catches any missed kills.
- **`ServiceEnabled` field not present on older iLO firmware** — missing field decodes as `false` in Go, so those BMCs land on `ModeUnsupported` (same as today). No regression.
- **Copied code diverges from MMO** — subscription logic in MMO may evolve. Mitigation: accept the divergence; the two operators have different lifecycles. If the gap grows painful, extract a shared library at that point.

## Migration Plan

No existing `BMCTelemetry` resources reference `MetricReport` or `Excluded` mode today (the enum only has `SSE` and `Unsupported`). Adding enum values is backward-compatible; existing resources are unaffected. The ConfigMap and Helm chart values are additive (new keys). No migration needed for HPE-managed clusters; MTO begins collecting from HPE BMCs automatically once deployed.

Rollback: remove the `--push-receiver-url` flag (or set it to empty); the reconciler short-circuits at mode selection and skips push subscription creation, leaving SSE-mode BMCs unaffected.

## Open Questions

None — all decisions that affect the spec or task breakdown are resolved above.
