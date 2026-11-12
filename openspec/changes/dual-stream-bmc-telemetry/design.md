# Design

## Context

See proposal.md — Why for motivation. Relevant current-state facts:

- `defaultClientFactory.NewClient()` creates one `http.Transport` per reconcile with keep-alives enabled. A single `ensureSubscriptions` call issues 1 GET (collection) + N GETs (members) + up to 2 POSTs over the same connection. HPE iLO 6 closes keep-alive connections after ~4 requests; Go never retries a non-idempotent POST on a dead connection.
- `CreateEventSubscription` returns a generic error string on non-2xx; callers cannot distinguish a 409 slot-exhaustion from a transient error.
- One `BMCTelemetry` resource per BMC is created by `ensureTelemetry()` at `Name: bmc.Name`. All status writers (`SetConnected`, `IncrReconnectCount`, `SetLastEventTime`) look up by bare BMC name.
- `sse.ConnConfig` holds `BMCName` but no stream kind; `ConnectionManager` stores one `*sseConn` per BMC name. `StatusWriter` interface methods take only `(ctx, bmcName)`.
- `sse.VendorConfig.SSEURL()` hard-codes the MetricReport filter. No `AlertSSEURL` method exists.
- `push.StatusWriter` exposes only `RecordMetricReport`; the alerts receiver handler calls `OnResourceRemoved` but does not update `LastEventTime`.

## Goals / Non-Goals

**Goals:**
- Eliminate EOF-on-POST by disabling keep-alives on the Redfish HTTP transport.
- Surface BMC subscription slot exhaustion as a stable, user-visible `SlotsFull` state with backoff.
- Two independently-tracked `BMCTelemetry` resources per BMC — one per monitored stream.
- Symmetric two-stream model across both modes: SSE gets two SSE connections (different `EventFormatType` filter); MetricReport gets two EventDestination subscriptions.
- Alert activity (non-ResourceRemoved POSTs) updates `LastEventTime` on the alerts stream resource.

**Non-Goals:**
- Changing mode detection or precedence logic.
- Supporting more than two streams per BMC.
- Tracking raw subscription slot counts or predicting when a slot frees.
- In-place migration of the old single `BMCTelemetry` resource (controller replaces it on first reconcile).

## Decisions

### D1 — DisableKeepAlives on Redfish HTTP transport

Add `DisableKeepAlives: true` to the `http.Transport` in `defaultClientFactory.NewClient()`.

**Why not retry logic?** Retrying non-idempotent POSTs risks duplicate subscriptions. A fresh connection per request is minimal and sufficient — the reconciler runs on a minutes cadence, so connection overhead is irrelevant.

### D2 — Typed sentinel error for slot exhaustion

`CreateEventSubscription` parses the response body on HTTP 409 and returns `var ErrSubscriptionSlotsFull` when the extracted `MessageId` contains `"ResourceInUse"` or `"EventSubscriptionLimitExceeded"`. Callers use `errors.Is`.

**Why not just check the status code?** 409 is generic — it also appears for duplicate destinations. Inspecting `MessageId` distinguishes slot exhaustion from other conflicts and handles both iLO 6 (`ResourceInUse`) and iLO 7 (`EventSubscriptionLimitExceeded`) in one check.

### D3 — Naming: `{bmc-name}-metrics` and `{bmc-name}-alerts`

Both resources are owned (controller reference) by the BMC object. `ensureTelemetry` creates both; `deleteOwnedTelemetry` deletes both. Name-based lookup is O(1) via the API server name index.

**Breaking change**: existing `{bmc-name}` resources are deleted on first reconcile after upgrade.

### D4 — StreamKind in spec, treated as immutable

`BMCTelemetrySpec.StreamKind` is set at creation and never changed by the controller. Callers can query a stream resource by its deterministic name and confirm the kind programmatically without string-splitting.

### D5 — ConnectionManager keyed by `{bmcName, streamKind}`

The internal map key changes from `string` to a `connKey{bmcName string; streamKind StreamKind}` struct. Public API:
- `Ensure(ctx, ConnConfig)` — start or reconcile a single stream connection.
- `Stop(bmcName, streamKind)` — stop one stream's connection.
- `StopAll(bmcName)` — stop both streams (used on BMC deletion).

`ConnConfig` gains a `StreamKind` field. One manager holds the `MaxConcurrentConnects` semaphore across all streams.

**Why not two managers?** Splitting would either double-count against the concurrency limit or require shared state between managers.

### D6 — StatusWriter interface gains a streamKind parameter

All methods in `sse.StatusWriter` and `push.StatusWriter` gain a `streamKind telemetryv1alpha1.StreamKind` parameter. `BMCReconciler` implements both and routes each call to `{bmcName}-metrics` or `{bmcName}-alerts` accordingly.

**Why not pass the full resource name?** Keeps suffix construction in one place (the reconciler's methods) rather than spread across every call site.

### D7 — AlertSSEURL method on VendorConfig

`VendorConfig.AlertSSEURL()` mirrors `SSEURL()` with `EventFormatType eq Event` instead of `MetricReport`. Quoted vs. unquoted encoding follows the same `FilterSyntax` switch already in `SSEURL()`.

### D8 — SlotsFull requeue after 10 minutes

On `errors.Is(err, ErrSubscriptionSlotsFull)`, the reconciler sets `ConnectionState: SlotsFull` on the stream resource and returns `ctrl.Result{RequeueAfter: 10 * time.Minute}, nil`. This suppresses error-rate noise while allowing recovery when an external actor frees a slot.

## Risks / Trade-offs

- **Breaking rename** — Old `{bmc-name}` resource is deleted on first reconcile after upgrade. Monitoring or GitOps referencing the bare name will see a gap. Mitigation: release notes; resources are discoverable immediately via label selector `metal.ironcore.dev/bmc={bmc-name}`.
- **SlotsFull masks non-slot 409s** — A destination-already-registered 409 has a different `MessageId` and falls through to the generic error path, so this is not a concern in practice.
- **Two SSE connections per SSE-mode BMC** — Doubles open HTTP streams to the BMC. No per-client SSE limit is documented in Redfish; iLO 6/7 support multiple concurrent SSE clients.
- **ModeOverride lives on both resources** — Since mode is BMC-level, only `{bmc}-metrics` is read for `ModeOverride`; `{bmc}-alerts` carries a copy but it is ignored. Mitigation: document that overrides must be set on `-metrics`; enforce later with a webhook if needed.

## Migration Plan

1. Deploy updated operator.
2. On the next reconcile for each BMC: controller deletes the old `{bmc-name}` BMCTelemetry and creates `{bmc-name}-metrics` + `{bmc-name}-alerts`.
3. No manual step required; the controller self-heals.
4. **Rollback**: downgrade restores single-resource creation. The two stream resources become orphans (no matching owner controller) and can be pruned with `kubectl delete bmctelemetry -l metal.ironcore.dev/bmc={bmc-name}`.

## Open Questions

- Should `ModeOverride` on `{bmc}-alerts` be accepted and silently ignored, or rejected by a validation webhook? Deferrable — the functional behaviour (read from `-metrics`) is correct either way.
