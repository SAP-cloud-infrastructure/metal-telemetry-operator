# Design

## Context

See `proposal.md — Why` for motivation.

The BMC reconciler (`internal/controller/bmc_reconciler.go`) follows this path on every reconcile for an SSE-mode BMC:

```
Reconcile()
  -> resolveEarlyMode()           (no BMC contact)
  -> probeEventServiceForBMC()    (HTTP GET /redfish/v1/EventService)
      -> setSchemaDetectedCondition(false, err.Error())
          -> meta.SetStatusCondition()   // sets Message to raw error string
          -> r.Status().Patch()          // ALWAYS issues patch
      -> return RequeueAfter: 5*time.Minute
```

The controller is registered with `Owns(&telemetryv1alpha1.BMCTelemetry{})`. Any status patch to a `BMCTelemetry` resource immediately re-enqueues a reconcile for its owning `BMC`, bypassing the `RequeueAfter`. Because the raw error string contains an ephemeral source port (e.g. `read tcp 10.42.0.207:55420->...`), each call produces a distinct `Message`, so `meta.SetStatusCondition` considers it a change, and the patch is never a no-op.

The SSE stream layer (`internal/sse/conn.go`) already solves an analogous problem with exponential backoff (5s→5min, factor 2, ±25% jitter). The probe path has no equivalent.

## Goals / Non-Goals

**Goals:**
- Break the watch-triggered feedback loop so `RequeueAfter` is respected.
- Apply exponential probe backoff (5 min → 30 min cap) to reduce load from persistently unreachable BMCs.
- Produce stable, readable `SchemaDetected` condition messages that include the current backoff duration.
- Keep BMCTelemetry status coherent with actual BMC reachability: clear the `Connected` condition and set `connectionState=ProbeBackoff` when the probe fails, so `kubectl get bmctelemetry` reflects the true state.

**Non-Goals:**
- Persisting the failure counter across operator restarts (in-memory is sufficient; a fresh restart grants a few retries before backoff rebuilds, which is acceptable).
- Changing the SSE stream reconnect layer (already correct).
- Rate-limiting the probe at the HTTP client level.

## Decisions

### 1. Idempotency guard: skip patch when Status+Reason unchanged

Before calling `r.Status().Patch()` in `setSchemaDetectedCondition`, compare the computed `(Status, Reason)` pair against the existing condition. If they match, return early without patching.

**Why not check Message too?** After normalisation (Decision 3), the message is stable for the same failure class, so Status+Reason is sufficient. Checking Message would also suppress a patch when the message legitimately improves (e.g. after a firmware upgrade that changes the error text) — not a real problem but adds fragility for no gain.

**Alternative considered: predicate on Owns() watch to ignore status-only updates.** This would filter the watch trigger globally for all BMCTelemetry status changes, which risks suppressing useful reconcile triggers (e.g. an external tool writing status). The guard in `setSchemaDetectedCondition` is surgical: it only suppresses our own idempotent writes.

### 2. Per-BMC exponential backoff via in-memory sync.Map

Add a `probeFailures sync.Map` field to `BMCReconciler`, keyed by BMC name, value `int` (consecutive failure count). On each probe failure, increment the counter and compute:

```
requeue = min(5min * 2^(failures-1), 30min)
```

Reset the counter at the **start** of `probeEventServiceForBMC` (on any reconcile trigger), not only on success. This matches the stated requirement: any external change that triggers a reconcile (credential rotation, spec update) resets the backoff.

**Backoff table:**

| Failures | RequeueAfter |
|----------|-------------|
| 1        | 5 min       |
| 2        | 10 min      |
| 3        | 20 min      |
| 4+       | 30 min      |

**Why in-memory over status field?** A status field would require an additional patch on every reconcile (to write the counter), re-introducing patch traffic. In-memory is zero-overhead and aligns with how controller-runtime recommends handling rate-limiting state. The cost of a restart resetting the counter is negligible: the operator retries once, then immediately backs off again if the BMC is still unreachable.

**Why not use controller-runtime's built-in `RateLimiter`?** The default work queue rate limiter applies per-item exponential backoff, but it is reset by any re-enqueue (including from the `Owns` watch), which is exactly the trigger that causes the storm. We need the counter to reset on any trigger, and the requeue duration to be computed per-trigger from the counter — this is cleaner expressed explicitly than by tuning the work queue limiter.

### 3. Error message normalisation via regex stripping

Before passing `err.Error()` to `setSchemaDetectedCondition`, strip patterns matching ephemeral TCP address/port data. A single regexp covers the Go net package's standard format:

```
read tcp <src_addr>:<src_port>-><dst_addr>:<dst_port>: <error>
    →  <error>
```

and the wrapping `Get "https://..."` prefix added by `net/http`:

```
GET EventService: Get "https://<host>/redfish/v1/EventService": <error>
    →  GET EventService: <error>
```

The message also appends the computed next-retry duration as a human-readable suffix: `(next retry in ~20m)`. The `~` prefix signals an approximate round value, not a precise deadline. The duration is rounded to the nearest minute.

This produces stable messages like `"EventService probe failed: connection reset by peer (next retry in ~20m)"`.

**Where to apply:** A small `normaliseProbeError(err error) string` helper in `bmc_reconciler.go`, called at the single point where the error becomes a condition message. The `requeue` duration computed in `probeEventServiceForBMC` is threaded into the helper (or appended inline) before passing to `setSchemaDetectedCondition`.

**Alternative considered: wrapping errors with a stable type.** `sse.ProbeEventService` could return a typed error that carries a stable message separately from the underlying error chain. This is cleaner architecturally but requires changes to the probe API surface and tests. The normalisation helper is contained entirely within the reconciler and requires no API changes.

### 4. Clear Connected condition on probe failure

When `probeEventServiceForBMC` returns a non-nil error, call `setConnectedCondition(ctx, tel, false, "ProbeFailed")` before returning. This uses the same idempotency guard pattern as `setSchemaDetectedCondition`: if the condition is already `False/ProbeFailed`, no patch is issued.

**Why the probe path and not the SSE goroutine?** The SSE `run()` goroutine calls `SetConnected(false)` when the stream loop exits non-cleanly. However, when `StopBMC` cancels the context, the goroutine exits via the `ctx.Err() != nil` early-return path without calling `SetConnected(false)`, leaving the condition stale. The probe path is called on every reconcile and is therefore the reliable place to clear the condition when the BMC is unreachable.

**Scope:** The probe path only sets `Connected: False`. It does not set `Connected: True` — that remains the SSE stream layer's responsibility, which sets it when a stream is established.

### 5. ProbeBackoff ConnectionState (minor CRD change)

Set `tel.Status.ConnectionState = ConnectionStateProbeBackoff` in the same probe-failure code path as Decision 4. This makes `kubectl get bmctelemetry` show `ProbeBackoff` in the State column instead of the stale `Connected`.

**CRD change required:**
1. Add `ConnectionStateProbeBackoff ConnectionState = "ProbeBackoff"` constant in `api/v1alpha1/bmctelemetry_types.go`.
2. Update the kubebuilder marker: `// +kubebuilder:validation:Enum=Connected;Reconnecting;Failed;SubscriptionActive;SlotsFull;ProbeBackoff`.
3. Run `make generate manifests` to regenerate CRD YAML.

**Backward compatibility:** Adding a new enum value is backward-compatible. No existing BMCTelemetry resource holds `ProbeBackoff`; no controller reads this field to branch on. The API server accepts the new value without a migration.

**Alternative considered: reuse `Reconnecting`.** `Reconnecting` semantically covers the SSE stream reconnect loop, not the probe backoff path. Reusing it would conflate two distinct states and make it impossible to distinguish "SSE stream dropped, reconnecting" from "BMC unreachable at probe level, backing off". A distinct value preserves that signal.

## Risks / Trade-offs

- **Backoff counter reset on any trigger is intentional but subtle.** A high-frequency watch (e.g. another controller updating the BMC object repeatedly) could prevent backoff from building up. In practice, BMC objects are low-churn; this is an acceptable trade-off for correct credential-rotation behaviour.
- **In-memory counter lost on pod restart.** After a restart the operator will probe all BMCs once before backoff resumes. For a fleet of N unreachable BMCs this is N probes at startup — bounded, and much better than the current unbounded loop.
- **Normalisation regex must not over-strip.** A too-broad regex could erase useful context from non-TCP errors (e.g. HTTP 401, TLS cert errors). The regex MUST be anchored to the specific `read tcp <addr>:<port>-><addr>:<port>:` prefix and should not match other error shapes.
- **Connected condition cleared by probe, not restored by probe.** The probe sets `Connected: False` on failure but does not set it to `True` on success — that remains the SSE layer's job. If the probe succeeds but the SSE stream never establishes (e.g. a firewall blocks the event stream), `Connected` will remain `False`. This is correct behaviour: `Connected` reflects stream state, not probe state.
- **ProbeBackoff CRD change requires a manifest update.** Running `make generate manifests` is required before deployment. Skipping it means the API server rejects the new enum value. The task list makes this explicit as a prerequisite step.
