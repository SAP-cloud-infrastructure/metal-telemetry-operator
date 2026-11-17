# Proposal

## Why

When a BMC's Redfish EventService is unreachable, the operator enters a runaway reconciliation loop that hammers the unreachable BMC at ~3,500 probe attempts per minute and consumes the operator's full CPU budget — observed in production against node006-bb098, which has been unreachable since 2026-11-16T18:16Z. The loop is caused by a missing idempotency guard in condition patching combined with the absence of exponential backoff on the probe path, an omission that contrasts with the well-designed backoff already present in the SSE stream reconnect layer.

## What Changes

- **Idempotent condition patch guard**: `setSchemaDetectedCondition` will skip the Kubernetes `Status().Patch()` call when the existing condition already carries the same `Status` and `Reason`, preventing a no-meaningful-change patch from triggering the `Owns(BMCTelemetry)` watch and re-enqueueing an immediate reconcile.
- **Error message normalisation**: The error string stored in the `SchemaDetected` condition will strip ephemeral TCP address/port data (e.g. `read tcp 10.42.0.207:55420->10.245.252.135:443`) so repeated failures produce a stable, human-readable message rather than a rotating one that defeats the patch guard.
- **Exponential probe backoff**: A thread-safe in-memory per-BMC failure counter (`sync.Map`) is added to `BMCReconciler`. Each consecutive probe failure doubles the `RequeueAfter` from an initial 5 min up to a cap of 30 min. The counter resets on any reconcile trigger so that credential rotation or a BMC spec change immediately retries.
- **Connected condition cleared on probe failure**: When the probe fails, `setConnectedCondition(false, "ProbeFailed")` is called on the BMCTelemetry resource. Prevents the stale `Connected: True` that persists when a BMC goes unreachable after a prior SSE session was active. The same idempotency guard applies — no patch is issued if the condition is already `False/ProbeFailed`.
- **Retry duration hint in condition message**: The normalised `SchemaDetected` error message appends the computed next-retry duration (e.g. `"EventService probe failed: connection reset by peer (next retry in ~20m)"`), giving operators backoff visibility without consulting controller logs.
- **`ProbeBackoff` ConnectionState enum value**: A new `ProbeBackoff` value is added to the `ConnectionState` type. During active probe backoff `status.connectionState` is set to `ProbeBackoff`, so `kubectl get bmctelemetry` reflects the actual state rather than the stale `Connected`.

## Capabilities

### New Capabilities

_(none)_

### Modified Capabilities

- `bmc-sse-collection`: Add a new requirement covering EventService probe failure recovery — idempotent condition updates, error message normalisation, and exponential backoff with a 30-minute cap and per-reconcile-trigger reset.

## Impact

- `internal/controller/bmc_reconciler.go`: new `probeFailures sync.Map` field; updated `probeEventServiceForBMC` (backoff calc, message normalisation with retry hint, Connected condition clear, connectionState update); updated `setSchemaDetectedCondition` (idempotency guard); new `setConnectedCondition` helper (idempotency guard).
- `api/v1alpha1/bmctelemetry_types.go`: new `ConnectionStateProbeBackoff` constant, updated kubebuilder validation marker to include `ProbeBackoff`. CRD manifests regenerated via `make generate manifests`. Minor backward-compatible CRD change — new enum value, no field removal or type change.
- `openspec/specs/bmc-sse-collection/spec.md`: new requirements added.
- No changes to other controllers or the SSE stream layer.
