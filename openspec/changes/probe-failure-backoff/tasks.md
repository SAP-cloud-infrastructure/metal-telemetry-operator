# Tasks

## 1. Error message normalisation helper

- [x] 1.1 Add `normaliseProbeError(err error) string` to `internal/controller/bmc_reconciler.go`. The function strips `read tcp <src>:<port>-><dst>:<port>: ` prefixes and the `Get "https://…": ` wrapper added by `net/http`, returning a stable human-readable error class. Verify: unit test `TestNormaliseProbeError` covers connection-reset, i/o timeout, and a plain HTTP 401 (must pass through unchanged); run `go test ./internal/controller/...`.

- [x] 1.2 Thread `normaliseProbeError` into `probeEventServiceForBMC`: replace the raw `err.Error()` passed to `setSchemaDetectedCondition` with `normaliseProbeError(err)`. Also append the computed `requeue` duration to the message using a helper such as `formatProbeMessage(msg string, requeue time.Duration) string` that appends `(next retry in ~Xm)`. Verify: existing reconciler tests continue to pass (`go test ./internal/controller/...`); a manual probe failure against node006-bb098 produces a condition message without port numbers and with a retry hint.

- [x] 1.3 Add `ProbeBackoff` CRD enum value: in `api/v1alpha1/bmctelemetry_types.go`, add the constant `ConnectionStateProbeBackoff ConnectionState = "ProbeBackoff"` and update the kubebuilder marker to `// +kubebuilder:validation:Enum=Connected;Reconnecting;Failed;SubscriptionActive;SlotsFull;ProbeBackoff`. Run `make generate manifests` to regenerate CRD YAML. Verify: `go build ./...` compiles; `kubectl apply` of the updated CRD reports no schema validation error.

- [x] 1.4 Clear status on probe failure: in `probeEventServiceForBMC`, after `setSchemaDetectedCondition(false, ...)` and before returning, call `setConnectedCondition(ctx, tel, false, "ProbeFailed")` (new helper with the same idempotency guard pattern as `setSchemaDetectedCondition`) and set `tel.Status.ConnectionState = ConnectionStateProbeBackoff` in the same status patch. Verify: unit test `TestProbeFailureClearsConnectedCondition` confirms that a BMCTelemetry starting with `Connected: True` and `connectionState: Connected` transitions to `Connected: False/ProbeFailed` and `connectionState: ProbeBackoff` after a simulated probe failure; a second call with the same state issues no additional patches.

## 2. Idempotent condition patch guard

- [x] 2.1 Update `setSchemaDetectedCondition` in `bmc_reconciler.go` to check the existing condition before patching: if `meta.FindStatusCondition(tel.Status.Conditions, ConditionSchemaDetected)` already has the same `Status` and `Reason`, return `nil` without calling `r.Status().Patch()`. Verify: unit test `TestSetSchemaDetectedCondition_idempotent` asserts that a second call with the same status+reason issues zero patch calls; a call that changes status or reason does patch. Run `go test ./internal/controller/...`.

## 3. Per-BMC exponential probe backoff

- [x] 3.1 Add `probeFailures sync.Map` field to `BMCReconciler` (key: BMC name `string`, value: `int`). Verify: field is present and the struct still compiles (`go build ./...`).

- [x] 3.2 At the top of `probeEventServiceForBMC`, reset the failure counter for the BMC (`r.probeFailures.Delete(bmc.Name)`). On probe failure, load the counter, increment it, store it back, then compute `requeue = min(5min * 2^(failures-1), 30min)` and return that duration. Verify: unit test `TestProbeBackoff` covers failures 1–4+ and asserts requeue values 5/10/20/30 min; reset on any reconcile trigger (success or failure on a re-triggered reconcile) returns to 5 min on the next failure. Run `go test ./internal/controller/...`.

## 4. Integration verification

- [x] 4.1 Run the full test suite and confirm no regressions: `go test ./...` passes.

- [ ] 4.2 Verify against the live cluster: after deploying the updated operator to a-qa-de-100, confirm that node006-bb098 probe errors in the MTO logs drop to at most one per 5 minutes initially, and that the rate continues to decrease as backoff builds (check with `kubectl logs -n shoot--cp--m-qa-de-100 <mto-pod> --since=1h | grep node006-bb098 | grep -oP '\d{4}-\d{2}-\d{2}T\d{2}:\d{2}' | uniq -c`).

- [ ] 4.3 Confirm operator CPU drops from ~220m back to baseline after the backoff takes effect (`kubectl top pod -n shoot--cp--m-qa-de-100`).

## Workflow follow-up

- Archive the change once the PR is merged and the fix is confirmed stable in a-qa-de-100.
