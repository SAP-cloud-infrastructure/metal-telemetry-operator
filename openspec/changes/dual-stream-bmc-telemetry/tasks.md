# Tasks

## 1. Thread A: Fix keep-alive exhaustion

- [x] 1.1 In `internal/push/reconciler.go` `defaultClientFactory.NewClient()`, add `DisableKeepAlives: true` to the `http.Transport` literal. Verify by adding a unit test that calls `NewClient` and asserts `Transport.(*http.Transport).DisableKeepAlives == true`.

## 2. Thread A: Surface subscription slot exhaustion

- [x] 2.1 Define `var ErrSubscriptionSlotsFull = errors.New("subscription slots full")` in `internal/push/reconciler.go`. In `CreateEventSubscription`, on HTTP 409 parse the response body for `MessageId`; if it contains `"ResourceInUse"` or `"EventSubscriptionLimitExceeded"`, return `ErrSubscriptionSlotsFull`. Verify with a unit test that passes a 409 response with each MessageId variant and asserts `errors.Is(err, ErrSubscriptionSlotsFull)`.
- [x] 2.2 In `ensureSubscriptions`, when `errors.Is(err, ErrSubscriptionSlotsFull)`, set `ConnectionState: SlotsFull` on the affected stream resource and return `ctrl.Result{RequeueAfter: 10 * time.Minute}, nil` instead of propagating the error. Verify with a reconciler test that the result is a requeue-after and no error is returned.

## 3. API types: StreamKind and ConnectionState extension

- [x] 3.1 In `api/v1alpha1/bmctelemetry_types.go`, define `StreamKind string` type with values `StreamKindMetrics = "Metrics"` and `StreamKindAlerts = "Alerts"`, add a `+kubebuilder:validation:Enum` marker, and add an optional `StreamKind StreamKind` field to `BMCTelemetrySpec`. Verify the type compiles.
- [x] 3.2 Add `ConnectionStateSubscriptionActive ConnectionState = "SubscriptionActive"` and `ConnectionStateSlotsFull ConnectionState = "SlotsFull"` constants, and update the `+kubebuilder:validation:Enum` marker on `ConnectionState` to include both new values. Verify the type compiles.
- [x] 3.3 Add `+kubebuilder:printcolumn:name="Stream",type=string,JSONPath=`.spec.streamKind`` to the BMCTelemetry struct markers, positioned between State and Last Event columns. Verify it renders via `kubectl explain bmctelemetry` after CRD update.
- [x] 3.4 Run `make generate && make manifests` and verify the generated CRD YAML in `config/crd/bases/` contains `streamKind`, `SubscriptionActive`, and `SlotsFull`.

## 4. SSE package: alert URL, dual-stream ConnConfig, and ConnectionManager

- [x] 4.1 In `internal/sse/vendor.go`, add `AlertSSEURL() string` to `VendorConfig` that returns `SSEURI + "?$filter=EventFormatType eq Event"` (quoted or unquoted per `FilterSyntax`, mirroring `SSEURL()`). Verify by adding cases to `vendor_test.go` for both filter syntax variants.
- [x] 4.2 Add `StreamKind telemetryv1alpha1.StreamKind` to `sse.ConnConfig`. Update `configEqual` to include `StreamKind` in the comparison. Verify the existing SSE manager tests still pass.
- [x] 4.3 Add `streamKind` parameter to all three `sse.StatusWriter` interface methods (`SetConnected`, `IncrReconnectCount`, `SetLastEventTime`). Update `sseConn.run()` and `stream()` to pass `cfg.StreamKind` through to every status writer call. Verify the code compiles and existing SSE connection tests pass.
- [x] 4.4 Change `ConnectionManager.conns` map key from `string` to an unexported `connKey struct { bmcName string; streamKind telemetryv1alpha1.StreamKind }`. Update `Ensure` and `Stop` to key on `connKey{cfg.BMCName, cfg.StreamKind}` and `connKey{bmcName, streamKind}` respectively. Verify the manager unit tests compile and pass.
- [x] 4.5 Add `StopBMC(bmcName string)` to `ConnectionManager` that stops and removes all entries whose `connKey.bmcName == bmcName`. Verify by adding a test that ensures both stream connections stop when `StopBMC` is called.

## 5. Controller: dual-resource lifecycle and dual SSE connections

- [x] 5.1 Rewrite `ensureTelemetry` in `internal/controller/bmc_reconciler.go` to create (or retrieve) `{bmc.Name}-metrics` (StreamKind=Metrics) and `{bmc.Name}-alerts` (StreamKind=Alerts), both owned by the BMC. Return both resources. Verify with a controller unit test that two BMCTelemetry resources are created with the correct names and spec fields.
- [x] 5.2 Update `deleteOwnedTelemetry` to delete both `{name}-metrics` and `{name}-alerts` (ignore NotFound). Verify in the same test suite that both resources are absent after deletion.
- [x] 5.3 Update `SetConnected`, `IncrReconnectCount`, and `SetLastEventTime` to accept a `streamKind telemetryv1alpha1.StreamKind` parameter and look up the resource named `bmcName + "-" + strings.ToLower(string(streamKind))`. Update the `sse.StatusWriter` compile-time assertion. Verify the reconciler satisfies the updated interface.
- [x] 5.4 In `Reconcile`, when mode is SSE, call `connMgr.Ensure` twice: once with `ConnConfig{..., StreamKind: Metrics, Vendor: vendorCfg}` (URL from `vendorCfg.SSEURL()`), and once with `ConnConfig{..., StreamKind: Alerts, Vendor: vendorCfg}` (URL from `vendorCfg.AlertSSEURL()`). Verify both connections appear in the manager's internal map during testing.
- [x] 5.5 Replace all `connMgr.Stop(bmc.Name)` calls in `Reconcile` with `connMgr.StopBMC(bmc.Name)`. Verify the change compiles and no SSE connection leaks in the controller test.

## 6. Push pipeline: dual-resource status and alert stream tracking

- [x] 6.1 Add `RecordAlertEvent(ctx context.Context, bmcName string)` to the `push.StatusWriter` interface in `internal/push/receiver.go`. Verify the interface compiles.
- [x] 6.2 In `internal/push/reconciler.go` `Reconcile`, change the `BMCTelemetry` lookup from `types.NamespacedName{Name: req.Name}` to `types.NamespacedName{Name: req.Name + "-metrics"}`. Verify with a reconciler test that the lookup targets the metrics resource.
- [x] 6.3 In `ensureSubscriptions`, after successfully creating or confirming a MetricReport subscription, set `ConnectionState: SubscriptionActive` on `{bmc}-metrics`; after the alerts subscription, set `SubscriptionActive` on `{bmc}-alerts`. When `ErrSubscriptionSlotsFull` is returned for the MetricReport sub, set `SlotsFull` on `{bmc}-metrics`; for the alerts sub, set `SlotsFull` on `{bmc}-alerts`. Verify per-stream state in a table-driven reconciler test.
- [x] 6.4 Implement `RecordAlertEvent` on `BMCReconciler` (satisfying `push.StatusWriter`): look up `{bmcName}-alerts` and update `LastEventTime`. Verify the reconciler satisfies the updated interface at compile time and the correct resource is updated in a unit test.
- [x] 6.5 Implement `RecordMetricReport` routing change: update the existing `BMCReconciler.RecordMetricReport` to look up `{bmcName}-metrics` instead of the bare `bmcName`. Verify `LastEventTime` is set on the metrics resource in a unit test.
- [x] 6.6 In `internal/push/receiver.go` `handleAlerts`, call `r.cfg.StatusWriter.RecordAlertEvent(req.Context(), bmcName)` after confirming the event is not a ResourceRemoved event. Verify with a handler unit test that a non-ResourceRemoved POST calls `RecordAlertEvent` and a ResourceRemoved POST does not.

## 7. Documentation

- [x] 7.1 In `baremetal-workspace/redfish/vendors/hpe-ilo.md`, add a note to the subscription limits section (or create one if absent) documenting: iLO 6 has the same 3-subscription hard cap as iLO 7; iLO 6 returns HTTP 409 with `Base.1.19.ResourceInUse` when the limit is hit (unlike iLO 7 which uses `EventSubscriptionLimitExceeded`). Verify the file renders correctly.

## Workflow follow-up

- Run `openspec validate --change dual-stream-bmc-telemetry` after all tasks are done to confirm no spec errors.
- Archive the change once all tasks are marked complete and the PR is merged.
