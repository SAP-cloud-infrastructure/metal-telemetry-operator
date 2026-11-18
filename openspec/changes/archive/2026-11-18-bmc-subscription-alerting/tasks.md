# Tasks

## 1. Create SubscriptionStatusSink

- [x] 1.1 Create `internal/sink/prometheus/subscriptionsink.go`: port `SubscriptionStatusSink` from MMO (`metal-maintenance-operator/internal/telemetry/sink/prometheus/subscriptionsink.go`). Struct holds a `*prometheus.GaugeVec` and a `tracked map[string]prometheus.Labels` (guarded by `sync.Mutex`). Gauge name `"bmc_event_subscription_status"`, namespace `metricNamespace` (already defined in `metricsink.go`), labels `labelHostname`, `"vendor"`, `"model"`, `"reason"`. Methods: `RecordSubscriptionStatus(bmcName, vendor, model, reason string)` (swaps label sets atomically when reason changes), `ForgetSubscriptionStatus(bmcName string)`. Define `SubscriptionStatusRecorder` interface in the same file. Verify `go test ./internal/sink/prometheus/` passes.

## 2. Wire SubscriptionStatusRecorder into push SubscriptionReconciler

- [x] 2.1 Add `StatusRecorder SubscriptionStatusRecorder` field to `SubscriptionReconciler` in `internal/push/reconciler.go`. In `ensureSubscriptions`, after the `r.setConnState(ctx, bmc.Name+"-metrics", ConnectionStateSubscriptionActive)` call (line 250), call `r.StatusRecorder.RecordSubscriptionStatus(bmc.Name, bmc.Status.Manufacturer, bmc.Status.Model, "subscribed")` guarded by a nil check. In both `ErrSubscriptionSlotsFull` branches (MetricReport subscription, line 244; alerts subscription, line 257), call `r.StatusRecorder.RecordSubscriptionStatus(bmc.Name, bmc.Status.Manufacturer, bmc.Status.Model, "slots_full")`. Note: `bmc.Status.Manufacturer` and `bmc.Status.Model` are available from the `*metalv1alpha1.BMC` passed to `ensureSubscriptions` — update the signature to thread vendor/model through, or read them from the already-fetched `bmc` struct. Verify `go vet ./internal/push/...` passes.

- [x] 2.2 Expose `SubscriptionStatusRecorder` input field on `push.Options` in `internal/push/push.go` alongside the existing `TestRecorder` field; in `AddTo`, assign it to the reconciler's `StatusRecorder` field. Verify `go build ./internal/push/...` passes.

## 3. Wire SubscriptionStatusSink into BMCReconciler

- [x] 3.1 Add `statusSink *prometheusSink.SubscriptionStatusSink` field to `BMCReconciler`. In `SetupWithManager`, create the sink (`prometheusSink.NewSubscriptionStatusSink(ctrlmetrics.Registry)`) alongside the existing `testSink` and `AlertEventSink` creation. Pass `r.statusSink` to `push.Options.SubscriptionStatusRecorder` in `main.go`. Verify `go build ./...` passes.

- [x] 3.2 In `BMCReconciler.Reconcile`, add three new call sites using `r.statusSink` (nil-guarded):
  - After `resolveEarlyMode` returns `TelemetryModeExcluded` (around line 248): `r.statusSink.RecordSubscriptionStatus(bmc.Name, bmc.Status.Manufacturer, bmc.Status.Model, "excluded")`
  - After `probeEventServiceForBMC` increments `probeFailures` (error path, around line 358): `r.statusSink.RecordSubscriptionStatus(bmc.Name, bmc.Status.Manufacturer, bmc.Status.Model, "probe_failed")`
  - After mode resolves to `TelemetryModeSSE` (the successful probe + SSE branch, around line 314 where `updateTelemetryStatus` is called with `TelemetryModeSSE`): `r.statusSink.RecordSubscriptionStatus(bmc.Name, bmc.Status.Manufacturer, bmc.Status.Model, "sse_mode")`
  Verify `go vet ./internal/controller/...` passes.

- [x] 3.3 In BMC deletion paths in `Reconcile` (the `apierrors.IsNotFound` branch and the `DeletionTimestamp`/disabled-state branch), call `r.statusSink.ForgetSubscriptionStatus(bmc.Name)` alongside the existing `r.testSink.Forget(...)` and `r.sinkInst.Forget(...)` calls. Verify `go build ./...` passes.

## 4. Replace and add PrometheusRule files

- [x] 4.1 Delete `dist/chart/rules/alert-redfishHardwareAlert.rules`. Create `dist/chart/rules/alert-redfishHardwareEvent.rules` containing two rules in a group named `redfish_hardware_alerts`:
  - `RedfishCriticalHardwareEvent`: `expr: increase(redfish_event_alert_total{severity="Critical"}[5m]) > 0`, `for: 1m`, labels `hostname`, `severity`, `message_id`
  - `RedfishWarningHardwareEvent`: `expr: increase(redfish_event_alert_total{severity="Warning"}[5m]) > 0`, `for: 5m`, labels `hostname`, `severity`, `message_id`
  Verify `helm lint dist/chart/` passes.

- [x] 4.2 Create `dist/chart/rules/alert-redfishCriticalAlertActive.rules` with a single rule in group `redfish_critical_alerts`:
  - `RedfishCriticalAlertActive`: `expr: redfish_server_critical_alert > 0`, `for: 5m`, labels `hostname`, annotation `summary` describing a sustained critical condition.
  Verify `helm lint dist/chart/` passes.

- [x] 4.3 In `dist/chart/templates/extras/recording-rules.yaml`, replace the `redfishHardwareAlert` file reference with `redfishHardwareEvent` and add a reference for `redfishCriticalAlertActive`. In `dist/chart/values.yaml`, under `prometheusRules.alerts`, remove the `redfishHardwareAlert` key and add `redfishCriticalHardwareEvent.enabled: true`, `redfishWarningHardwareEvent.enabled: true`, and `redfishCriticalAlertActive.enabled: true`. Verify `helm template dist/chart/ --set prometheusRules.enabled=true,prometheusRules.alerts.enabled=true` renders all three new rules.

## 5. Integration check

- [x] 5.1 Run `make test` and confirm all tests pass. Verify `go vet ./...` reports no new issues. Check that `redfish_bmc_event_subscription_status` appears in the metrics output by running the operator locally with Tilt or checking the registered metrics list in a test.

## Workflow follow-up

- Run `openspec validate --change bmc-subscription-alerting` before archiving.
- Archive the change after the project's review requirements are satisfied.
