# Tasks

## 1. API types and code generation

- [x] 1.1 Add `MetricReport` and `Excluded` constants to `TelemetryMode` in `api/v1alpha1/bmctelemetry_types.go`; add `ModeOverride *TelemetryMode` field to `BMCTelemetrySpec`; update `+kubebuilder:validation:Enum` markers on both fields; run `make generate manifests` and verify it exits 0
- [x] 1.2 Verify `config/crd/bases/*.yaml` includes `MetricReport` and `Excluded` in the `mode` enum and `modeOverride` field in the spec schema

## 2. SSE probe: MetricReport auto-detection

- [x] 2.1 Add `ServiceEnabled bool` to `eventServiceResponse` and `VendorConfig` in `internal/sse/vendor.go`; add `ModeMetricReport Mode = "MetricReport"` constant; update `detectVendor` Case 1 — when `ServerSentEventUri` is empty and `ServiceEnabled` is true, return `VendorConfig{Mode: ModeMetricReport, ServiceEnabled: true}` instead of `ModeUnsupported`
- [x] 2.2 Add `TestProbeEventService_hpeILO_MetricReport` in `internal/sse/vendor_test.go` (ServiceEnabled=true, empty ServerSentEventUri → ModeMetricReport); remove the incorrect "HPE iLO sends MetricReports" comment on the scanner buffer in `conn.go`; verify `go test ./internal/sse/...` passes

## 3. Push pipeline: config types and exclusion

- [x] 3.1 Create `internal/push/config.go` with ConfigMap data types (`ModeOverrideRule`, `ExcludeHardwareRule`, `Config`) and a ConfigMap loader that live-reloads via controller-runtime's `Watch`; create `internal/push/exclude.go` with the annotation constant and an `IsExcluded(bmc)` function that checks both the annotation and ConfigMap `excludeHardware` rules
- [x] 3.2 Add unit tests in `internal/push/config_test.go` and `internal/push/exclude_test.go` covering ConfigMap parsing, rule matching, and annotation detection; verify `go test ./internal/push/...` passes

## 4. Push receiver

- [x] 4.1 Create `internal/push/receiver.go`: plain HTTP server (`http.ListenAndServe`), route `/serverevents/{subscriberID}/metricsreport/{bmcName}` → parse MetricReport JSON → publish to `sink.MetricReportSink`, route `/serverevents/{subscriberID}/alerts/{bmcName}` → parse event → detect `ResourceRemoved` → enqueue reconcile; implement `NeedLeaderElection() bool { return false }` and `Start(ctx)` so the server registers as a `manager.Runnable`; update `BMCTelemetryStatus.LastEventTime` and `ConnectionState` via a `StatusWriter`
- [x] 4.2 Add receiver tests in `internal/push/receiver_test.go` covering: MetricReport POST dispatches samples, unknown-BMC POST returns 200 and discards, ResourceRemoved alert enqueues a reconcile request; verify `go test ./internal/push/...` passes

## 5. Push subscription reconciler

- [x] 5.1 Create `internal/push/reconciler.go`: controller-runtime `Reconciler` watching `metalv1alpha1.BMC` objects; for each MetricReport-mode BMC create/maintain one MetricReport and one alerts `EventDestination` subscription (subscriber ID namespaced); detect and delete subscriptions with mismatched subscriber IDs; delete subscriptions when BMC leaves MetricReport mode; on startup re-create any missing subscriptions; expose a channel or `EnqueueBMC(name)` method for the receiver's ResourceRemoved path
- [x] 5.2 Add reconciler tests in `internal/push/reconciler_test.go` covering: creates subscriptions on first reconcile, does not duplicate existing subscriptions, deletes subscriptions when mode changes, re-creates on restart; verify `go test ./internal/push/...` passes

## 6. Push AddTo wiring

- [x] 6.1 Create `internal/push/push.go` with `type Options struct` (matching fields from design.md) and `func AddTo(mgr manager.Manager, opts *Options) error` that registers the receiver `Runnable` and sets up the subscription reconciler with `SetupWithManager`; verify `make build` compiles the push package without errors

## 7. bmc_reconciler: mode selection and dispatch

- [x] 7.1 Add a `ConfigLoader` reference (or live-config accessor) to `BMCReconciler` in `internal/controller/bmc_reconciler.go`; implement the four-step mode resolver function: (1) exclusion annotation → `Excluded`, (2) `spec.ModeOverride` → use as-is, (3) ConfigMap `modeOverrides` rule match, (4) `VendorConfig.Mode` from probe; update the reconcile loop to call the resolver and write the result to `BMCTelemetryStatus.Mode`
- [x] 7.2 Add the MetricReport dispatch branch: `ModeMetricReport` → call `push.BMCReconciler.EnqueueBMC` (or use a shared request channel); `ModeExcluded` → ensure SSE conn and push subscription are stopped; keep existing SSE dispatch for `ModeSSE`
- [x] 7.3 Add unit tests in `internal/controller/bmc_reconciler_test.go` covering each priority step of the resolver (annotation beats override, override beats ConfigMap, ConfigMap beats probe) and each dispatch branch; verify `go test ./internal/controller/...` passes

## 8. main.go wiring and Helm chart

- [x] 8.1 Add five flags to `cmd/main.go`: `--push-receiver-url`, `--push-events-addr`, `--push-subscriber-id`, `--push-config-name`, `--push-config-namespace`; call `push.AddTo(mgr, opts)` after the existing SSE reconciler setup; verify `make build` exits 0
- [x] 8.2 Add `push.*` values section to `values.yaml` (receiverURL, eventsAddr, subscriberID, configName, configNamespace); update the Deployment template to pass the push flags; add `RollingUpdate` strategy (`maxSurge: 1, maxUnavailable: 0`), `preStop: exec: sleep 5` lifecycle hook, and readiness probe on the receiver port; add a `PodDisruptionBudget` template with `minAvailable: 1`; verify `helm template .` renders without errors

## 9. Final validation

- [x] 9.1 Run `make test` from the metal-telemetry-operator root and verify all tests pass with no new failures
- [x] 9.2 Run `openspec validate --change add-hpe-metricreport-telemetry` and verify no errors are reported

## Workflow follow-up

- Archive the change after review requirements are satisfied (`/opsx:archive add-hpe-metricreport-telemetry`).
- Verify the archived specs appear in `openspec list --specs`.
