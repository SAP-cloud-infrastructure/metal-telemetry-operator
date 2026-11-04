# Tasks

## 1. Repository scaffolding

- [ ] 1.1 Run `kubebuilder init --domain metal.ironcore.dev --repo github.com/cobaltcore-dev/metal-telemetry-operator` and verify `go.mod`, `Makefile`, `cmd/main.go`, and `config/` are generated
- [ ] 1.2 Run `kubebuilder create api --group metal --version v1alpha1 --kind BMCTelemetry` and verify `api/v1alpha1/bmctelemetry_types.go` and `internal/controller/bmctelemetry_controller.go` scaffolded correctly
- [ ] 1.3 Add `metal-operator` CRD types as a Go module dependency (`go get github.com/ironcore-dev/metal-operator/api`) and verify `go.sum` updated and `go build ./...` succeeds
- [ ] 1.4 Add `prometheus/client_golang` and `go-logr/logr` dependencies and verify `go mod tidy` completes cleanly
- [ ] 1.5 Copy and adapt the `Dockerfile` from `metal-maintenance-operator` for multi-stage Go build and verify `docker build .` produces an image without errors

## 2. BMCTelemetry CRD

- [ ] 2.1 Define `BMCTelemetrySpec` (empty) and `BMCTelemetryStatus` fields in `api/v1alpha1/bmctelemetry_types.go`: `Mode` (enum: `SSE`, `Unsupported`), `SSEURI`, `FilterSyntax` (enum: `Quoted`, `Unquoted`), `ConnectionState` (enum: `Connected`, `Reconnecting`, `Failed`), `LastEventTime`, `ReconnectCount`, `Conditions` — and verify `make generate` produces `zz_generated.deepcopy.go` without error
- [ ] 2.2 Add kubebuilder markers (`+kubebuilder:object:root=true`, `+kubebuilder:subresource:status`, `+kubebuilder:printcolumn` for Mode and ConnectionState) and verify `make manifests` produces `config/crd/bases/metal.ironcore.dev_bmctelemetries.yaml`
- [ ] 2.3 Register `BMCTelemetry` in the scheme in `cmd/main.go` and verify `kubectl apply -f config/crd/bases/` against a local k3d cluster creates the CRD without errors

## 3. Vendor detection

- [ ] 3.1 Implement `internal/sse/vendor.go`: `ProbeEventService(ctx, httpClient, bmcIP, creds) → (VendorConfig, error)` that fetches `/redfish/v1/EventService` and returns `{Mode, SSEURI, FilterSyntax}` using the four-case detection logic (no SSE URI → Unsupported; `SSEFilterPropertiesSupported` present → Lenovo/Unquoted; `v1_11_0` in `@odata.type` → Dell iDRAC10/Quoted; else → iDRAC9/Unquoted) and verify by unit tests covering all four cases using `httptest.NewServer` fixtures from recorded EventService responses

## 4. SSE consumer (dedup, sentinel filter, Sample conversion)

- [ ] 4.1 Implement `internal/sse/consumer.go`: `ParseMetricReport(eventBody []byte) ([]sink.Sample, error)` that unmarshals `MetricReport`, deduplicates by latest `Timestamp` per `(MetricId, MetricProperty)` key, filters sentinel value `255.0`, normalizes `MetricID` (use `MetricId` when present, else extract fragment after `#/` from `MetricProperty`), and skips entries with no usable ID — and verify by unit tests covering: Dell multi-value batch (picks latest timestamp), Lenovo sentinel `255` (filtered), Lenovo `MetricProperty`-only entry (ID extracted from URL fragment), empty `MetricValues` (returns empty slice, no error)

## 5. MetricReportSink

- [ ] 5.1 Create `internal/sink/sink.go` with the `MetricReportSink` interface (matching `sink.MetricReportSink` in MMO: `PublishSamples`, `Forget`) and verify it compiles
- [ ] 5.2 Create `internal/sink/prometheus/metricsink.go` adapted from MMO's implementation with metric name `redfish_telemetry_reading` and same label set (`hostname`, `metric_id`, `type`, `unit`, `origin_context`) — and verify unit tests confirm: `PublishSamples` sets the gauge for each sample; `Forget(bmcName)` removes all series for that BMC; concurrent `PublishSamples` + `Forget` calls do not race (run with `-race`)

## 6. ConnectionManager and SSE goroutine

- [ ] 6.1 Implement `internal/sse/conn.go`: `sseConn` struct with a goroutine that opens an HTTP GET to the SSE URI with `?$filter=...`, parses SSE lines with `bufio.Scanner`, calls `consumer.ParseMetricReport` on each `#MetricReport.*` event, calls `sink.PublishSamples`, handles non-MetricReport events silently, reconnects with exponential backoff+jitter on stream close/error, and updates `BMCTelemetry.status` via a `StatusWriter` interface — and verify by unit test with `httptest.NewServer` serving a synthetic SSE stream of two MetricReport events followed by stream close, confirming both events are consumed and reconnect is attempted
- [ ] 6.2 Implement `internal/sse/manager.go`: `ConnectionManager` with `sync.Map[bmcName → *sseConn]`, weighted semaphore (`golang.org/x/sync/semaphore`) for bounded concurrent connects (configurable `MaxConcurrentConnects`, default 50), `Ensure(ctx, bmcName, config)` (no-op when healthy, otherwise cancel old + start new) and `Stop(bmcName)` — and verify unit test confirms that starting 100 BMCs simultaneously never exceeds 50 concurrent in-flight dials (instrument with an atomic counter)

## 7. BMC Reconciler

- [ ] 7.1 Implement `internal/controller/bmc_reconciler.go`: `BMCReconciler` watching `BMC` resources — reconcile loop: fetch BMC + BMCSecret; handle deletion (Stop + Forget + delete BMCTelemetry finalizer); create BMCTelemetry if missing with owner reference; probe EventService if `SchemaDetected` condition absent; write mode + SSE params to `BMCTelemetry.status`; call `manager.Ensure` — and verify by envtest-based test covering: new SSE-capable BMC creates BMCTelemetry with `SchemaDetected=True`; BMC with no SSE URI sets `Mode=Unsupported` and does not requeue; BMC deletion calls `Stop` and `Forget`
- [ ] 7.2 Wire the reconciler into `cmd/main.go` with `NeedLeaderElection: true` and register the `BMCTelemetry` scheme — and verify `make run` against local k3d cluster starts without panic and logs initial reconcile cycles

## 8. Helm chart

- [ ] 8.1 Create `foundation-charts/charts/metal-telemetry-operator/` following the MMO chart structure: `Chart.yaml`, `values.yaml` (image tag, `maxConcurrentConnects`, resource limits), `templates/deployment.yaml`, `templates/clusterrole.yaml`, `templates/clusterrolebinding.yaml`, `templates/serviceaccount.yaml`, `templates/prometheusrule.yaml` (optional placeholder) — and verify `helm lint charts/metal-telemetry-operator/` passes with no errors
- [ ] 8.2 Add the chart as a Tilt resource in `metal-telemetry-operator/Tiltfile` following the MMO Tiltfile pattern and verify `tilt up` from the workspace root builds and deploys the operator to the local k3d cluster without errors

## 9. Integration verification

- [ ] 9.1 Deploy to QA cluster (`m-qa-de-100`) via Helm and verify `BMCTelemetry` resources appear for all BMCs: `kubectl get bmctelemetry -n metal-operator-system` shows `Mode=SSE` for Dell and Lenovo nodes and `Mode=Unsupported` for HPE/Fsas/BULL nodes
- [ ] 9.2 Verify `redfish_telemetry_reading` metrics are present in Prometheus by querying `count by (hostname) (redfish_telemetry_reading)` — confirm at least one time series per connected BMC appears within 20 minutes of deployment (Lenovo's maximum batch interval)
- [ ] 9.3 Confirm `redfish_telemetry_reading` and `redfish_monitor_reading` coexist in Prometheus without label collision and both scrape targets are healthy

## Workflow follow-up

- Archive the change after implementation is reviewed and QA verification is confirmed.
