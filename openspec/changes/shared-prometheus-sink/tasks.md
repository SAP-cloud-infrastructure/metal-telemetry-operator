# Tasks

## 1. Add injectable Sink to BMCReconciler

- [x] 1.1 Add `Sink sink.MetricReportSink` field to the `BMCReconciler` struct in `internal/controller/bmc_reconciler.go`; in `SetupWithManager`, use `r.Sink` when non-nil, otherwise fall back to `prometheusSink.New(ctrlmetrics.Registry)` as today — verify `make test ./internal/controller/...` passes unchanged

## 2. Add injectable Sink to push.Options

- [x] 2.1 Add `Sink sink.MetricReportSink` field to `push.Options` in `internal/push/push.go`; in `AddTo`, use `opts.Sink` when non-nil, otherwise fall back to `prometheusSink.New(ctrlmetrics.Registry)` as today — verify `make test ./internal/push/...` passes unchanged

## 3. Wire shared sink in main.go

- [x] 3.1 In `cmd/main.go`, call `prometheusSink.New(ctrlmetrics.Registry)` once (before building `BMCReconciler`) and pass the result to both `BMCReconciler{Sink: sink}` and `push.Options{Sink: sink, ...}` — verify `go build ./...` succeeds
- [x] 3.2 Add a unit test in `internal/push/push_test.go` (or `cmd/main_test.go`) that calls `push.AddTo` with a pre-built mock sink and confirms it does not call `prometheusSink.New` a second time — verify with `make test`

## 4. End-to-end startup verification

- [ ] 4.1 Run the operator locally (or in Tilt) with both `--push-receiver-url` set and at least one SSE-capable BMC present; confirm startup log shows no "duplicate metrics collector" error and `redfish_telemetry_reading` appears in `/metrics` — verify by `curl localhost:8080/metrics | grep redfish_telemetry_reading`

## Workflow follow-up

- Archive the change after implementation is reviewed.
