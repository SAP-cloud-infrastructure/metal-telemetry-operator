# Tasks

## 1. Core data types and interfaces

- [x] 1.1 Add `Event` struct (`MessageID`, `EventID`, `Severity`, `EventTimestamp`, `OriginOfCondition`, `Message`) and `EventSink` interface (`PublishEvents(ctx, bmcName, []Event) error`, `Forget(bmcName)`) to `internal/sink/sink.go`; verify it compiles and existing `MetricReportSink` tests still pass
- [x] 1.2 Define `ConsumeIfTest` interface in `internal/push/receiver.go` (`ConsumeIfTest(bmcName, messageId string) bool`) alongside the existing `StatusWriter` interface; verify the package compiles

## 2. Wire format decoder

- [x] 2.1 Create `internal/push/wire.go` with `wireEvent` struct (accepting both `EventId`/`MessageId` and `Severity`/`MessageSeverity`), `originOfConditionRef` with custom `UnmarshalJSON` for Dell plain-string quirk, `eventEnvelope` (accepting both `Events[]` and `Alerts[]`), and `toEvent()` conversion; verify `go vet ./internal/push/...`
- [x] 2.2 Write unit tests in `internal/push/wire_test.go` covering: `Events[]` vs `Alerts[]` key, deprecated `EventId`/`Severity` field names, Dell `OriginOfCondition` as plain string, standard `{"@odata.id":"..."}` form; verify `go test ./internal/push/` passes

## 3. Prometheus event sink

- [x] 3.1 Create `internal/sink/prometheus/eventsink.go` with `redfish_event_alert_total` counter (`hostname`, `severity`, `message_id`, `component` labels), bounded FIFO dedup set (4096 per BMC), deferred-commit pattern (dispatch handler first, commit to dedup set on success), `OnCritical`/`OnClear`/`OnWarning` hooks, `canonicalSeverity()`, and `componentFromOrigin()`; verify it compiles and existing `prometheus.go` tests still pass
- [x] 3.2 Write unit tests in `internal/sink/prometheus/eventsink_test.go` covering: counter increments on first event, no double-count on resent EventID, dedup state cleared by `Forget()`, `OnCritical`/`OnClear`/`OnWarning` callbacks fire with correct severity, `canonicalSeverity` normalizes casing; verify `go test ./internal/sink/prometheus/` passes

## 4. Alert handlers

- [x] 4.1 Create `internal/alert/criticalevent/handler.go` with `ConditionHandler` (`HandleCritical`, `HandleClear`, `SetAlertGauge`), `CriticalEventConditionType` constant, `NewCriticalAlertGauge()` Prometheus gauge, and `ClearCriticalAlert()` helper; handler uses `spec.bmcRef.name` field index to list matching Servers; verify `go vet ./internal/alert/...`
- [x] 4.2 Create `internal/alert/warningevent/handler.go` with `Handler.HandleWarning()` that emits a Kubernetes Warning Event on matching Servers without touching conditions; verify `go vet ./internal/alert/...`
- [x] 4.3 Write unit tests for both handlers using `envtest` or fake client: critical handler sets condition+emits Event+sets gauge, clear handler resets condition+gauge, warning handler emits Event with no condition change; verify `go test ./internal/alert/...` passes

## 5. Server field indexer and RBAC

- [x] 5.1 Register `spec.bmcRef.name` field index on `metalv1alpha1.Server` in `BMCReconciler.SetupWithManager` using the same `constants.BMCRefField` key as MMO; verify the reconciler compiles
- [x] 5.2 Add RBAC markers to `internal/controller/bmc_reconciler.go`: `servers` (get;list;watch), `servers/status` (get;patch), `core/v1 events` (create;patch); run `make generate` and verify the generated ClusterRole includes all three new rules

## 6. Alert gauge prewarm

- [x] 6.1 Register a `manager.RunnableFunc` in `SetupWithManager` that lists all Servers after cache sync and calls `SetAlertGauge(1)` for each Server with `CriticalEventReceived=True` condition; verify it compiles and the prewarm loop runs without error in a unit test with a fake client pre-populated with conditioned Servers

## 7. Push receiver wiring — Phase 1

- [x] 7.1 Update `internal/push/receiver.go` `handleAlerts`: replace the stub call with full `eventEnvelope` decode → call `ConsumeIfTest` per event → route real events to `cfg.EventSink.PublishEvents`; call `StatusWriter.RecordAlertEvent` only for non-test, non-ResourceRemoved events; keep the `isResourceRemoved` early-exit path; verify `go test ./internal/push/` passes
- [x] 7.2 Add `EventSink sink.EventSink` and `TestFilter ConsumeIfTest` fields to `push.ReceiverConfig` (or equivalent); update construction in `bmc_reconciler.go`; verify operator starts without error (`go build ./...`)
- [x] 7.3 Add `SubmitTestEvent` heartbeat to `internal/push/reconciler.go`: on each reconcile pass for an active subscription, check if a heartbeat is due (cadence from config with 5-minute default); fire `SubmitTestEvent` and store the sent MessageId per BMC; implement `ConsumeIfTest` on the reconciler to match arrivals and record health metric; verify `go test ./internal/push/` passes including a test that `ConsumeIfTest` returns true for a tracked MessageId and false for an untracked one
- [x] 7.4 Wire `EventSink`, alert handlers, and prewarm together in `bmc_reconciler.go` SetupWithManager: create `EventSink`, set `OnCritical`/`OnClear`/`OnWarning`, register prewarm, pass `EventSink` and `ConsumeIfTest` to push receiver; verify `go build ./...` and `make test` pass

## 8. Spike — SSE alert stream wire format

- [x] 8.1 Using `u8s set --context m-qa-de-100`, retrieve credentials and IP for at least one Dell BMC (node003-bb098), one HPE iLO BMC, and one Lenovo XCC BMC (if available); for each, open the alerts SSE URL with `curl -N` in background, fire `SubmitTestEvent` via `curl -X POST`, capture the raw SSE frame(s); document vendor, `@odata.type` value observed, top-level key (`Events` or `Alerts`), any field naming differences, and any quirks in `redfish/vendors/<vendor>.md` or a spike note in this change directory
- [x] 8.2 Based on spike findings, confirm or update the Phase 2 task descriptions below before starting Phase 2 implementation; if any vendor uses an unexpected format, update the wire decoder in task 2.1 accordingly

## 9. SSE alert stream routing — Phase 2

- [x] 9.1 Add `EventSink sink.EventSink` and `TestFilter ConsumeIfTest` fields to `internal/sse/conn.go` `sseConn` struct and `ConnConfig`; update `startConn` signature to accept them; verify `go vet ./internal/sse/...`
- [x] 9.2 Update `sseConn.handleEvent`: when `cfg.StreamKind == StreamKindAlerts` and `@odata.type` starts with `#Event.`, decode the payload using `eventEnvelope`, iterate events, call `TestFilter.ConsumeIfTest` per event; route real events to `cfg.EventSink.PublishEvents`; update `{bmc}-alerts LastEventTime` only for real events (skip test events); verify `go test ./internal/sse/` passes
- [x] 9.3 Update `internal/sse/manager.go` `ConnectionManager.NewConnectionManager` and `Ensure` to pass `EventSink` and `ConsumeIfTest` through to `startConn`; wire them in `bmc_reconciler.go`; verify `go build ./...` and `make test` pass
- [ ] 9.4 Integration smoke test: with a MetricReport-mode BMC in QA or local tilt cluster, verify `redfish_event_alert_total` increments and `redfish_server_critical_alert` reflects the Server condition after a test event round-trip and a real alert (or simulated alert via `SubmitTestEvent` with Critical severity); verify `LastEventTime` on `{bmc}-alerts` does NOT update for the test event

## Workflow follow-up

- Run `openspec validate --change bmc-alert-processing` before archiving.
- Archive the change after the project's review requirements are satisfied.
