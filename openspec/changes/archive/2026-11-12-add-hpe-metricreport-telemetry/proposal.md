# Proposal

## Why

HPE iLO servers have no SSE support — `ServerSentEventUri` is never populated — so MTO currently marks every HPE BMC as `Unsupported` and collects no metrics from them. HPE supports Redfish MetricReport push subscriptions (BMC POSTs MetricReports to a receiver), which is the same mechanism Metal Maintenance Operator (MMO) already uses in production for all vendors. Adding this mode to MTO brings HPE servers into the same Prometheus metrics pipeline as Dell and Lenovo.

## What Changes

- **New `TelemetryMode` values** — `MetricReport` (BMC pushes MetricReports to MTO's receiver) and `Excluded` (operator opt-out, distinct from `Unsupported`).
- **`BMCTelemetrySpec.ModeOverride`** — optional field allowing the operator to override the auto-detected mode per BMC resource, bridging the gap between detection and initial deployment config.
- **ConfigMap-based configuration** — live-reloaded config (modelled on MMO's pattern) with `modeOverrides` rules (vendor/model → mode) and `excludeHardware` rules, set at deployment time via Helm values rather than by patching CRs.
- **Per-BMC exclusion annotation** — `telemetry.metal.ironcore.dev/exclude: "true"` on a BMC object stops all telemetry for that server, surfaced in `BMCTelemetryStatus.Mode` as `Excluded`.
- **Push receiver** (`internal/push/`) — plain HTTP listener (port configurable) accepting MetricReport POSTs and alert event POSTs from BMCs. Runs on all pods with `NeedLeaderElection: false` (same as MMO). Two routes: `/serverevents/[<subscriberID>/]metricsreport/{bmcName}` and `/serverevents/[<subscriberID>/]alerts/{bmcName}`.
- **Subscription reconciler** (`internal/push/`) — watches BMC objects, creates and maintains one MetricReport subscription and one alert subscription per HPE BMC. Handles `ResourceRemoved` alert events for fast re-subscription when HPE's `TerminateAfterRetries` kills a subscription.
- **`VendorConfig` extended** — adds `ServiceEnabled bool` so the BMC reconciler can distinguish "no SSE but EventService enabled" (HPE → MetricReport eligible) from "EventService disabled" (Unsupported).
- **`cmd/main.go` wiring** — new flags `--push-receiver-url`, `--push-events-addr`, `--push-subscriber-id`, `--push-config-name`, `--push-config-namespace`; push pipeline wired alongside the existing SSE reconciler.
- **Helm chart HA config** — single-replica deployment with `maxSurge: 1 / maxUnavailable: 0`, `preStop` sleep, readiness probe on receiver port, PodDisruptionBudget `minAvailable: 1`.
- **`internal/sse/conn.go` cleanup** — remove the incorrect HPE SSE comment on the scanner buffer (HPE has never sent SSE events).

## Capabilities

### New Capabilities

- `push-telemetry`: BMC-initiated MetricReport push subscription lifecycle — receiver endpoint, subscription creation and maintenance, alert-driven re-subscription, ConfigMap-based config, and HA deployment configuration.
- `telemetry-mode`: Per-BMC telemetry mode selection — auto-detection from EventService probe, deployment-time overrides via ConfigMap, per-BMC exclusion via annotation, and the `Excluded` and `MetricReport` mode values.

### Modified Capabilities

*(none — no existing specs)*

## Impact

- `api/v1alpha1/bmctelemetry_types.go` — new mode enum values, new `ModeOverride` spec field, validation markers
- `internal/sse/vendor.go` — `VendorConfig.ServiceEnabled bool`; `detectVendor` sets it from `EventService.ServiceEnabled`
- `internal/sse/conn.go` — remove incorrect HPE SSE scanner buffer comment
- `internal/push/` — new package: receiver server, subscription reconciler, config types, exclusion matcher (all adapted from `metal-maintenance-operator/internal/telemetry/`)
- `internal/controller/bmc_reconciler.go` — mode selection logic using extended `VendorConfig` and ConfigMap config; annotation check; dispatch to push reconciler vs SSE connection manager
- `cmd/main.go` — five new flags; `push.AddTo(mgr, opts)` call
- Helm chart — new `push.*` values, PDB, rolling update strategy, preStop hook, readiness probe
- `metal-maintenance-operator` — source reference for receiver and subscription reconciler patterns; no changes to MMO itself
