# Design

## Context

See `proposal.md — Why` for motivation.

Three independent code areas are touched: a new Prometheus sink for subscription health, the push subscription reconciler and BMC reconciler (call sites for the new sink), and the Helm chart alert rule files.

The existing `TestResultRecorder` interface pattern (defined in `push/reconciler.go`, implemented by `internal/sink/prometheus/testsink.go`) establishes the model for how sinks are decoupled from reconcilers. The new subscription status sink follows this pattern exactly.

The existing `redfish_bmc_event_subscription_status` gauge in MMO (`metal-maintenance-operator/internal/telemetry/sink/prometheus/subscriptionsink.go`) is the reference implementation — label schema, label-set swap logic, and `ForgetSubscriptionStatus` are carried over unchanged.

## Goals / Non-Goals

**Goals:**
- Ship `redfish_bmc_event_subscription_status` with full fleet coverage: every BMC managed by MTO has at most one series, and every series has a deterministic reason.
- Replace `RedfishHardwareAlert` with severity-split rules that are individually toggleable.
- Add `RedfishCriticalAlertActive` to cover the prewarmed-gauge case the counter-based rule misses.

**Non-Goals:**
- Changing CRD status fields — `BMCTelemetry.Status.ConnectionState` remains the authoritative Kubernetes-native view; this change adds a Prometheus projection alongside it, not instead.
- Alerting on `redfish_bmc_event_subscription_status` itself — that is a follow-on operational concern, not part of this change.
- Porting `no_submit_test_event` from MMO — MTO's test event system is config-based, not capability-probed; this reason has no equivalent.

## Decisions

### 1. Interface for subscription status recording

Define a `SubscriptionStatusRecorder` interface in `internal/push/reconciler.go` alongside the existing `TestResultRecorder`. The push `SubscriptionReconciler` holds an optional `StatusRecorder SubscriptionStatusRecorder` field; the BMC reconciler holds the same interface reference via a new unexported field set during `SetupWithManager`.

**Alternative considered**: Pass the concrete `*SubscriptionStatusSink` directly. Rejected — the existing codebase consistently uses interfaces to keep reconciler packages free of Prometheus imports and simplify testing.

### 2. Recording call sites

Five call sites, two packages:

| Call site | Location | Reason recorded |
|---|---|---|
| `resolveEarlyMode` returns `Excluded` | `bmc_reconciler.go` | `excluded` |
| `probeEventServiceForBMC` increments failure counter | `bmc_reconciler.go` | `probe_failed` |
| Mode resolves to SSE (after successful probe) | `bmc_reconciler.go` | `sse_mode` |
| `ensureSubscriptions` sets `SubscriptionActive` | `push/reconciler.go` | `subscribed` |
| `ensureSubscriptions` hits `ErrSubscriptionSlotsFull` | `push/reconciler.go` | `slots_full` |
| BMC deletion | `bmc_reconciler.go` | `ForgetSubscriptionStatus` |

`vendor` and `model` are read from `bmc.Status.Manufacturer` and `bmc.Status.Model`. These are set by metal-operator from prior reconcile cycles and are available even when probing fails on a subsequent cycle.

### 3. SSE-mode reason

SSE-mode BMCs emit `reason="sse_mode"` with value `0`. This was preferred over emitting nothing because a query like `count(redfish_bmc_event_subscription_status)` can then serve as a full fleet headcount, and `reason="sse_mode"` makes the gauge self-documenting.

**Alternative considered**: Omit the gauge for SSE-mode BMCs entirely. Rejected — silent omission makes it impossible to distinguish "SSE active, no push needed" from "BMC not yet reconciled" in dashboards.

### 4. Alert rule file layout

Replace `dist/chart/rules/alert-redfishHardwareAlert.rules` with `dist/chart/rules/alert-redfishHardwareEvent.rules` containing both `RedfishCriticalHardwareEvent` and `RedfishWarningHardwareEvent`. Add `dist/chart/rules/alert-redfishCriticalAlertActive.rules` for the new persistent-condition rule.

The Helm template (`recording-rules.yaml`) renders one PrometheusRule per `rules/` file. Each file maps to a separate enable flag (`prometheusRules.alerts.<camelCaseName>.enabled`). The old `redfishHardwareAlert` flag is replaced by `redfishCriticalHardwareEvent` and `redfishWarningHardwareEvent`.

**Migration note**: The old flag `prometheusRules.alerts.redfishHardwareAlert.enabled` is removed. Operators who set it to `false` to suppress the old alert must update their values file to suppress the new rules individually.

### 5. Alert debounce values

| Rule | `for` | Rationale |
|---|---|---|
| `RedfishCriticalHardwareEvent` | `1m` | Suppresses sub-minute BMC reconnect storms; critical severity still pages quickly |
| `RedfishWarningHardwareEvent` | `5m` | Higher noise threshold; warnings rarely need immediate escalation |
| `RedfishCriticalAlertActive` | `5m` | Avoids alerting on a critical that self-heals before the next scrape cycle |

## Risks / Trade-offs

- **Values file migration break**: Operators with `prometheusRules.alerts.redfishHardwareAlert.enabled: false` in their values will silently re-enable alerting after upgrade because the new rule flags default to `true`. Document this in the PR. → Mitigation: the `RedfishWarningHardwareEvent` 5m debounce reduces noise during any transient gap.

- **`vendor`/`model` availability on first reconcile**: If a BMC was never successfully probed in a prior lifecycle (fresh cluster), `bmc.Status.Manufacturer` and `bmc.Status.Model` may be empty strings when `probe_failed` is first recorded. The empty string is a valid label value; the series will update with real values once the probe succeeds. → Acceptable; label cardinality is bounded by the BMC fleet size.

## Open Questions

None — all debounce values and label schema decisions are resolved above.
