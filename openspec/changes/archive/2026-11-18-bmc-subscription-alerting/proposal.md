# Proposal

## Why

MTO's telemetry pipeline has three observability gaps compared to its predecessor MMO: subscription health per BMC is not queryable from Prometheus (only visible in Kubernetes CRD status), the PrometheusRule for incoming alerts fires immediately on any event without severity context, and a BMC that had a critical alert set on startup (prewarmed gauge) and never cleared will never re-fire the counter-based alert rule. These gaps make fleet-level triage harder and leave persistent faults invisible to alerting.

## What Changes

- **New Prometheus gauge `redfish_bmc_event_subscription_status`**: records per-BMC subscription health with labels `hostname`, `vendor`, `model`, `reason`. Reason values: `subscribed` (active push subscription, value 1), `excluded`, `probe_failed`, `slots_full`, `sse_mode` (value 0 for all non-subscribed states). Recorded from the push subscription reconciler and the BMC reconciler.
- **New PrometheusRule `RedfishCriticalAlertActive`**: fires when `redfish_server_critical_alert > 0` has been sustained for 5 minutes. Covers the prewarmed gauge case that the counter-based rule misses.
- **Replace `RedfishHardwareAlert` with severity-split rules**: replace the single `increase(redfish_event_alert_total[5m]) > 0` rule (which fires immediately regardless of severity) with two rules — `RedfishCriticalHardwareEvent` (`for: 1m`) and `RedfishWarningHardwareEvent` (`for: 5m`) — so oncall can triage by severity.

## Capabilities

### New Capabilities

- `bmc-subscription-status`: Per-BMC push subscription health as a Prometheus gauge, covering all known subscription states and cleanup on BMC deletion.
- `prometheus-alerting-rules`: PrometheusRule definitions shipped with the chart — hardware alert rules and persistent-condition rules.

### Modified Capabilities

(none — no existing spec-level requirements change)

## Impact

- `internal/sink/prometheus/`: new `subscriptionsink.go` file, following the same pattern as `testsink.go`.
- `internal/push/reconciler.go`: add `SubscriptionStatusRecorder` field; call `RecordSubscriptionStatus` after successful subscription and on `ErrSubscriptionSlotsFull`.
- `internal/controller/bmc_reconciler.go`: create and register the new sink; call `RecordSubscriptionStatus` for excluded, probe-failed, and SSE-mode BMCs; call `ForgetSubscriptionStatus` on BMC deletion.
- `dist/chart/rules/`: add `alert-redfishCriticalAlertActive.rules`; replace `alert-redfishHardwareAlert.rules` with `alert-redfishHardwareEvent.rules`.
- `dist/chart/templates/extras/recording-rules.yaml`: register the new alert rule file; update the per-alert enable flags in `values.yaml`.
