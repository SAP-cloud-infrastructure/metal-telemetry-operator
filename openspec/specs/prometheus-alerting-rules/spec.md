# Prometheus Alerting Rules Specification

## Purpose

Defines the PrometheusRule alerting and recording-rule resources shipped with the chart, covering hardware event alerting and persistent critical-condition detection.

## Requirements

### Requirement: Critical hardware events fire a severity-aware alert

The system SHALL ship a PrometheusRule that fires separate alerts for Critical and Warning severity Redfish hardware events, each with an appropriate debounce period.

#### Scenario: Critical event alert fires after short debounce
- **WHEN** `redfish_event_alert_total{severity="Critical"}` increases within a 5-minute window and the condition persists for at least 1 minute
- **THEN** the `RedfishCriticalHardwareEvent` alert fires with labels `hostname`, `severity`, and `message_id`

#### Scenario: Warning event alert fires after longer debounce
- **WHEN** `redfish_event_alert_total{severity="Warning"}` increases within a 5-minute window and the condition persists for at least 5 minutes
- **THEN** the `RedfishWarningHardwareEvent` alert fires with labels `hostname`, `severity`, and `message_id`

#### Scenario: Single transient event does not fire warning alert
- **WHEN** `redfish_event_alert_total{severity="Warning"}` has exactly one increment and no further increments occur within the debounce window
- **THEN** the `RedfishWarningHardwareEvent` alert does NOT fire

### Requirement: Persistent critical condition fires a sustained alert

The system SHALL ship a PrometheusRule that fires an alert when `redfish_server_critical_alert > 0` has been sustained for at least 5 minutes, covering BMCs whose critical condition was prewarmed on startup and never cleared.

#### Scenario: Prewarmed critical alert fires after sustained period
- **WHEN** `redfish_server_critical_alert{hostname="<bmc>"}` is `1` continuously for more than 5 minutes
- **THEN** the `RedfishCriticalAlertActive` alert fires with label `hostname`

#### Scenario: Transient critical alert does not fire sustained rule
- **WHEN** `redfish_server_critical_alert` is set to `1` and then cleared to `0` within 5 minutes (e.g., HandleClear received)
- **THEN** the `RedfishCriticalAlertActive` alert does NOT fire

### Requirement: Legacy undifferentiated hardware alert rule replaced

The existing `RedfishHardwareAlert` rule (which fires immediately on any severity) SHALL be replaced by the severity-split rules. The new rules MUST be enabled by default so operators who relied on `RedfishHardwareAlert` receive equivalent coverage without additional configuration.

#### Scenario: Upgraded chart disables old rule
- **WHEN** the chart is upgraded from a version containing `RedfishHardwareAlert` to a version containing the severity-split rules
- **THEN** `RedfishHardwareAlert` no longer appears in Prometheus, and `RedfishCriticalHardwareEvent` and `RedfishWarningHardwareEvent` are active

### Requirement: Each alert rule independently toggleable via Helm values

Each alert rule SHALL have a dedicated enable flag in `values.yaml` so operators can disable individual rules without forking the chart.

#### Scenario: Disable persistent critical alert only
- **WHEN** `prometheusRules.alerts.redfishCriticalAlertActive.enabled` is set to `false`
- **THEN** the `RedfishCriticalAlertActive` PrometheusRule is not rendered, and the severity-split hardware event rules remain active
