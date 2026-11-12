# telemetry-mode Specification

## Purpose

Determines the telemetry collection mode for each BMC based on firmware probe results, deployment-time ConfigMap rules, per-resource overrides, and exclusion annotations, recording the effective mode in BMCTelemetry status.

## Requirements

### Requirement: MetricReport mode value
The `TelemetryMode` enum SHALL include the value `MetricReport`, used when a BMC supports push-based telemetry collection via Redfish EventDestination subscriptions and MTO is configured to use it.

#### Scenario: MetricReport is a valid TelemetryMode
- **WHEN** a `BMCTelemetry` resource has `status.mode` set to `MetricReport`
- **THEN** the resource is valid and the Kubernetes API accepts it without a validation error

### Requirement: Excluded mode value
The `TelemetryMode` enum SHALL include the value `Excluded`, used when a BMC is explicitly opted out of all telemetry collection. `Excluded` is distinct from `Unsupported`: `Unsupported` means the BMC cannot provide telemetry; `Excluded` means the operator chose not to collect it.

#### Scenario: Excluded is a valid TelemetryMode
- **WHEN** a `BMCTelemetry` resource has `status.mode` set to `Excluded`
- **THEN** the resource is valid and accepted by the Kubernetes API

### Requirement: Auto-detect MetricReport from EventService probe
When the EventService probe returns `ServiceEnabled: true` and `ServerSentEventUri` is empty, the system SHALL assign the BMC telemetry mode `MetricReport` as the auto-detected mode.

#### Scenario: HPE iLO auto-detected as MetricReport
- **WHEN** the EventService probe returns ServiceEnabled true and an empty ServerSentEventUri
- **THEN** the effective telemetry mode is MetricReport

#### Scenario: Missing EventService does not yield MetricReport
- **WHEN** the EventService probe fails or ServiceEnabled is false
- **THEN** the effective telemetry mode is Unsupported, not MetricReport

### Requirement: ConfigMap mode override rules
The system SHALL read named mode override rules from a deployment-configured ConfigMap. Each rule maps a vendor/model identifier pattern to a forced `TelemetryMode`, overriding auto-detection for matching BMCs.

#### Scenario: ConfigMap rule forces mode
- **WHEN** a BMC's vendor/model matches a rule in the ConfigMap that specifies mode MetricReport
- **THEN** the effective mode is MetricReport regardless of what the EventService probe returned

#### Scenario: No matching rule leaves mode unchanged
- **WHEN** no ConfigMap rule matches a BMC's vendor/model
- **THEN** the auto-detected mode is used

### Requirement: Per-BMC ModeOverride in BMCTelemetrySpec
When `BMCTelemetrySpec.ModeOverride` is set on a `BMCTelemetry` resource, the system SHALL use the specified mode in place of both auto-detection and ConfigMap rules.

#### Scenario: Spec ModeOverride takes effect
- **WHEN** `BMCTelemetrySpec.ModeOverride` is set to SSE on a BMC whose probe would return MetricReport
- **THEN** the effective mode is SSE

#### Scenario: Nil ModeOverride falls through to lower precedence
- **WHEN** `BMCTelemetrySpec.ModeOverride` is nil
- **THEN** the system applies ConfigMap rules and then auto-detection to determine the mode

### Requirement: Exclusion annotation stops all telemetry
When a BMC object carries the annotation `telemetry.metal.ironcore.dev/exclude: "true"`, the system SHALL assign mode `Excluded`, skip all telemetry collection for that BMC, and record `Excluded` in `BMCTelemetryStatus.Mode`.

#### Scenario: Annotated BMC gets Excluded mode
- **WHEN** a BMC has annotation `telemetry.metal.ironcore.dev/exclude: "true"`
- **THEN** `BMCTelemetryStatus.Mode` is `Excluded` and no SSE connection or push subscription is created

#### Scenario: Removing exclusion annotation restores collection
- **WHEN** the annotation `telemetry.metal.ironcore.dev/exclude: "true"` is removed from a BMC
- **THEN** the system re-evaluates the mode on the next reconcile and resumes telemetry collection if eligible

### Requirement: Mode selection precedence
The system SHALL resolve the effective mode in this order (highest to lowest): exclusion annotation, `BMCTelemetrySpec.ModeOverride`, ConfigMap rules, auto-detection from EventService probe.

#### Scenario: Annotation beats spec override
- **WHEN** a BMC has both the exclusion annotation and `BMCTelemetrySpec.ModeOverride: SSE`
- **THEN** the effective mode is `Excluded`

#### Scenario: Spec override beats ConfigMap rule
- **WHEN** a BMC has `BMCTelemetrySpec.ModeOverride: MetricReport` and a ConfigMap rule that maps it to SSE
- **THEN** the effective mode is MetricReport

### Requirement: BMCTelemetryStatus.Mode reflects effective mode
The system SHALL record the effective `TelemetryMode` in `BMCTelemetryStatus.Mode` after each reconcile cycle, keeping the status current with ConfigMap or annotation changes.

#### Scenario: Status updated after reconcile
- **WHEN** the BMC reconciler completes a reconcile for a BMC
- **THEN** `BMCTelemetryStatus.Mode` reflects the mode resolved in that reconcile

#### Scenario: ConfigMap update propagates to status
- **WHEN** the mode override ConfigMap is updated and the affected BMC is reconciled
- **THEN** `BMCTelemetryStatus.Mode` reflects the new effective mode

### Requirement: ConfigMap rules are live-reloaded
When the mode override ConfigMap is updated, the system SHALL apply the new rules to subsequent reconciles without requiring a controller restart.

#### Scenario: Rule change takes effect without restart
- **WHEN** the ConfigMap is updated to add a rule that forces a BMC's mode to Excluded
- **THEN** the BMC's next reconcile uses the new rule and sets mode Excluded
