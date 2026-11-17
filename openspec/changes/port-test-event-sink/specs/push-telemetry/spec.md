# Spec Delta

## ADDED Requirements

### Requirement: Push path records pipeline test result on round-trip completion
The system SHALL record a pipeline health result when a test event sent via `SubmitTestEvent` is received back from the BMC on the push path.

#### Scenario: Test event received back on push path
- **WHEN** the push receiver processes an alert event whose MessageId matches the in-flight test event for a BMC
- **THEN** the system records a success result via the pipeline health recorder and does not route the event to the alert pipeline

#### Scenario: Non-test alert event is not consumed as a test
- **WHEN** the push receiver processes an alert event whose MessageId does not match any in-flight test for that BMC
- **THEN** the system routes the event to the alert pipeline and does not record a pipeline health result

### Requirement: SSE path records pipeline test result on round-trip completion
The system SHALL record a pipeline health result when a test event is received on the SSE alert stream.

#### Scenario: Test event received on SSE alerts stream
- **WHEN** the SSE consumer processes an alert event on the alerts stream whose MessageId matches the in-flight test event for that BMC
- **THEN** the system records a success result via the pipeline health recorder and does not route the event to the alert pipeline

### Requirement: Pipeline health SubmitTestEvent payload is HPE iLO 7 compatible
The `SubmitTestEvent` request SHALL include all required fields so HPE iLO 7 accepts the action.

#### Scenario: SubmitTestEvent accepted by HPE iLO 7
- **WHEN** the operator fires a `SubmitTestEvent` heartbeat against an HPE iLO 7 BMC
- **THEN** the BMC returns a 2xx response (not 400 ActionParameterMissing)
