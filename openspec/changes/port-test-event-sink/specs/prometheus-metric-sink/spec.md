# Spec Delta

## ADDED Requirements

### Requirement: Pipeline health timestamp metric published per BMC
The system SHALL publish a `redfish_event_pipeline_test_timestamp` gauge recording the Unix epoch time of the last successful pipeline round-trip health check for each BMC. The metric uses label set `{hostname, result="success"}`.

#### Scenario: Successful round-trip recorded
- **WHEN** a test event completes a full round-trip (sent by the operator, received back from the BMC)
- **THEN** `redfish_event_pipeline_test_timestamp{hostname=<bmc>, result="success"}` is set to the current Unix time

#### Scenario: No successful round-trip yet
- **WHEN** the operator starts and no round-trip has completed for a BMC
- **THEN** no `redfish_event_pipeline_test_timestamp` series exists for that BMC

### Requirement: Pipeline health last-result metric published per BMC
The system SHALL publish a `redfish_event_pipeline_test_last_result` gauge that is `1` when the most recent pipeline test succeeded and `0` when it failed or timed out. Label set: `{hostname}`.

#### Scenario: Test succeeds — gauge set to 1
- **WHEN** a pipeline round-trip test event is successfully received back
- **THEN** `redfish_event_pipeline_test_last_result{hostname=<bmc>}` is set to `1`

#### Scenario: Test times out — gauge set to 0
- **WHEN** a sent test event is not received back within the configured timeout
- **THEN** `redfish_event_pipeline_test_last_result{hostname=<bmc>}` is set to `0`

### Requirement: Pipeline health metrics removed on BMC deletion
When a BMC is removed, the pipeline health gauge series for that BMC SHALL be deleted so stale series do not persist.

#### Scenario: BMC deleted — health metrics cleared
- **WHEN** a `BMC` resource is deleted
- **THEN** both `redfish_event_pipeline_test_timestamp` and `redfish_event_pipeline_test_last_result` series for that hostname are removed from the Prometheus registry
