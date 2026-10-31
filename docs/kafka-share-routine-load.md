# Experimental Kafka share consumption

Routine Load accepts `"kafka_consumer_type" = "share"` in its `FROM KAFKA`
properties. The default is `standard`, retaining partition assignment and existing
recovery behavior. Upgrade all FEs and BEs before enabling share mode.
The former `kafka_consumer_mode` property remains an alias so existing jobs survive
FE restart; use `kafka_consumer_type` for new jobs. Do not specify both properties.

```sql
CREATE ROUTINE LOAD events_share ON events
PROPERTIES ("desired_concurrent_number" = "9")
FROM KAFKA (
    "kafka_broker_list" = "kafka:9092",
    "kafka_topic" = "events",
    "kafka_consumer_type" = "share",
    "property.group.id" = "doris-events-share"
);
```

Use a broker with share groups enabled and compatible with librdkafka 2.16.
Reader concurrency can exceed the number of partitions but remains subject to
Doris task and backend limits. Each reader loads one polled batch per transaction.
Doris keeps a generated group ID across restart when none is supplied. Use a
separate group for each logical destination: members of one group divide records.
Initial position and replay are managed by the broker's share-group configuration.

Delivery is **at least once**: records are accepted only after Doris commits.
Failed tasks attempt to release their acquired batch for retry. A successful
transaction accepts only the records fed into its load pipeline and releases the unprocessed
remainder. An expired acquisition does not prevent acknowledgements for other
records; any acknowledgement failure discards that consumer instead of pooling it.
If release fails or the BE dies, retry depends on broker-side acquisition expiry.
A crash after commit but before acknowledgement, an expired acquisition, or an
acknowledgement failure can duplicate rows. There is no destination replay
suppression in this phase. Retention and delivery-attempt limits can prevent
retry; load filtering retains the configured Routine Load behavior. Configure
these policies explicitly for correctness experiments.

The experimental implementation supports non-cloud, single-table jobs. Partition
selection, explicit offsets/default offsets, custom acknowledgement mode, AWS IAM,
FILE-backed properties, and source-property changes through ALTER are rejected.
It does not support resetting progress through Doris. Stop/pause does not reset
broker acknowledgement state. Legacy offset-based lag is unavailable in share mode.

BE bvars expose `routine_load_share_consumers`,
`routine_load_share_polling_consumers`, `routine_load_share_received_records`,
`routine_load_share_committed_records`, and `routine_load_share_ack_failures`.
Committed records count source-record attempts, not unique events or output rows.
Polling consumers count consumers in the receive/append phase, not assigned partitions.

For crash testing, `RoutineLoadTaskExecutor.before_share_ack` blocks after Doris
commit and before Kafka acknowledgement. Terminating that BE exercises redelivery.
This implementation makes no exactly-once or performance claim.

## Kafka trunk regression

Use a disposable non-cloud Doris cluster with debug points enabled, a Kafka trunk
broker with share groups enabled, and a built checkout of that same Kafka revision.
Set `enableKafkaTest = "true"`, `kafkaTrunkHome`, `externalEnvIp`, and `kafka_port`
in the regression configuration's `otherConfigs`, then run:

```bash
./run-regression-test.sh --run -d load_p0/routine_load -s test_kafka_share_at_least_once
```

The suite uses Kafka trunk's `kafka-configs.sh` to configure a fresh share group
with `earliest` and `read_committed`, then tests transaction failure/retry and
pause/resume with more readers than partitions. Its Duplicate Key table preserves
duplicates while the checks require every input ID and its payload to arrive.
The suite skips explicitly unless Kafka tests and `kafkaTrunkHome` are configured.
It uses ordinary share acknowledgements and requires no transactional
acknowledgement extension.
