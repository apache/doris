# Experimental Kafka share consumption

Routine Load accepts `"kafka_consumer_type" = "share"` in its `FROM KAFKA`
properties. The default is `standard`, retaining partition assignment and existing
recovery behavior. Upgrade all FEs and BEs before enabling share mode.

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
Exactly-once completion tracking and paired performance dashboards are subsequent
phases; this implementation makes no exactly-once or performance claim.
