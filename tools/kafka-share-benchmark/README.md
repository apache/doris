<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Native Kafka Routine Load comparison

This opt-in integration benchmark runs this branch's native standard and share
Routine Load implementations against the same preloaded topic, using independent
groups and Duplicate Key destination tables. It injects no per-record sleeps.
The standard path uses Doris's existing partition assignment; this does not test
Java consumer-group rebalance performance.

Use a disposable non-cloud cluster built from the branch under test, Kafka trunk
with share groups enabled, and enough disk for the build and test data. A single
broker and one-replica Doris tables test ingestion behavior, not replicated
durability. Never use the older custom-reader benchmark as evidence for this path.

## Run

Set these properties in a regression configuration copied from the repository's
`regression-test/conf/regression-conf.groovy`, including its JDBC and HTTP endpoints:

```groovy
enableKafkaTest = true
enableKafkaShareBenchmark = true
externalEnvIp = "127.0.0.1"
kafka_port = 9092
kafkaTrunkHome = "/path/to/built/kafka-trunk"
kafkaShareBenchmarkDorisRevision = "<running Doris commit>"
kafkaShareBenchmarkKafkaRevision = "<running Kafka trunk commit>"
kafkaShareBenchmarkBuildType = "ASAN"
kafkaShareBenchmarkOutput = "/tmp/native-kafka-skew.jsonl"
kafkaShareBenchmarkRecords = 100000
kafkaShareBenchmarkPayloadBytes = 256
kafkaShareBenchmarkRepetitions = 3
kafkaShareBenchmarkReaders = "1,3,9"
```

All FEs and BEs must run the same Doris build. Set the FE
`max_routine_load_task_concurrent_num` to at least the highest requested reader
count, and provision enough BE routine-load task slots. A capped concurrency
fails the comparison instead of being mislabeled. Use a new output filename for
each invocation. Run the recovery integration test before the performance matrix:

```bash
./run-regression-test.sh --run --conf /tmp/kafka-benchmark-conf.groovy \
  -d load_p0/routine_load -s test_kafka_share_at_least_once
./run-regression-test.sh --run --conf /tmp/kafka-benchmark-conf.groovy \
  -d performance_p0 -s test_kafka_share_skew_benchmark
python3 tools/kafka-share-benchmark/report.py /tmp/native-kafka-skew.jsonl \
  --output /tmp/native-kafka-skew-report
```

The recovery test needs debug points enabled. The performance matrix does not.
Its output includes HTML, CSV and JSON. Incomplete, failed, mismatched-input or
incorrect runs are rejected by the report generator.

## Workload and measurements

| Control | Kafka partitions | Distribution |
| --- | --- | --- |
| balanced | 3 | Round-robin |
| hot95 | 3 | 95% on P0; remainder split over P1/P2 |
| single | 1 | All input on P0 |

Each control runs with 1, 3 and 9 requested readers by default. Each pair reads
the same producer-confirmed records, with equal producer batching, payload,
table DDL and Routine Load limits. Modes run sequentially to avoid competing for
Doris capacity. Their order alternates across repetitions. One warmup per pair
is excluded; three repetitions are measured. Results can show either mode winning.
Topic data is deleted after both modes finish; the final tables remain for debugging.

- Throughput is unique visible IDs divided by job-creation-to-completion time.
- The 95% time measures when a query first sees 95% of expected IDs. It is not
  per-record p95 latency. Both measurements include scheduling and a 500 ms query
  observation interval; increase input volume for meaningful sustained throughput.
- Verification checks the complete contiguous ID set, partition distribution,
  numeric values, deterministic MD5-derived payloads and duplicate row count.
  Duplicate rows do not inflate throughput and remain visible in the results.
- The two implementations retain their native batching behavior. The share path
  commits one polled batch; the standard path accumulates to its task limits.
  Actual transaction statistics are retained for interpreting this difference.
- Three hash buckets distribute destination writes by event ID, keeping Kafka
  source skew separate from destination-table skew. No time partition or secondary
  index is needed for this small append-only test fixture.
- Actual FE/BE versions are recorded; revision fields are operator declarations
  and must match the running builds. Record hardware, build type, resource limits
  and background workloads alongside the report before publishing capacity claims.

For evidence of which readers receive P0, enable the existing
`routine_load.reader_telemetry` BE debug point for both modes on the disposable
cluster, save all matching BE INFO logs, then disable it. Pass each log using
`report.py --be-log /path/to/be.INFO`. Receipt counts are included in `results.json`
and HTML. They describe participation over a run, not instantaneous concurrency.
Use the same telemetry setting for both modes; its logging can affect throughput.

## Correctness coverage and remaining fault tests

| Check | Automated coverage |
| --- | --- |
| All IDs and payloads arrive in both modes | Benchmark reconciliation |
| Hot partition and readers beyond partition count | Benchmark matrix and effective concurrency assertion |
| Failed Doris commit is retried | `test_kafka_share_at_least_once` |
| Pause, produce more input, resume | `test_kafka_share_at_least_once` |
| Processed prefix ACCEPT; remainder RELEASE | `KafkaShareConsumerTest` |
| Expired acquisition does not strand other ACKs | `KafkaShareConsumerTest` |
| Consume/pipeline/commit failure and pool reuse | `RoutineLoadTaskExecutorTest.Share*` |
| Committed publish timeout and failed ACK response | `RoutineLoadTaskExecutorTest.Share*` |

BE crash after commit/before ACK, FE leader failover, Kafka broker/coordinator
restart and replicated storage failures still require a multi-process fault run.
`RoutineLoadTaskExecutor.before_share_ack` provides the crash window. These cases
are not claimed as passed by this benchmark. Run fault experiments separately
from timing trials; allow duplicates for at-least-once recovery and require zero
missing or corrupt IDs. Retention and delivery-attempt limits still bound recovery.

Validate the report's correctness gates independently:

```bash
python3 -m unittest discover -s tools/kafka-share-benchmark -v
```
