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
kafkaShareBenchmarkBuildType = "Release"
kafkaShareBenchmarkOutput = "/tmp/native-kafka-skew.jsonl"
kafkaShareBenchmarkRecords = 100000
kafkaShareBenchmarkPayloadBytes = 256
kafkaShareBenchmarkRepetitions = 3
kafkaShareBenchmarkReaders = "1,3,9"
kafkaShareBenchmarkTimeoutSeconds = 600
```

Build both FE and BE from the branch under test; use Release for performance
measurements. Record binary hashes if generated version labels are stale. Set the FE
`max_routine_load_task_concurrent_num` to at least the highest requested task
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

Optional PNG/SVG plots require Matplotlib:

```bash
uv run --no-project --with matplotlib python tools/kafka-share-benchmark/plot.py \
  /tmp/native-kafka-skew-report/results.json --output /tmp/native-kafka-skew-report
```

## Workload and measurements

| Control | Kafka partitions | Distribution |
| --- | --- | --- |
| balanced | 3 | Round-robin |
| hot95 | 3 | 95% on P0; remainder split over P1/P2 |
| single | 1 | All input on P0 |

Each control runs with 1, 3 and 9 requested Routine Load tasks by default. Each pair reads
the same producer-confirmed records, with equal producer batching, payload,
table DDL and Routine Load limits. Modes run sequentially to avoid competing for
Doris capacity. Their order alternates across repetitions. One warmup per pair
is excluded; three repetitions are measured. Results can show either mode winning.
Topic data is deleted after both modes finish; the final tables remain for debugging.
The `Readers` configuration sets desired task concurrency. A classic task can
contain several source consumers, capped by `max_consumer_num_per_group` and its
assigned partitions. A share task owns one consumer. The report labels task
concurrency separately from reader receipt evidence and preserves native pooling.

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
Set `kafkaShareBenchmarkBeLogs` to a JSON array of accessible BE INFO log paths
while that point is enabled. The suite then waits for successful share ACK stages
or classic offset-submission stages before stopping each job and deleting its
topic. A share ACK failure invalidates the timing trial. Source-progress completion
time is reported separately; visible-row throughput does not include this wait.
Without these paths, Kafka progress completion is not verified by the benchmark.

## Correctness coverage and remaining fault tests

| Check | Automated coverage |
| --- | --- |
| All IDs and payloads arrive in both modes | Benchmark reconciliation |
| Hot partition and readers beyond partition count | Benchmark matrix and effective concurrency assertion |
| Failed Doris commit is retried | `test_kafka_share_at_least_once` |
| Pause, produce more input, resume | `test_kafka_share_at_least_once` |
| Aborted Kafka transaction is excluded | `test_kafka_share_at_least_once` with group `read_committed` |
| BE crash after commit, before ACK; redelivery produces duplicates | Recovery suite with `kafkaShareRestartBeCommand` |
| Broker restart preserves source and share-group recovery | Recovery suite with `kafkaShareRestartKafkaCommand` |
| Processed prefix ACCEPT; remainder RELEASE | `KafkaShareConsumerTest` |
| Expired acquisition does not strand other ACKs | `KafkaShareConsumerTest` |
| Consume/pipeline/commit failure and pool reuse | `RoutineLoadTaskExecutorTest.Share*` |
| Committed publish timeout and failed ACK response | `RoutineLoadTaskExecutorTest.Share*` |

For the BE crash case, set `kafkaShareRestartBeCommand` to a JSON array such as
`'["/path/to/restart-disposable-be.sh"]'`. The command must SIGKILL and restart only
the disposable BEs, preserve their storage, and return after their HTTP APIs are
ready. The suite blocks before ACK, waits for committed rows to become visible,
runs the command, and requires actual duplicate delivery plus complete payloads.
It skips this case explicitly when the command is absent.

Set `kafkaShareRestartKafkaCommand` to another JSON command array for a disposable
broker restart, preserving its logs and metadata and waiting until its API is
ready. The suite pauses the job, produces another batch, restarts Kafka and
resumes the same group. This case also skips explicitly without its command.

FE leader failover and replicated storage
failures still require a multi-process fault run. These cases are not claimed as
passed by this benchmark. Run fault experiments separately
from timing trials; allow duplicates for at-least-once recovery and require zero
missing or corrupt IDs. Retention and delivery-attempt limits still bound recovery.

Validate the report's correctness gates independently:

```bash
python3 -m unittest discover -s tools/kafka-share-benchmark -v
```
