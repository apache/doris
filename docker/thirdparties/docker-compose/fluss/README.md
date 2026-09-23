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

# Fluss regression environment

Stack: ZooKeeper, MinIO, a fluss coordinator server, one fluss tablet server,
and a Flink cluster (jobmanager, taskmanager, sql-client). The sql-client
container builds the fixtures once and then idles; its healthcheck only turns
green after every statement succeeded, so `--wait` gates on the fixtures being
complete.

The fluss cluster is lakehouse-enabled: `datalake.format: paimon` with a
filesystem Paimon catalog whose warehouse is `s3://fluss-lake/wh` in MinIO. The
sql-client container runs the fluss lake tiering service as a Flink job while
building the fixtures.

## Where the images come from

Both images are the ones the fluss project publishes for its 1.0.0 release,
pinned in `fluss.env.tpl` and pulled by compose:

- `apache/fluss:1.0.0` runs the coordinator and the tablet server. It carries
  `plugins/paimon` (fluss-lake-paimon, paimon-bundle, a shaded hadoop) but not
  `paimon-s3`, which the servers need to open the object-store warehouse the
  moment a datalake-enabled table is created. `fetch-paimon-s3.sh` downloads
  that one jar into `cache/` (git-ignored, checked against the repository's
  `.sha1`, looked up in `~/.m2` first) and the compose file bind mounts it into
  the plugin directory of both server containers -- the same arrangement the
  fluss project's lakehouse quickstart uses.
- `apache/fluss-quickstart-flink:1.20-1.0.0` runs the Flink cluster and the
  sql-client: stock Flink 1.20 plus the fluss connector, the lake tiering job
  under `opt/`, and the paimon runtime (paimon-flink, paimon-s3,
  fluss-lake-paimon, hadoop) kept aside under `/opt/flink/paimon` until the
  image's `init_paimon.sh` copies it into `lib/`. Every flink container here
  starts through that script.

The version is the same one `fe/pom.xml` pins `fluss.version` to, so the
cluster the suites run against is the release the connector was compiled
against. Moving it means editing the two image tags, and `FLUSS_PAIMON_VERSION`
if the fluss parent pom's `paimon.version` moved with it.

Note that the paimon this environment runs (the one fluss was compiled against,
2.0.0 at the time of writing) is not Doris's own `paimon.version` (1.3.1). Doris
reads the lake half of a table with its own paimon, so the tables tiered here
double as a cross-version read check.

## Start / stop

```bash
CONTAINER_UID=doris-e2e-- bash docker/thirdparties/run-thirdparties-docker.sh -c fluss

CONTAINER_UID=doris-e2e-- bash docker/thirdparties/run-thirdparties-docker.sh -c fluss --stop
```

Then enable the suites in `regression-test/conf/regression-conf.groovy`:

```groovy
enableFlussTest=true
```

## Ports and paths

| What | Host port |
|---|---|
| ZooKeeper | 22181 |
| fluss coordinator server | 19123 |
| fluss tablet server | 19124 |
| Flink jobmanager UI | 18085 |
| MinIO S3 API | 19125 |

The servers advertise `<host ip>:<published port>`, because Doris FE/BE run on
the host rather than inside the compose network.

The remote-data directory is bind mounted at the same absolute path inside the
containers and on the host because Doris reads those files directly and the path
string is recorded rather than translated:

| Directory | Written by | Read by |
|---|---|---|
| `data/remote` (`remote.data.dir`) | fluss servers | Doris BE — kv snapshots, remote log segments |

Lake files are objects under `s3://fluss-lake/wh`, written by the tiering job and
read by Doris FE/BE through the Paimon connector. `data/paimon` remains only as
the documented local-directory debugging fallback. `data/minio-control` is a
small request/response directory used by the sql-client to ask MinIO's `mc` tool
to remove a failed attempt's `fluss_test.db` prefix before retrying.
`data/zookeeper-control` connects the SQL client to a read-only ZooKeeper
sidecar that exports the exact lake snapshot IDs the Fluss coordinator has
published as readable.

## Fixtures

The fixtures recreate database `fluss_test` from scratch on every start:

| Table | Shape |
|---|---|
| `log_basic` | log table, 3 rows, table and column comments |
| `log_types` | log table, one column per mapped fluss type, plus an all-NULL row |
| `log_part` | log table partitioned by `dt`, partitions `20260101`, `20260102`, `20260103` |
| `log_empty` | log table with no rows at all (planning must emit zero scan ranges) |
| `log_nested` | log table whose complex types are nested inside complex types, plus rows with NULLs at every level |
| `log_time` | log table carrying a fluss TIME column, the one type Doris cannot represent |
| `part_types` | log table partitioned by one column of every type that survives fluss's partition naming (STRING, CHAR, BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, DATE, BINARY) |
| `part_ts` | log table partitioned by a TIMESTAMP, whose value fluss rewrites into the partition name and nothing can read back |
| `pk_basic` | primary-key table, one updated row and one deleted row |
| `pk_types` | primary-key table with the same type coverage as `log_types` |
| `pk_part` | primary-key table partitioned by `dt`, with an update and a delete inside a partition |
| `pk_nested` | primary-key table with the same nesting as `log_nested`, in the kv row format |
| `pk_empty` | primary-key table with no rows and therefore no kv snapshot |
| `lake_log` | lake table, 4 rows tiered + 2 in the log, 3 buckets (some bucket has no tail) |
| `lake_cold` | lake table read entirely from the lake — no log tail at all |
| `lake_types` | lake table with the full type coverage; non-NULL rows tiered, the all-NULL row in the log |
| `lake_part` | lake table partitioned by `dt`; only `20260101` has a log tail |
| `lake_pk` | primary-key lake table, one bucket; its tail updates one tiered row, deletes another and adds a key the lake never saw |
| `lake_pk_multi` | primary-key lake table over 3 buckets; the tail reaches some buckets and not others, which is what makes per-bucket binding observable |
| `lake_pk_part` | primary-key lake table partitioned by `dt`: `20260101` is lake + tail, `20260102` is lake only, `20260103` was written after tiering stopped so the lake has never seen it |
| `lake_pk_cold` | primary-key lake table read entirely from the lake — no tail, so nothing to merge |
| `lake_nested` | lake table with nested complex types; the populated row is tiered, the all-NULL row stays in the log |
| `lake_empty` | lake table nothing was ever written to, so tiering has never committed and there is no snapshot to read |
| `lake_part_int` | lake table partitioned by an INT; one partition has a tail, the other does not |
| `lake_pk_part_int` | primary-key lake table partitioned by an INT — the halves cannot be matched by a non-STRING partition value, so it falls back to the fluss-only read |
| `big_log` | lake table, 100000 rows tiered + 1000 in the log (ids 1..101000) |
| `big_pk` | primary-key lake table, 100000 keys tiered; the tail updates 500, adds 500 and deletes 5 |

There is deliberately no deletion-vector fixture. Fluss does forward a
`paimon.deletion-vectors.enabled` table property into the paimon table it
creates, but its tiering service writes no deletion vector index, and paimon then
reads such a table as empty — see the note in `sql/init.sql`.

### Lake tables are frozen half in, half out

Building them takes three steps (`scripts/run-init-sql.sh`):

1. `sql/init.sql` writes the rows that belong in paimon, tiering service running;
2. `sql/lake-row-counts.sql` is polled until Paimon holds every one of them;
   the ZooKeeper sidecar then reads each coordinator-published readable
   snapshot ID, and `sql/lake-readable-counts.sql` is generated to count that
   exact Paimon snapshot rather than Paimon's latest snapshot or Fluss's
   snapshot-plus-log view;
3. the tiering job is cancelled, and only then does `sql/init-lake-tail.sql`
   write the rows that must stay in the fluss log.

Both the counting and the cancelling are load-bearing. Left running, the tiering
service would keep consuming the tail, and a suite asserting that a table is read
as "lake plus log" would quietly become one asserting "lake only" — passing or
failing by how long the environment had been up. And waiting for *a* paimon
snapshot rather than verifying the coordinator's *exact readable snapshot*
would freeze some fixtures half-tiered: an older readable snapshot plus its log
tail can still return the complete Fluss count.

Fixture initialization has one 3000-second wall-clock deadline shared by
startup, SQL probes, cleanup and all three retries. Every command and stage is
capped by the remaining time, and the compose healthcheck stays in its startup
period for that same budget before applying its retry window.

### Primary-key tables come with a kv snapshot

The server takes kv snapshots every ten seconds here rather than every ten
minutes, and startup does not report the environment ready until each
primary-key table has one on disk (`wait_for_kv_snapshots` in
`scripts/run-init-sql.sh`).

That is not tuning: Doris BE reads those snapshot files directly, from the
host, at the path this container wrote them to — the directory is bind mounted
at the same absolute path on both sides — and nothing but an end-to-end run
covers that. A primary-key table with no snapshot is read by replaying its
whole change log instead, which is equally correct and takes a different code
path, so without the wait the interesting path would only be exercised by luck.

Short intervals do not pile up files: a tablet whose log has not advanced since
its last snapshot is skipped, and the fixtures stop writing when init ends.
