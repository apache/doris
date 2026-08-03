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

Stack: ZooKeeper, a fluss coordinator server, one fluss tablet server, and a
Flink cluster (jobmanager, taskmanager, sql-client). The sql-client container
builds the fixtures once and then idles; its healthcheck only turns green after
every statement succeeded, so `--wait` gates on the fixtures being complete.

The fluss cluster is lakehouse-enabled: `datalake.format: paimon` with a
filesystem warehouse under `data/paimon`, and the sql-client container runs the
fluss lake tiering service as a Flink job while building the fixtures.

## Prerequisite: a built fluss checkout

Fluss 1.0 is not released yet, so neither image can be pulled. `build-images.sh`
builds both from a local source tree, which must be packaged first:

```bash
git clone https://github.com/apache/fluss.git
mvn -f fluss/pom.xml \
    -pl fluss-dist,fluss-flink/fluss-flink-1.20,fluss-flink/fluss-flink-tiering,fluss-lake/fluss-lake-paimon \
    -am package -DskipTests
```

The paimon runtime the tiering job needs (`paimon-flink-1.20`) is resolved from
the local maven repository, or downloaded from Maven Central when it is missing.

When fluss 1.0 ships, this step and `build-images.sh` are replaced by the
official `apache/fluss` and Flink images.

## Start / stop

```bash
CONTAINER_UID=doris-e2e-- FLUSS_SOURCE_DIR=/path/to/fluss \
    bash docker/thirdparties/run-thirdparties-docker.sh -c fluss

CONTAINER_UID=doris-e2e-- bash docker/thirdparties/run-thirdparties-docker.sh -c fluss --stop
```

`build-images.sh` reuses images that already exist; set
`FLUSS_DOCKER_REUSE_IMAGES=0` to force a rebuild after changing the fluss
checkout.

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

The servers advertise `<host ip>:<published port>`, because Doris FE/BE run on
the host rather than inside the compose network.

Two directories are bind mounted at the same absolute path inside the containers
and on the host, because Doris reads the files in them directly and the path
string is recorded rather than translated:

| Directory | Written by | Read by |
|---|---|---|
| `data/remote` (`remote.data.dir`) | fluss servers | Doris BE — kv snapshots, remote log segments |
| `data/paimon` (`datalake.paimon.warehouse`) | the tiering job | Doris FE/BE through the paimon connector |

## Fixtures

The fixtures recreate database `fluss_test` from scratch on every start:

| Table | Shape |
|---|---|
| `log_basic` | log table, 3 rows, table and column comments |
| `log_types` | log table, one column per mapped fluss type, plus an all-NULL row |
| `log_part` | log table partitioned by `dt`, partitions `20260101`, `20260102`, `20260103` |
| `log_empty` | log table with no rows at all (planning must emit zero scan ranges) |
| `pk_basic` | primary-key table, one updated row and one deleted row |
| `pk_types` | primary-key table with the same type coverage as `log_types` |
| `pk_part` | primary-key table partitioned by `dt`, with an update and a delete inside a partition |
| `lake_log` | lake table, 4 rows tiered + 2 in the log, 3 buckets (some bucket has no tail) |
| `lake_cold` | lake table read entirely from the lake — no log tail at all |
| `lake_types` | lake table with the full type coverage; non-NULL rows tiered, the all-NULL row in the log |
| `lake_part` | lake table partitioned by `dt`; only `20260101` has a log tail |
| `lake_pk` | primary-key lake table; its tail updates one tiered row, deletes another and adds a key the lake never saw |

### Lake tables are frozen half in, half out

Building them takes three steps (`scripts/run-init-sql.sh`):

1. `sql/init.sql` writes the rows that belong in paimon, tiering service running;
2. `sql/lake-row-counts.sql` is polled until paimon holds every one of them;
3. the tiering job is cancelled, and only then does `sql/init-lake-tail.sql`
   write the rows that must stay in the fluss log.

Both the counting and the cancelling are load-bearing. Left running, the tiering
service would keep consuming the tail, and a suite asserting that a table is read
as "lake plus log" would quietly become one asserting "lake only" — passing or
failing by how long the environment had been up. And waiting for *a* paimon
snapshot rather than for the *row counts* would freeze some fixtures half-tiered,
which is the same flakiness one step earlier.

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
