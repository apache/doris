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


```
tools:
    gen_data.py: generate random data
    save_docker.sh: save the current docker state
```

## Run on Apple Silicon macOS

Install Docker Desktop and Bash 4 or newer first. The macOS system Bash
is version 3.2 and cannot run `run-thirdparties-docker.sh`.

```bash
brew install bash
```

Set `CONTAINER_UID` to a unique value containing only letters, numbers,
and hyphens. Then start the local Iceberg stack from the repository
root:

```bash
CONTAINER_UID=dorismac- /opt/homebrew/bin/bash \
    docker/thirdparties/run-thirdparties-docker.sh -c iceberg
```

On macOS, the startup script uses the native ARM64 `postgres:14` image
instead of the amd64-only `postgis/postgis:14-3.3` image. Linux keeps
using the existing PostGIS image. Other images in this stack provide
native ARM64 variants.

The separate `iceberg-rest` stack includes legacy Hadoop images that
only provide amd64 variants. Docker Desktop runs these two images with
amd64 emulation:

```bash
CONTAINER_UID=dorismac- /opt/homebrew/bin/bash \
    docker/thirdparties/run-thirdparties-docker.sh -c iceberg-rest
```

## Server-side scan planning regression

The `iceberg` stack also starts `rest-scan-planning` and `scan-planning-init`.
These services are required by `test_iceberg_rest_server_planning_guard` whenever
`enableIcebergTest=true`. The existing `rest` service remains the Spark/Postgres
baseline; the new REST service has its own disposable SQLite catalog and uses
the private `scan-planning` bucket in the same MinIO service.

The new fixture is pinned to the multi-platform image digest used by
[Iceberg Go's integration tests](https://github.com/apache/iceberg-go/issues/1880).
There is currently no `apache/iceberg-rest-fixture:1.11.0` tag. Do not replace the
pin with a floating `latest` tag or an older released image: the regression
requires the real `/plan` implementation. The pinned index includes both
`linux/amd64` and `linux/arm64`.

The image was built on 2026-04-29 from upstream `main` commit
[`b0df3ca01d61b2f7ae7143ac660c6b16e33b6e46`](https://github.com/apache/iceberg/commit/b0df3ca01d61b2f7ae7143ac660c6b16e33b6e46),
as recorded in its JAR's `iceberg-build.properties`. This commit is an ancestor
of `apache-iceberg-1.11.0`, with 105 intervening commits; it is a development
snapshot, not a 1.11.0 release build. The digest fixes the image contents;
`latest` moving does not change what is pulled. If the old manifest becomes
untagged and upstream removes it, fresh pulls will fail.

The preferred replacement is a multi-platform image built from the upstream
`apache-iceberg-1.11.0` tag and published by Doris CI maintainers as
`doristhirdpartydocker/iceberg-rest-fixture:1.11.0`. That image has not yet been
provisioned for this change. Keep the current digest until the replacement is
available, then verify its source and architectures, pin its digest, and rerun
the regression before switching. Mirroring the current image into the Doris
namespace is a fallback for retention, but would still be a snapshot build.
Upstream versioned-image availability is tracked by Iceberg Go issue #1880;
this change does not depend on an upstream publishing fix arriving first.

The standard `run-thirdparties-docker.sh -c iceberg` command starts the new
services automatically. Docker initialization creates the private bucket and a
test-only `scan_data_reader` identity. This identity can read table data but is
explicitly denied access to metadata under `wh/rest_guard_db/`. Initialization
is repeatable and does not change the existing `warehouse` bucket's policy.

Ports and regression settings must match:

| Compose environment variable | Default | Regression setting |
| --- | --- | --- |
| `ICEBERG_SCAN_PLANNING_REST_PORT` | 18182 | `iceberg_scan_planning_rest_uri_port` |
| `MINIO_API_PORT` | 19001 | `iceberg_minio_port` |

After starting the stack and FE/BE, run from the repository root:

```bash
bash run-regression-test.sh --run -d external_table_p0/iceberg \
    -s test_iceberg_rest_server_planning_guard
```

The regression creates an empty table and a four-row table through a normal
client-planned catalog. It submits a real REST plan, reads a byte from every
planned data file with restricted credentials, and checks that a manifest HEAD
fails with HTTP 403. Doris server-planned reads must then report the explicit
unsupported-mode error, including with restricted storage credentials. `DESC`,
`SHOW CREATE TABLE`, and `INSERT` are also rejected during metadata acquisition;
client-planned reads remain unchanged. Both enabled and disabled table caches
are covered.

An absent service, missing `/plan`, failed storage initialization, or an invalid
storage-policy control **fails** the enabled suite. There is no capability-based
skip. The normal top-level `enableIcebergTest` gate still applies to jobs that do
not run external Iceberg tests.

For a lightweight local run without the Spark data archive, render the same
Compose template with a unique prefix, then start only the new fixture and its
MinIO dependencies (from this directory):

```bash
sed 's/doris--/scan-check-/g' iceberg.yaml.tpl > iceberg.yaml
# Override these ports if another local test stack already uses the defaults.
docker compose -p scan-check --env-file iceberg.env -f iceberg.yaml \
    up -d --wait rest-scan-planning
```

This uses the same repository services and initialization as the full stack.
Do not share the generated YAML between simultaneously running stacks. Set the
matching host/ports and `enableIcebergTest=true` in your local regression config.
The new catalog is ephemeral across container recreation; the suite recreates
its own tables. The bucket remains in MinIO's existing data volume.

This bug regression does not simulate an additional server row policy. A
client-supplied filter and a server-imposed policy are different contracts;
server-forced planning mode is covered separately by real RESTCatalog unit tests.

## Delta Lake regression

The optional `deltalake` Compose profile reuses this stack's MinIO and the
`apache/spark:4.0.0` image. Delta gets a separate Spark service and Hive 3 HMS:
`DeltaCatalog` must own `spark_catalog`, while the existing Iceberg/Paimon Spark
configuration has its own session catalog and extensions. Delta uses
`delta-spark_4.0_2.13:4.4.1`, the artifact for Spark 4.0.

Enable it with the standard runner:

```bash
ENABLE_DELTALAKE_TEST=1 CONTAINER_UID=delta-test- \
    bash docker/thirdparties/run-thirdparties-docker.sh -c iceberg
```

Use Bash 4 or newer on macOS. Pass the same `ENABLE_DELTALAKE_TEST=1` when stopping
this stack with `--stop`. With the flag omitted, the existing Iceberg/Paimon
services run without the optional Delta services.

The runner first prepares the existing Iceberg archive, then calls
`scripts/deltalake/prepare.py`. The Python script downloads and verifies SHA-256
for all seven Spark jars and the pinned Delta source archive. It caches them in
`cache/deltalake`, extracts 31 selected golden tables, normalizes table directory
names to lower-case underscores, and retains upstream `LICENSE.txt`, `NOTICE.txt`
and a source manifest. A missing table or checksum mismatch fails preparation.
`MAVEN_REPOSITORY_URL` can select a Maven mirror; pinned hashes remain mandatory.

Golden tables come from Delta Lake v4.4.1, commit
`cebdc44dc07d331383e72811177af4ffc8638990`, under
`connectors/golden-tables/src/main/resources/golden/`, licensed under Apache 2.0.
The archive SHA-256 is
`a9b81e3e0a10c4b32afec823b5a2803eab57c06f542664478ecd438b2b982339`.
The exact selection and artifact hashes live in `scripts/deltalake/prepare.py`.
Golden fixtures are uploaded to the private `delta` bucket under
`warehouse/delta_golden`; the metadata-only `checkpoint` fixture intentionally
has no data objects, and `no_delta_log_folder` is an error-path fixture.

The Spark service runs the SQL fixtures in
`scripts/create_preinstalled_scripts/deltalake/`, followed by the log-compaction,
golden registration and Parquet pruning helpers. Only after these complete and
Spark Thrift answers a query does the service become healthy. The first successful
setup writes `cache/deltalake/spark/FIXTURES_READY`. Restarts reuse those fixtures
and only restart Spark Thrift. To deliberately regenerate fixtures after changing
their definitions, explicitly set `DELTALAKE_REGENERATE_FIXTURES=1`; this replaces
the fixture tables before populating them. Return the flag to `0` for subsequent
starts. Keep the Spark, HMS and MinIO state directories together when restoring a
retained environment; the marker assumes their corresponding data is still present.
Regression suites retain their catalogs/tables for inspection. Spark uses two local cores
and a 2 GiB driver; allow additional Docker memory when running it alongside the
full Iceberg stack.

| Compose environment variable | Default | Regression setting |
| --- | --- | --- |
| `MINIO_API_PORT` | 19001 | `deltalakeMinioPort` |
| `DELTALAKE_HMS_PORT` | 9383 | `deltalakeHmsPort` |
| `DELTALAKE_SPARK_PORT` | 11002 | `deltalakeSparkPort` |

Set these in `regression-test/conf/regression-conf-custom.groovy`, together with
`enableDeltaLakeTest=true`, `externalEnvIp` and
`deltalakeWarehouse="s3a://delta/warehouse"`, then run:

```bash
bash run-regression-test.sh --run -d external_table_p0/deltalake \
    -s test_deltalake_basic,test_deltalake_skipping,test_deltalake_features,test_deltalake_metadata,test_deltalake_hive,test_deltalake_errors,test_deltalake_parquet,test_deltalake_metrics
```

The tests use both Delta HMS/filesystem catalogs and a Hive catalog's Delta
routing. They cover native reads, mapping/renames, type widening, deletion
vectors, checkpoints/sidecars/compaction, TTL/REFRESH, streaming, COUNT, pruning
and profiles. Spark Thrift is required for the refresh tests' new commits.
The optional million-file fixture is prepared separately; see
`regression-test/suites/external_table_p2/deltalake/README.md`.

For a Delta-only local run, use the same template and preparation script without
starting the existing Iceberg services or downloading their data archive:

```bash
python3 docker/thirdparties/docker-compose/iceberg/scripts/deltalake/prepare.py
cd docker/thirdparties/docker-compose/iceberg
sed 's/doris--/delta-local-/g' iceberg.yaml.tpl > iceberg.yaml
ICEBERG_MINIO_DATA_DIR=./cache/deltalake/minio_data \
    docker compose -p delta-local --env-file iceberg.env -f iceberg.yaml \
    --profile deltalake up -d --wait spark-deltalake
```

The MinIO directory override keeps this lightweight run from creating the
`data/` directory that the full runner uses to detect its Iceberg archive.
Use a unique container prefix and unused host ports for each worktree, and keep
the same directory override when restarting this lightweight stack. Its data
and Hive metastore persist in `cache/deltalake`; stop containers before removing
that generated state.
