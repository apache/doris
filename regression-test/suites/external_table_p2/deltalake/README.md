# Delta Lake p2 acceptance

## Storage backends

`test_deltalake_storage` runs the same expected results against one storage
backend at a time. Prepare the pinned golden tables with
`docker/thirdparties/docker-compose/iceberg/scripts/deltalake/prepare.py`, then
copy its `cache/deltalake/golden/` contents into
`<warehouse>/delta_golden/` on the target storage. Preserve every table's
relative paths, including `_delta_log`, checkpoint sidecars and DV files.

For example, with an existing HDFS cluster and its Hadoop client configuration:

```bash
hdfs dfs -mkdir -p /warehouse/delta_golden
hdfs dfs -put -f docker/thirdparties/docker-compose/iceberg/cache/deltalake/golden/* \
    /warehouse/delta_golden/
```

Create a local JSON file containing the catalog's warehouse and normal
`fe-filesystem` storage properties, for example:

```json
{
  "warehouse": "hdfs://namenode:8020/warehouse",
  "fs.defaultFS": "hdfs://namenode:8020",
  "hadoop.username": "hadoop"
}
```

Keep credentials and environment-specific files outside version control. Set
these values in the local regression config:

```groovy
enableDeltaLakeTest=true
enableDeltaLakeStorageTest=true
deltalakeStoragePropertiesFile="/absolute/path/to/storage-properties.json"
```

Then run:

```bash
./run-regression-test.sh --run -d external_table_p2/deltalake -s test_deltalake_storage
```

The suite covers column mapping, timestamps, nested type widening, disk DVs,
partitioned checkpoints and both V2 checkpoint formats. Synchronous and streaming
planning read 128-byte splits, including splits sharing a file-global DV. It
uses the same `.out` for each backend; an enabled but unavailable or misconfigured
backend fails the suite rather than being skipped.

Local acceptance on 2026-10-30 passed against both MinIO and ordinary HDFS 3.2.1.
OSS, COS, OBS, Azure, GCS and Kerberos HDFS remain unverified pending test
environments. Reusing filesystem properties does not establish coverage of
these backends. On the local macOS ARM64 ASAN build, the embedded HDFS JVM needed
compressed oops/class pointers and CDS disabled to avoid a JVM address-space
allocation failure; this was a local BE configuration adjustment.

## TPC-H / TPC-DS comparison

The opt-in `test_deltalake_benchmark` suite compares native Delta and ordinary
Hive Parquet reads of **the same immutable Parquet objects**. It runs the existing
22 TPC-H SQL files and 103 TPC-DS SQL files (including the split query variants)
from `tools/`. TPC-H query 15 uses an equivalent CTE in place of `revenue0`, since
the external catalogs are read-only. TPC-DS uses the repository's `sf100` query
parameters on SF1 data. This is reader acceptance, not a certified TPC result or
a production throughput benchmark.

Generate SF1 data with the existing `tools/tpch-tools/bin/build-tpch-dbgen.sh`
and `tools/tpcds-tools/bin/build-tpcds-tools.sh` tools in their supported Linux
environment. Run `dbgen -s 1` with `DSS_PATH` pointing to a dedicated output
directory, and `dsdgen -SCALE 1 -DIR <directory>` from its tools directory. Retain
the generated `.tbl`/`.dat` files and tool distributions for reuse. The sources
used for local acceptance are the repository's TPC-H 3.0.0 and TPC-DS 3.2.0rc2
archives, with SHA-256 respectively:

- `30f0884c5b89fcddebf7daed264691a46d9e07049a3b89dfeb191b189c30cee3`
- `fa1d64cec6df6bd3ef50cff416172dd8a0ffc55500819343fe9af4dfd7ff36eb`

Install `duckdb==1.4.4` in a Python environment (no DuckDB extensions required),
then convert the raw data:

```bash
python3 docker/thirdparties/docker-compose/iceberg/scripts/deltalake/generate_benchmark_parquet.py \
    --tpch-generator-dir /path/to/tpch/dbgen --tpch-data-dir /path/to/tpch-data \
    --tpcds-generator-dir /path/to/tpcds/tools --tpcds-data-dir /path/to/tpcds-data \
    --output /path/to/benchmark
```

The converter uses the generators' DDL, preserves decimal/date types, explicitly
transcodes TPC-DS's Latin-1 text to UTF-8, and writes Snappy Parquet with 122,880-row
groups. It records row counts, sizes and SHA-256 in `manifest.json`. It refuses to
overwrite a nonempty output directory; reruns should reuse the retained fixtures
or choose a new directory. Equivalent Parquet encoding can produce different
bytes, so use the manifest corresponding to the files actually uploaded.

Upload `delta_tpch/` and `delta_tpcds/` beneath `deltalakeWarehouse`, retaining the
layout `warehouse/delta_tpch/lineitem/data/part-00000.parquet` and its equivalents.
Copy the manifest into the Spark service's mounted `cache/deltalake/spark/benchmark/`
directory. In the optional Delta Compose environment, initialize metadata:

```bash
docker compose --env-file iceberg.env -f iceberg.yaml --profile deltalake exec \
    spark-deltalake /opt/spark/bin/spark-submit \
    --properties-file /mnt/scripts/deltalake/spark-defaults.conf \
    /mnt/scripts/deltalake/prepare_benchmark.py \
    --manifest /delta-state/benchmark/manifest.json --warehouse s3a://delta/warehouse
```

The helper registers each Hive table at its `data/` subdirectory and runs
`CONVERT TO DELTA` on the parent directory, without rewriting Parquet. It checks
Hive's provider remains Parquet and Spark reads the expected row count through
both formats. Pointing a plain Hive table at the Delta root would also enumerate
log files, so the separate data directory is required. These fixtures have no
DV or concurrent writes; use the ordinary p0 suites for those semantics.

Set `enableDeltaLakeTest=true` and `enableDeltaLakeBenchmarkTest=true` along with
the existing MinIO/HMS/warehouse settings, then run:

```bash
./run-regression-test.sh --run -d external_table_p2/deltalake -s test_deltalake_benchmark
```

The suite computes the reference result through Hive and compares every Delta
and Hive execution against it. These are dynamic cross-reader comparisons,
not handwritten expected results. It disables SQL/file caches, uses FileScannerV2
and four pipeline tasks for both readers, records a first execution of each, then
three alternating measured repetitions. Each `DELTA_BENCHMARK` log record contains
the workload/query, result row count, first execution times and all six measured
times. Summarize per-query medians; investigate substantial absolute and relative
gaps using EXPLAIN/profiles. First executions are not guaranteed cold OS/object
store caches. Do not run fixture generation or exports concurrently with timing.

Both readers use an 8-GiB query memory limit and a 120-second timeout. SQL failures
are logged as `DELTA_BENCHMARK_FAILURE`; the suite attempts the other reader and
remaining queries, then fails if any query failed. Failed queries have no valid
paired timing and must not be included in performance aggregates. Result mismatches
fail immediately. For a focused rerun, set `deltalakeBenchmarkQueryPattern` to a
regular expression matching `tpch/q7.sql` or `tpcds/query1.sql`, for example
`"tpch/q[789][.]sql"`. Its default `".*"` runs the full inventory.

Delta M1 does not supply CBO statistics. Doris can disable join reordering when
row counts are unknown, which can yield expensive intermediate results for
multi-table queries. The default suite preserves those plans and reports failures;
do not present a hinted or rewritten diagnostic query as default-plan performance.

Catalogs, HMS tables, Parquet files and Delta logs remain available after testing.
For repeat runs, invoke the regression directly; no data regeneration is needed.

## Planning scale

This opt-in fixture declares one million active AddFiles in a classic checkpoint. It deliberately creates **no data objects**: it tests metadata COUNT and FE range planning, not Parquet read throughput or correctness across one million physical objects. The ordinary Delta p0 suites cover real data, deletes and schema evolution.

Install PyArrow 21.0.0 in a Python environment. Use the `only-checkpoint-files` golden table from Delta v4.4.1 (commit `cebdc44dc07d331383e72811177af4ffc8638990`), renamed `only_checkpoint_files` by the fixture importer. Generate into a warehouse/database/table layout:

```bash
python3 docker/thirdparties/docker-compose/iceberg/scripts/create_preinstalled_scripts/deltalake/generate_planning_fixture.py \
  --golden-table /path/to/golden/only_checkpoint_files \
  --output /path/to/warehouse/delta_bench/million_files
```

The source golden table is Apache-2.0 licensed. Its original source is [Delta's pinned golden tables](https://github.com/delta-io/delta/tree/cebdc44dc07d331383e72811177af4ffc8638990/connectors/golden-tables/src/main/resources/golden/only-checkpoint-files). The generator uses its checkpoint schema and protocol/metadata, replaces the table id, and writes synthetic AddFiles plus the checkpoint's own JSON commit. Older commits are intentionally absent.

Run the connector's actual streaming source three times with a small heap:

```bash
mvn -f fe/pom.xml -pl :fe-connector-deltalake -am install \
  -Dtest=DeltaLakePlanningScaleTest -DfailIfNoTests=false \
  -Dmaven.build.cache.enabled=false -DargLine=-Xmx512m \
  -Ddeltalake.planning.table=/path/to/warehouse/delta_bench/million_files
```

The test verifies every pass produced exactly 1,000,000 ranges and 527,000,000 declared bytes. It logs snapshot loading, the streaming decision, first-range latency, full enumeration time, and heap samples every 10,000 ranges. It is skipped unless the table property is supplied. The declared bytes are metadata, not measured data-file traffic.

For Doris p2 acceptance, upload `delta_bench/million_files` beneath the configured `deltalakeWarehouse`, enable `enableDeltaLakeTest` and `enableDeltaLakePlanningTest`, and run:

```bash
./run-regression-test.sh --run -d external_table_p2/deltalake -s test_deltalake_planning
```

This verifies cold/warm metadata COUNT and the streaming plan. Do not issue ordinary data queries against this fixture. Its absent Parquet objects also make an accidental fallback from metadata count fail visibly.

## Local measurement

Measured on 2026-10-30 with Java 17, Kernel 4.4.1, an 18-logical-CPU/48-GiB macOS host and a 512-MiB test JVM. The generated checkpoint was 7,685,296 bytes. These are individual runs, not a throughput benchmark or a cross-machine performance guarantee.

| Pass | Snapshot (ms) | Streaming decision (ms) | First range (ms) | Full enumeration (ms) | Highest sampled heap (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cold | 1641.946 | 1.487 | 59.983 | 1863.624 | 199.9 |
| Warm 1 | 0.887 | 0.016 | 25.438 | 1645.469 | 111.0 |
| Warm 2 | 0.289 | 0.023 | 21.849 | 1458.675 | 112.9 |

The cold snapshot includes first-use library initialization. Heap peaks are sampled, not continuous maxima. The streaming decision does not enumerate AddFiles. The warm runs reuse snapshot metadata while Kernel still enumerates the checkpoint.

Against local MinIO, Doris metadata COUNT returned 1,000,000 in 1.648 and 1.372 seconds. Both profiles recorded zero data `FileReadBytes` and `FileReadCalls`. This count measurement is separate from the JVM range-enumeration test above.

## Phase timing and cache decision

`DeltaLakePlanningMetricsTest` uses the same opt-in property and heap command as the scale test. It times Kernel handler/iterator boundaries and expression evaluation, then creates the same range DTO shape used for this unpartitioned fixture. This probe is separate from the complete connector source measurement: it omits storage URI normalization, authentication-context entry and source buffering. Read/decode includes filesystem reads and Parquet decoding; replay/iteration is the remainder inside Kernel iteration. These are combined phase costs, not a claim to isolate disk wait from CPU time.

| Probe | Checkpoint read/decode (ms) | Stats JSON parsing (ms) | Expression evaluation (ms) | Replay/iteration (ms) | Range DTO construction (ms) | Selected files |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| No filter | 310.370 | 0 | 2.118 | 226.255 | 1137.967 | 1,000,000 |
| `id < 0` (all files skipped) | 272.070 | 365.928 | 220.749 | 167.768 | 0 | 0 |

For this local workload, M1 keeps `DefaultEngine` without adding a log/checkpoint content cache: snapshot loading already becomes sub-millisecond on a hit, while range construction and stats/replay work account for most enumeration time. A byte cache cannot remove those costs, and would need additional memory accounting and ownership rules. The production planner instead avoids copying an immutable range when the split covers the complete file, and avoids re-entering the authentication context when `hasNext()` already has a buffered range. Remote high-latency storage or a long JSON-only history may have a different balance and should be measured before choosing a content cache.

An isolated repeat of the complete-source test after those two changes produced:

| Pass | Snapshot (ms) | Streaming decision (ms) | First range (ms) | Full enumeration (ms) | Highest sampled heap (MiB) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Cold | 1564.612 | 1.203 | 43.802 | 1529.518 | 169.7 |
| Warm 1 | 0.782 | 0.013 | 21.083 | 1472.836 | 132.7 |
| Warm 2 | 0.248 | 0.020 | 19.747 | 1418.931 | 131.6 |

Both runs used the same fixture and 512-MiB heap. Individual timings and heap samples depend on JIT/GC state; this is evidence of preserved scale behavior and the observed improvement, not a statistical performance claim.
