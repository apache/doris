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
