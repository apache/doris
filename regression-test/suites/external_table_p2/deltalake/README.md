# Delta planning scale acceptance

This opt-in fixture declares one million active AddFiles in a classic checkpoint. It deliberately creates **no data objects**: it tests metadata COUNT and FE range planning, not Parquet read throughput or correctness across one million physical objects. The ordinary Delta p0 suites cover real data, deletes and schema evolution.

Install PyArrow 21.0.0 in a Python environment. Use the `only-checkpoint-files` golden table from Delta v4.4.1 (commit `cebdc44dc07d331383e72811177af4ffc8638990`), renamed `only_checkpoint_files` by the fixture importer. Generate into a warehouse/database/table layout:

```bash
python3 docker/thirdparties/docker-compose/iceberg/scripts/create_preinstalled_scripts/deltalake/generate_planning_fixture.py \
  --golden-table /path/to/golden/only_checkpoint_files \
  --output /path/to/warehouse/delta_bench/million_files
```

The source golden table is Apache-2.0 licensed. Its original source is [Delta's pinned golden tables](https://github.com/delta-io/delta/tree/cebdc44dc07d331383e72811177af4ffc8638990/connectors/golden-tables/only-checkpoint-files). The generator uses its checkpoint schema and protocol/metadata, replaces the table id, and writes synthetic AddFiles plus the checkpoint's own JSON commit. Older commits are intentionally absent.

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
