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

# Build the EMR fixtures

Start with the [shared preparation and migration workflow](../README.md).

These scripts reproduce the fixtures for `test_hive_migrate_iceberg`,
`test_hive_migrate_partition_iceberg` and `test_hive_migrate_paimon`.
Run them only in a dedicated, fresh Hive warehouse. Existing target tables cause
an error; the scripts do not drop or overwrite tables. They are initialization
scripts, not idempotent repair scripts. After a partial failure, inspect the
created tables and resume at the failed statement rather than rerunning inserts.

The migration history matters: create ordinary Hive files, import those files
with the format's migration procedure, append data, and (for Iceberg) rename
columns before another append. Creating only the final rows loses coverage of
Hive files without field IDs, nested schema evolution and partition import.

| Namespace | Tables (both `parquet` and `orc`) | Final rows per table |
| --- | --- | --- |
| `regression` | `hive_migrate_<format>` | 4 |
| `regression_iceberg` | `hive_migrate_partition_<format>` | 5 |
| `regression_paimon` | `hive_migrate_paimon_<format>` | 4 |

The Paimon source tables are `regression_paimon.hive_origin_<format>` and remain
available. Iceberg's migration procedure also retains Hive backup tables by
default. Preserve those until verification is complete.

## Initialize

The recipes were exercised with Hive 3.1.3, Spark 3.4.2, Iceberg 1.5.0 and the
EMR Paimon 0.9-ali-7 runtime. Use the matching connector jars for your Spark
version. The installed Paimon procedure accepts five parameters and has no
`parallelism` argument. Newer runtimes can have different procedure signatures.
For Kerberos environments, obtain a ticket using the operator-provided service
principal and keytab before running Hive or Spark. Do not disable authentication.

Set `SPARK_SQL`, `ICEBERG_JAR`, `PAIMON_JAR`, `HMS_URI` and `HDFS_WAREHOUSE` for the
target installation. Hive and Spark must load that installation's Hive and
Hadoop configuration, including HMS authentication.

```bash
FIXTURES=regression-test/script/external_table_p2/emr
hive --service cli -S -f "$FIXTURES/hive_sources.sql"

"$SPARK_SQL" --master 'local[2]' --driver-memory 2g \
  --jars "$ICEBERG_JAR" \
  --conf spark.ui.enabled=false \
  --conf "spark.sql.warehouse.dir=$HDFS_WAREHOUSE" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  -f "$FIXTURES/iceberg_migration.sql"

"$SPARK_SQL" --master 'local[2]' --driver-memory 2g \
  --jars "$PAIMON_JAR" \
  --conf spark.ui.enabled=false \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.metastore=hive \
  --conf "spark.sql.catalog.paimon.uri=$HMS_URI" \
  --conf "spark.sql.catalog.paimon.warehouse=$HDFS_WAREHOUSE" \
  -f "$FIXTURES/paimon_migration.sql"
```

## Verify through Doris

Use a private regression config with `enableExternalEmrTest=true`,
`enableExternalIcebergTest=true` and `enablePaimonTest=true`. Supply the existing
`emrCatalogCommonProp` string with the HMS URI and required Hadoop/HMS properties.
Set `emrPaimonWarehouse` to `HDFS_WAREHOUSE`; otherwise the suite retains its
original warehouse default. Every FE and BE that accesses the catalog must have
the configured keytab and Kerberos realm configuration, and network access to
the KDC, HMS, NameNode and DataNodes.

```bash
./run-regression-test.sh --run --conf /path/to/private-regression-conf.groovy \
  -d external_table_p2/iceberg,external_table_p2/paimon \
  -s test_hive_migrate_iceberg,test_hive_migrate_partition_iceberg,test_hive_migrate_paimon \
  -parallel 1
```

Use the existing golden results. Do not regenerate expected results to accommodate
fixture errors.

## ORC merged-stream fixture

`GenerateOrcMergeFixture.java` constructs a deterministic replacement for the
external file formerly called `004691_0` in `test_orc_merge_io_input_streams`.
It uses the same seven-column schema and contains 60,000 rows in three ZLIB
stripes, each with two 10,000-row groups. Within each stripe the first group has
`trace_id='1210647802'` and the second has `trace_id='1210647804'`. The missing
target `1210647803` is inside every stripe's min/max range but outside each
row group's range. All rows have `dt='20250415'`.

This forces the stream-lifecycle transition from [Doris #50185](https://github.com/apache/doris/pull/50185):
stripe statistics pass, row-group statistics reject every group, and the reader
advances to another stripe in the same call. The stream map must be cleared for
each stripe. The suite disables condition cache, keeps the file in one split,
and checks the query Profile for row-group rejection, one reader, multiple stripe
visits and merged IO. An empty result caused by a cache or file-level filter
therefore fails the test. Its existing golden results remain 0 and 60,000.

The generator was exercised with the ORC Java 1.8.6 jars bundled with Spark
3.4.2, including `hive-storage-api` 2.8.1. Set `SPARK_JARS` to the installation's
`jars` directory. Use the same Kerberos ticket preparation described above.
Run with `-ea` so the generator verifies stripe counts, row counts, compression,
index stride and stripe-level trace bounds before registration.

```bash
FIXTURES=regression-test/script/external_table_p2/emr
mkdir -p /tmp/doris-orc-merge-fixture/classes
javac -cp "$SPARK_JARS/*" -d /tmp/doris-orc-merge-fixture/classes \
  "$FIXTURES/GenerateOrcMergeFixture.java"
java -ea -cp "/tmp/doris-orc-merge-fixture/classes:$SPARK_JARS/*" \
  GenerateOrcMergeFixture /tmp/doris-orc-merge-fixture/orc_merge_io_input_streams.orc

ORC_MERGE_LOCATION=/user/hive/warehouse/regression.db/test_orc_merge_io_input_streams_table
hdfs dfs -mkdir "$ORC_MERGE_LOCATION"
hdfs dfs -put /tmp/doris-orc-merge-fixture/orc_merge_io_input_streams.orc "$ORC_MERGE_LOCATION/"
hive --service cli -S --hivevar orc_merge_location="$ORC_MERGE_LOCATION" \
  -f "$FIXTURES/orc_merge_table.sql"

./run-regression-test.sh --run --conf /path/to/private-regression-conf.groovy \
  -d external_table_p2/hive -s test_orc_merge_io_input_streams -parallel 1
```

Create a fresh output file, HDFS directory and table. The commands refuse existing
targets. The Hive registration SQL uses an external table, leaving the generated
file available for inspection and backup.
