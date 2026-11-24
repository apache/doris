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

# EMR migration fixtures for external P2

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
FIXTURES=tools/emr_storage_regression/p2_fixtures
hive --service cli -S -f "$FIXTURES/01-hive-sources.sql"

"$SPARK_SQL" --master 'local[2]' --driver-memory 2g \
  --jars "$ICEBERG_JAR" \
  --conf spark.ui.enabled=false \
  --conf "spark.sql.warehouse.dir=$HDFS_WAREHOUSE" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  -f "$FIXTURES/02-iceberg-migrate.sql"

"$SPARK_SQL" --master 'local[2]' --driver-memory 2g \
  --jars "$PAIMON_JAR" \
  --conf spark.ui.enabled=false \
  --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions \
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.metastore=hive \
  --conf "spark.sql.catalog.paimon.uri=$HMS_URI" \
  --conf "spark.sql.catalog.paimon.warehouse=$HDFS_WAREHOUSE" \
  -f "$FIXTURES/03-paimon-migrate.sql"
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
fixture errors. The separate ORC stream regression needs its original `004691_0`
file; these scripts do not reproduce that file or its physical layout.
