// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_iceberg_partition_enforced_filters",
        "p0,external,iceberg,external_docker,external_docker_iceberg") {
    if (!"true".equalsIgnoreCase(context.config.otherConfigs.get("enableIcebergTest"))) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    def oldScanner = sql("show variables like 'enable_file_scanner_v2'")[0][1]
    def oldBatch = sql("show variables like 'enable_external_table_batch_mode'")[0][1]
    def oldBatchSize = sql("show variables like 'num_files_in_batch_mode'")[0][1]

    spark_iceberg_multi """
        CREATE DATABASE IF NOT EXISTS demo.partition_enforced_filters_db;
        DROP TABLE IF EXISTS demo.partition_enforced_filters_db.transformed;
        DROP TABLE IF EXISTS demo.partition_enforced_filters_db.unpartitioned;
        CREATE TABLE demo.partition_enforced_filters_db.transformed (
            id INT, ts TIMESTAMP_NTZ, amount INT
        ) USING iceberg PARTITIONED BY (months(ts))
        TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');
        INSERT INTO demo.partition_enforced_filters_db.transformed VALUES
            (1, CAST('2026-10-12 00:00:00.000001' AS TIMESTAMP_NTZ), 10),
            (2, CAST('2026-10-13 01:00:00' AS TIMESTAMP_NTZ), 20);
        ALTER TABLE demo.partition_enforced_filters_db.transformed
            REPLACE PARTITION FIELD months(ts) WITH days(ts);
        INSERT INTO demo.partition_enforced_filters_db.transformed VALUES
            (3, CAST('2026-10-12 13:00:00' AS TIMESTAMP_NTZ), 30),
            (4, CAST('2026-10-12 20:00:00' AS TIMESTAMP_NTZ), 40),
            (5, NULL, 50);
        INSERT INTO demo.partition_enforced_filters_db.transformed VALUES
            (6, CAST('2026-10-13 02:00:00' AS TIMESTAMP_NTZ), 60);
        CREATE TABLE demo.partition_enforced_filters_db.unpartitioned
        USING iceberg TBLPROPERTIES ('write.format.default'='parquet')
        AS SELECT * FROM demo.partition_enforced_filters_db.transformed;
    """
    sql "drop catalog if exists iceberg_partition_enforced_filters"
    sql """create catalog iceberg_partition_enforced_filters properties (
        'type'='iceberg', 'iceberg.catalog.type'='rest',
        'uri'='http://${host}:${restPort}',
        's3.endpoint'='http://${host}:${minioPort}',
        's3.access_key'='admin', 's3.secret_key'='password', 's3.region'='us-east-1'
    )"""
    String db = "iceberg_partition_enforced_filters.partition_enforced_filters_db"
    String wholeDay = "ts >= '2026-10-12 00:00:00' and ts < '2026-10-13 00:00:00'"
    def queries = [
        "select sum(amount) from SOURCE_TABLE where ${wholeDay}",
        "select id, ts, amount from SOURCE_TABLE where ${wholeDay} order by id",
        "select sum(amount) from SOURCE_TABLE where ${wholeDay} and ts >= '2026-10-12 13:00:00'",
        "select sum(amount) from SOURCE_TABLE where ts is null",
        "select sum(amount) from SOURCE_TABLE where ts is not null",
        "select sum(amount) from SOURCE_TABLE where ts != '2026-10-12 13:00:00'",
        "select sum(amount) from SOURCE_TABLE where ts not in ('2026-10-12 13:00:00', null)",
        "select sum(t.amount) from SOURCE_TABLE t join ${db}.unpartitioned b on t.ts = b.ts " +
                "where b.id = 3 and t.ts >= '2026-10-12 00:00:00' and t.ts < '2026-10-13 00:00:00'"
    ]
    try {
        sql "set num_files_in_batch_mode = 1"
        [false, true].each { scanner ->
            sql "set enable_file_scanner_v2 = ${scanner}"
            [false, true].each { batch ->
                sql "set enable_external_table_batch_mode = ${batch}"
                queries.each { query ->
                    // The unpartitioned copy cannot supply a partition proof. Compare real rows
                    // across old month/new day specs, scanners, projection shapes and RF joins.
                    def expected = sql(query.replace("SOURCE_TABLE", "${db}.unpartitioned"))
                    def actual = sql(query.replace("SOURCE_TABLE", "${db}.transformed"))
                    assertEquals(expected, actual)
                }
            }
        }
    } finally {
        sql "set enable_file_scanner_v2 = ${oldScanner}"
        sql "set enable_external_table_batch_mode = ${oldBatch}"
        sql "set num_files_in_batch_mode = ${oldBatchSize}"
    }
}
