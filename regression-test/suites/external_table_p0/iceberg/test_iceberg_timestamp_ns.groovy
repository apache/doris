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

suite("test_iceberg_timestamp_ns", "p0,external,iceberg") {
    if (!context.config.otherConfigs.get("enableIcebergTest")?.equalsIgnoreCase("true")) {
        return
    }
    // This suite was written against the dynamic overwrite scope (the previous default). Pin it explicitly
    // now that iceberg_partition_overwrite_mode defaults to 'static'.
    sql """ set iceberg_partition_overwrite_mode = 'dynamic' """

    String host = context.config.otherConfigs.get("externalEnvIp")
    // The newer REST fixture preserves nanosecond schema defaults; the baseline 1.10 server rescales them.
    String restPort = context.config.otherConfigs.get("iceberg_scan_planning_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog = "test_iceberg_timestamp_ns"
    def zone = sql("SELECT @@time_zone")[0][0]
    // Read the deprecated option by its stable name; SHOW VARIABLES prefixes its display name.
    def scanner = sql("SELECT @@enable_file_scanner_v2")[0][0]
    try {
        sql "SET enable_file_scanner_v2=true"
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type"="iceberg", "iceberg.catalog.type"="rest", "uri"="http://${host}:${restPort}",
            "s3.endpoint"="http://${host}:${minioPort}", "s3.access_key"="admin",
            "s3.secret_key"="password", "s3.region"="us-east-1", "use_path_style"="true")"""
        sql "SWITCH ${catalog}"
        sql "CREATE DATABASE IF NOT EXISTS timestamp_ns_roundtrip"
        sql "USE timestamp_ns_roundtrip"
        for (String format : ["parquet", "orc"]) {
            String table = "ns_${format}"
            sql "DROP TABLE IF EXISTS ${table}"
            sql """CREATE TABLE ${table} (id INT, ts TIMESTAMP_NS, items ARRAY<TIMESTAMP_NS>,
                    detail STRUCT<t:TIMESTAMP_NS>, entries MAP<STRING,TIMESTAMP_NS>)
                PROPERTIES ("format-version"="3", "write.format.default"="${format}")"""
            sql "SET time_zone='Asia/Shanghai'"
            sql """INSERT INTO ${table} VALUES
                (1, '2024-01-02 03:04:05.123456789', ['2024-01-02 03:04:05.123456789', NULL],
                    named_struct('t', CAST('1969-12-31 23:59:58.999999999' AS TIMESTAMP_NS)),
                    map('x', CAST('1970-01-01 00:00:00.000000001' AS TIMESTAMP_NS))),
                (2, '2024-01-02 03:04:05.123456788', [], NULL, map()),
                (3, '1969-12-31 23:59:58.999999999', NULL, NULL, NULL),
                (4, '1970-01-01 00:00:00.000000001', NULL, NULL, NULL),
                (5, NULL, NULL, NULL, NULL),
                (6, '1677-09-21 00:12:43.145224192', NULL, NULL, NULL),
                (7, '2262-04-11 23:47:16.854775807', NULL, NULL, NULL)"""
            "qt_${format}_schema" "DESC ${table}"
            // TVF schema discovery must preserve nanos before the V2 scan constructs target columns.
            def expectedRows = sql("SELECT * FROM ${table} ORDER BY id")
            def tvfRows = []
            def files = sql("SELECT file_path FROM ${table}\$files WHERE content=0")
            assertTrue(!files.isEmpty())
            for (def file : files) {
                String tvf = """S3("uri"="${file[0]}", "format"="${format}",
                    "s3.endpoint"="http://${host}:${minioPort}", "s3.access_key"="admin",
                    "s3.secret_key"="password", "s3.region"="us-east-1", "use_path_style"="true")"""
                if (file == files[0]) {
                    "qt_${format}_tvf_schema" "DESC FUNCTION ${tvf}"
                }
                tvfRows.addAll(sql("SELECT * FROM ${tvf}"))
            }
            assertEquals(expectedRows, tvfRows.sort { a, b -> a[0] <=> b[0] })
            for (String sessionZone : ["UTC", "Asia/Shanghai"]) {
                sql "SET time_zone='${sessionZone}'"
                String tag = "${format}_${sessionZone.replace('/', '_')}"
                "qt_${tag}_values" "SELECT * FROM ${table} ORDER BY id"
                // The two positive timestamps differ only in the last nanosecond.
                "qt_${tag}_eq" """SELECT id FROM ${table}
                    WHERE ts=CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP_NS) ORDER BY id"""
                "qt_${tag}_range" """SELECT id FROM ${table}
                    WHERE ts<CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP_NS) ORDER BY id"""
                "qt_${tag}_aggregate" "SELECT min(ts), max(ts), count(DISTINCT ts) FROM ${table}"
            }
            sql "DROP TABLE IF EXISTS ${table}_copy"
            sql """CREATE TABLE ${table}_copy PROPERTIES ("format-version"="3",
                "write.format.default"="${format}") AS SELECT * FROM ${table}"""
            "qt_${format}_copy" "SELECT * FROM ${table}_copy ORDER BY id"
            // Filtered rewrites must bind a nanosecond literal without scaling it as microseconds.
            def beforeRewrite = sql("SELECT * FROM ${table} ORDER BY id")
            sql """ALTER TABLE ${table} EXECUTE rewrite_data_files("rewrite-all"="true")
                WHERE ts='2024-01-02 03:04:05.123456789'"""
            assertEquals(beforeRewrite, sql("SELECT * FROM ${table} ORDER BY id"))

            String defaultsTable = "${table}_defaults"
            sql "DROP TABLE IF EXISTS ${defaultsTable}"
            sql """CREATE TABLE ${defaultsTable} (id INT) PROPERTIES
                ("format-version"="3", "write.format.default"="${format}")"""
            // Exercise both metadata default parsing and the subsequent statement-scoped sink schema.
            sql """ALTER TABLE ${defaultsTable} ADD COLUMN ts TIMESTAMP_NS
                DEFAULT '2024-01-02 03:04:05.123456789'"""
            sql "INSERT INTO ${defaultsTable} (id) VALUES (1)"
            sql "INSERT INTO ${defaultsTable} VALUES (2, '2024-01-02 03:04:05.123456788')"
            "qt_${format}_defaults" "SELECT id, CAST(ts AS STRING) FROM ${defaultsTable} ORDER BY id"
            // Compact SQL literals must persist the same initial and write defaults as canonical text.
            sql """ALTER TABLE ${defaultsTable} ADD COLUMN compact_date TIMESTAMP_NS DEFAULT '20240102'"""
            sql """ALTER TABLE ${defaultsTable} ADD COLUMN compact_datetime TIMESTAMP_NS
                DEFAULT '20240102030405'"""
            sql "INSERT INTO ${defaultsTable} (id) VALUES (3)"
            "qt_${format}_compact_defaults" """SELECT id, CAST(compact_date AS STRING),
                CAST(compact_datetime AS STRING) FROM ${defaultsTable} ORDER BY id"""
            for (String transform : ["identity", "year", "month", "day", "hour", "bucket"]) {
                String partitionTable = "${table}_${transform}"
                String expression = transform == "identity" ? "ts"
                        : transform == "bucket" ? "bucket(16, ts)" : "${transform}(ts)"
                sql "DROP TABLE IF EXISTS ${partitionTable}"
                sql """CREATE TABLE ${partitionTable} (id INT, ts TIMESTAMP_NS)
                    PARTITION BY LIST (${expression}) ()
                    PROPERTIES ("format-version"="3", "write.format.default"="${format}")"""
                sql "INSERT INTO ${partitionTable} SELECT id, ts FROM ${table}"
                "qt_${format}_${transform}" "SELECT * FROM ${partitionTable} ORDER BY id"
                sql "INSERT OVERWRITE TABLE ${partitionTable} SELECT id, ts FROM ${table} WHERE id=2"
                "qt_${format}_${transform}_overwrite" "SELECT * FROM ${partitionTable} ORDER BY id"
                if (transform == "identity") {
                    // Static partition commit parsing must accept the same date-only input as the sink cast.
                    sql "INSERT OVERWRITE TABLE ${partitionTable} PARTITION(ts='2024-01-01') SELECT 8"
                    "qt_${format}_identity_static_date" "SELECT CAST(ts AS STRING) FROM ${partitionTable} WHERE id=8"
                    // An offset literal must replace the sink's normalized partition, preserving midnight.
                    sql "SET time_zone='UTC'"
                    sql "INSERT INTO ${partitionTable} VALUES (9, '2023-12-31 16:00:00')"
                    sql """INSERT OVERWRITE TABLE ${partitionTable}
                        PARTITION(ts='2024-01-01 00:00:00+08:00') SELECT 10"""
                    "qt_${format}_identity_static_offset" "SELECT * FROM ${partitionTable} ORDER BY id"
                    sql "SET time_zone='Asia/Shanghai'"
                }
            }
        }
    } finally {
        sql "SET time_zone='${zone}'"
        sql "SET enable_file_scanner_v2=${scanner}"
        sql "SWITCH internal"
    }
}
