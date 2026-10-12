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
    String host = context.config.otherConfigs.get("externalEnvIp")
    // The newer REST fixture preserves nanosecond schema defaults; the baseline 1.10 server rescales them.
    String restPort = context.config.otherConfigs.get("iceberg_scan_planning_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog = "test_iceberg_timestamp_ns"
    def zone = sql("SELECT @@time_zone")[0][0]
    def scanner = sql("SHOW VARIABLES LIKE 'enable_file_scanner_v2'")[0][1]
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
                }
            }
        }
    } finally {
        sql "SET time_zone='${zone}'"
        sql "SET enable_file_scanner_v2=${scanner}"
        sql "SWITCH internal"
    }
}
