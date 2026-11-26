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

suite("test_paimon_timestamp_ns", "p0,external,paimon") {
    if (!context.config.otherConfigs.get("enablePaimonTest")?.equalsIgnoreCase("true")) {
        return
    }
    String host = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalog = "test_paimon_timestamp_ns"
    def zone = sql("SELECT @@time_zone")[0][0]
    // Read the deprecated option by its stable name; SHOW VARIABLES prefixes its display name.
    def scanner = sql("SELECT @@enable_file_scanner_v2")[0][0]
    def jni = sql("SHOW VARIABLES LIKE 'force_jni_scanner'")[0][1]
    try {
        sql "DROP CATALOG IF EXISTS ${catalog}"
        sql """CREATE CATALOG ${catalog} PROPERTIES (
            "type"="paimon", "paimon.catalog.type"="filesystem", "warehouse"="s3://warehouse/wh",
            "s3.endpoint"="http://${host}:${minioPort}", "s3.access_key"="admin",
            "s3.secret_key"="password", "s3.region"="us-east-1", "s3.path.style.access"="true")"""
        sql "SWITCH ${catalog}"
        sql "CREATE DATABASE IF NOT EXISTS timestamp_ns_roundtrip"
        sql "USE timestamp_ns_roundtrip"
        for (String format : ["parquet", "orc"]) {
            for (boolean primaryKey : [false, true]) {
                String table = "ns_${format}_${primaryKey}"
                String key = primaryKey ? ', "primary-key"="id", "bucket"="1"' : ', "bucket"="-1"'
                sql "DROP TABLE IF EXISTS ${table}"
                sql """CREATE TABLE ${table} (id INT NOT NULL, ts TIMESTAMP_NS,
                    items ARRAY<TIMESTAMP_NS>, detail STRUCT<t:TIMESTAMP_NS>, entries MAP<STRING,TIMESTAMP_NS>)
                    ENGINE=paimon PROPERTIES ("file.format"="${format}" ${key})"""
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
                "qt_${table}_schema" "DESC ${table}"
                for (boolean v2 : [false, true]) {
                    sql "SET enable_file_scanner_v2=${v2}"
                    for (boolean forceJni : [false, true]) {
                        sql "SET force_jni_scanner=${forceJni}"
                        for (String sessionZone : ["UTC", "Asia/Shanghai"]) {
                            sql "SET time_zone='${sessionZone}'"
                            String tag = "${table}_${v2}_${forceJni}_${sessionZone.replace('/', '_')}"
                            "qt_${tag}_values" "SELECT * FROM ${table} ORDER BY id"
                            // Filter-only high-precision columns also require lossless materialization.
                            "qt_${tag}_eq" """SELECT id FROM ${table}
                                WHERE ts=CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP_NS) ORDER BY id"""
                            "qt_${tag}_nested" """SELECT id FROM ${table} WHERE
                                items[1]=CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP_NS) ORDER BY id"""
                            "qt_${tag}_aggregate" "SELECT min(ts), max(ts), count(DISTINCT ts) FROM ${table}"
                        }
                    }
                }
                if (!primaryKey) {
                    sql "SET enable_file_scanner_v2=true"
                    sql "SET force_jni_scanner=false"
                    // Automatic lossless JNI reads must retain physical metadata, including after filtering.
                    def metadata = sql("""SELECT id, CAST(ts AS STRING), __paimon_file_path, __paimon_row_index
                        FROM ${table} ORDER BY id""")
                    "qt_${table}_metadata" """SELECT id, CAST(ts AS STRING),
                        __paimon_file_path IS NOT NULL, __paimon_row_index >= 0 FROM ${table} ORDER BY id"""
                    "qt_${table}_metadata_unique" """SELECT COUNT(*) FROM
                        (SELECT DISTINCT __paimon_file_path, __paimon_row_index FROM ${table}) positions"""
                    assertEquals([metadata[0]], sql("""SELECT id, CAST(ts AS STRING),
                        __paimon_file_path, __paimon_row_index FROM ${table}
                        WHERE ts=CAST('2024-01-02 03:04:05.123456789' AS TIMESTAMP_NS)"""))
                }
                sql "DROP TABLE IF EXISTS ${table}_copy"
                sql """CREATE TABLE ${table}_copy PROPERTIES ("file.format"="${format}")
                    AS SELECT * FROM ${table}"""
                "qt_${table}_copy" "SELECT * FROM ${table}_copy ORDER BY id"
                if (primaryKey) {
                    sql """INSERT INTO ${table} VALUES (1, '2024-01-02 03:04:05.123456790', NULL, NULL, NULL)"""
                    "qt_${table}_upsert" "SELECT id, ts FROM ${table} ORDER BY id"
                }
            }
        }
    } finally {
        sql "SET time_zone='${zone}'"
        sql "SET enable_file_scanner_v2=${scanner}"
        sql "SET force_jni_scanner=${jni}"
        sql "SWITCH internal"
    }
}
