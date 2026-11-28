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

import org.apache.doris.regression.action.ProfileAction

suite("test_paimon_rust_write_tables", "p0,external,paimon,nonConcurrent") {
    if (!context.config.otherConfigs.get("enablePaimonTest")?.equalsIgnoreCase("true")) {
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.test_paimon_rust_tables_db;
        DROP TABLE IF EXISTS paimon.test_paimon_rust_tables_db.t_fixed;
        CREATE TABLE paimon.test_paimon_rust_tables_db.t_fixed (
            id BIGINT, region STRING, seq BIGINT, payload STRING
        ) USING paimon PARTITIONED BY (region)
        TBLPROPERTIES ('primary-key'='id,region', 'bucket'='4', 'bucket-key'='id',
            'sequence.field'='seq', 'file.format'='parquet');
        DROP TABLE IF EXISTS paimon.test_paimon_rust_tables_db.t_typed_key;
        CREATE TABLE paimon.test_paimon_rust_tables_db.t_typed_key (
            id DECIMAL(20,4), event_time TIMESTAMP_NTZ, payload STRING
        ) USING paimon TBLPROPERTIES ('primary-key'='id,event_time', 'bucket'='4', 'file.format'='parquet');
        DROP TABLE IF EXISTS paimon.test_paimon_rust_tables_db.t_dynamic;
        CREATE TABLE paimon.test_paimon_rust_tables_db.t_dynamic (
            id BIGINT, seq BIGINT, payload STRING
        ) USING paimon TBLPROPERTIES ('primary-key'='id', 'bucket'='-1',
            'sequence.field'='seq', 'file.format'='parquet');
        DROP TABLE IF EXISTS paimon.test_paimon_rust_tables_db.t_cross;
        CREATE TABLE paimon.test_paimon_rust_tables_db.t_cross (
            id BIGINT, region STRING, seq BIGINT, payload STRING
        ) USING paimon PARTITIONED BY (region)
        TBLPROPERTIES ('primary-key'='id', 'bucket'='-1',
            'file.format'='parquet');
        DROP TABLE IF EXISTS paimon.test_paimon_rust_tables_db.t_append;
        CREATE TABLE paimon.test_paimon_rust_tables_db.t_append (id INT, region STRING)
        USING paimon PARTITIONED BY (region)
        TBLPROPERTIES ('bucket'='2', 'bucket-key'='id', 'file.format'='parquet');
        DROP TABLE IF EXISTS paimon.test_paimon_rust_tables_db.t_partial;
        CREATE TABLE paimon.test_paimon_rust_tables_db.t_partial (id INT, name STRING, score INT)
        USING paimon TBLPROPERTIES ('primary-key'='id', 'bucket'='2',
            'merge-engine'='partial-update', 'file.format'='parquet');
        DROP TABLE IF EXISTS paimon.test_paimon_rust_tables_db.t_first;
        CREATE TABLE paimon.test_paimon_rust_tables_db.t_first (id INT, payload STRING)
        USING paimon TBLPROPERTIES ('primary-key'='id', 'bucket'='2',
            'merge-engine'='first-row', 'file.format'='parquet');
        DROP TABLE IF EXISTS paimon.test_paimon_rust_tables_db.t_aggregation;
        CREATE TABLE paimon.test_paimon_rust_tables_db.t_aggregation (id INT, total BIGINT)
        USING paimon TBLPROPERTIES ('primary-key'='id', 'bucket'='2',
            'merge-engine'='aggregation', 'fields.total.aggregate-function'='sum', 'file.format'='parquet');
    """
    sql "DROP CATALOG IF EXISTS test_paimon_rust_tables_catalog"
    sql """CREATE CATALOG test_paimon_rust_tables_catalog PROPERTIES (
        'type'='paimon', 'paimon.catalog.type'='filesystem', 'warehouse'='s3://warehouse/wh',
        's3.endpoint'='http://${externalEnvIp}:${minioPort}', 's3.region'='us-east-1',
        's3.access_key'='admin', 's3.secret_key'='password', 'use_path_style'='true')"""
    sql "SWITCH test_paimon_rust_tables_catalog"
    sql "USE test_paimon_rust_tables_db"
    def originalRust = sql "SELECT @@enable_paimon_rust_writer"
    def originalScanner = sql "SELECT @@force_jni_scanner"
    def originalParallelism = sql "SELECT @@parallel_pipeline_task_num"
    def originalProfile = sql "SELECT @@enable_profile"
    def profiles = new ProfileAction(context)
    def checkWriter = { String backend = "Rust FFI", String reason = "supported_append" ->
        String queryId = sql("SELECT last_query_id()")[0][0].toString()
        profiles.getProfile(queryId, ["PaimonWriteBackend: ${backend}".toString(),
                "PaimonWriterSelectionReason: ${reason}".toString(),
                "PaimonTransactionState: COMMITTED", "Profile Completion State: COMPLETE"])
    }
    def compareWithSpark = { String table ->
        assertSparkDorisResultEquals(
                spark_paimon("SELECT * FROM paimon.test_paimon_rust_tables_db.${table} ORDER BY id"),
                sql("SELECT * FROM ${table} ORDER BY id"))
    }
    try {
        sql "SET enable_profile=true"
        sql "SET force_jni_scanner=true"
        sql "SET parallel_pipeline_task_num=4"
        sql "SET enable_paimon_rust_writer=false"
        sql "INSERT INTO t_fixed SELECT 1, 'east', 10, 'java-base'"
        checkWriter("Java JNI", "rust_writer_disabled")
        sql "INSERT INTO t_dynamic SELECT 1, 10, 'java-base'"
        checkWriter("Java JNI", "rust_writer_disabled")
        sql """INSERT INTO t_typed_key SELECT CAST('1234567890123456.1234' AS DECIMAL(20,4)),
            CAST('2026-11-20 12:34:56.123456' AS DATETIMEV2(6)), 'java-base'
            UNION ALL SELECT CAST('-1.2500' AS DECIMAL(20,4)),
            CAST('1969-12-31 23:59:59.999999' AS DATETIMEV2(6)), 'java-negative'"""
        checkWriter("Java JNI", "rust_writer_disabled")
        sql "SET enable_paimon_rust_writer=true"

        // The higher user sequence must win even if the lower sequence arrives later.
        sql """INSERT INTO t_fixed SELECT 1, 'east', 20, 'new'
            UNION ALL SELECT 1, 'east', 15, 'old'
            UNION ALL SELECT 2, 'west', 10, NULL"""
        checkWriter()
        order_qt_fixed_sequence "SELECT * FROM t_fixed WHERE id < 100 ORDER BY id"
        test {
            sql """INSERT INTO t_fixed VALUES (99, 'east', 10, 'must-not-commit'),
                (NULL, 'east', 10, 'invalid-key')"""
            exception "Paimon Rust write failed, SDK error code=4"
        }
        qt_fixed_failed_insert_count "SELECT COUNT(*) FROM t_fixed"
        // Fixed-bucket routing is shared with JNI and assigns a bucket to one writer.
        sql """CREATE DATABASE IF NOT EXISTS internal.test_paimon_rust_tables_db"""
        sql "DROP TABLE IF EXISTS internal.test_paimon_rust_tables_db.t_source"
        sql """CREATE TABLE internal.test_paimon_rust_tables_db.t_source (
            id BIGINT, region STRING, seq BIGINT, payload STRING
        ) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 8
        PROPERTIES ('replication_num'='1')"""
        sql """INSERT INTO internal.test_paimon_rust_tables_db.t_source
            SELECT number + 100, IF(number % 2 = 0, 'east', 'west'), 1, CONCAT('row-', number)
            FROM numbers('number'='2048')"""
        sql """INSERT INTO t_fixed
            SELECT * FROM internal.test_paimon_rust_tables_db.t_source"""
        checkWriter()
        qt_fixed_summary "SELECT COUNT(*), MIN(id), MAX(id) FROM t_fixed"
        compareWithSpark("t_fixed")
        order_qt_fixed_rust_reader """SELECT /*+ SET_VAR(enable_file_scanner_v2=true,
            enable_paimon_rust_reader=true, force_jni_scanner=false) */ *
            FROM t_fixed WHERE id < 100 ORDER BY id"""
        String readQueryId = sql("SELECT last_query_id()")[0][0].toString()
        profiles.getProfile(readQueryId, ["PaimonRustReader", "Profile Completion State: COMPLETE"])

        // These keys require the SDK's bucket hash rather than Doris native routing.
        // Updating the Java key must stay in the same bucket and leave exactly one row.
        sql """INSERT INTO t_typed_key SELECT CAST('1234567890123456.1234' AS DECIMAL(20,4)),
            CAST('2026-11-20 12:34:56.123456' AS DATETIMEV2(6)), 'rust-new'
            UNION ALL SELECT CAST('-1.2500' AS DECIMAL(20,4)),
            CAST('1969-12-31 23:59:59.999999' AS DATETIMEV2(6)), 'negative-time'"""
        checkWriter()
        order_qt_typed_primary_key """SELECT CAST(id AS STRING), CAST(event_time AS STRING), payload
            FROM t_typed_key ORDER BY id"""
        assertSparkDorisResultEquals(
                spark_paimon("""SELECT CAST(id AS STRING), CAST(event_time AS STRING), payload
                    FROM paimon.test_paimon_rust_tables_db.t_typed_key ORDER BY id"""),
                sql("""SELECT CAST(id AS STRING), CAST(event_time AS STRING), payload
                    FROM t_typed_key ORDER BY id"""))

        // A new Rust writer must restore the Java snapshot and retain dynamic HASH indexes.
        sql """INSERT INTO t_dynamic SELECT 1, 20, 'rust-new'
            UNION ALL SELECT 2, 10, CAST(NULL AS STRING)"""
        checkWriter()
        sql "INSERT INTO t_dynamic SELECT 1, 15, 'stale' UNION ALL SELECT 3, 10, 'third'"
        checkWriter()
        order_qt_dynamic "SELECT * FROM t_dynamic ORDER BY id"
        compareWithSpark("t_dynamic")
        setBeConfigTemporary([paimon_rust_writer_memory_limit_bytes: 1]) {
            test {
                sql "INSERT INTO t_dynamic VALUES (1, 30, 'must-not-commit')"
                exception "exceeds writer budget 1"
            }
        }
        order_qt_dynamic_after_failure "SELECT * FROM t_dynamic ORDER BY id"

        // Cumulative primary-key input exceeds 64 MiB. The bounded sort buffer
        // must flush repeatedly instead of retaining the whole INSERT.
        setBeConfigTemporary([paimon_rust_writer_memory_limit_bytes: 67108864]) {
            sql """INSERT INTO t_dynamic
                SELECT number + 1000, 1, REPEAT('x', 4096) FROM numbers('number'='20000')"""
            checkWriter()
        }
        qt_dynamic_large_input """SELECT COUNT(*), MIN(id), MAX(id), SUM(LENGTH(payload))
            FROM t_dynamic WHERE id >= 1000"""
        assertSparkDorisResultEquals(
                spark_paimon("""SELECT COUNT(*), MIN(id), MAX(id), SUM(LENGTH(payload))
                    FROM paimon.test_paimon_rust_tables_db.t_dynamic WHERE id >= 1000"""),
                sql("""SELECT COUNT(*), MIN(id), MAX(id), SUM(LENGTH(payload))
                    FROM t_dynamic WHERE id >= 1000"""))

        // Cross-partition dynamic buckets use the existing single-writer distribution.
        sql "INSERT INTO t_cross SELECT 1, 'east', 10, 'initial'"
        checkWriter()
        sql "INSERT INTO t_cross SELECT 1, 'west', 20, 'moved'"
        checkWriter()
        order_qt_cross_partition "SELECT * FROM t_cross ORDER BY id"
        compareWithSpark("t_cross")

        sql "INSERT INTO t_append SELECT 1, CAST(NULL AS STRING) UNION ALL SELECT 2, 'east'"
        checkWriter()
        order_qt_fixed_append "SELECT * FROM t_append ORDER BY id"
        compareWithSpark("t_append")
        sql "INSERT OVERWRITE TABLE t_append SELECT 3, 'north'"
        checkWriter("Rust FFI", "supported_overwrite")
        order_qt_append_overwrite "SELECT * FROM t_append ORDER BY id"
        compareWithSpark("t_append")

        sql "INSERT INTO t_partial SELECT 1, 'alice', 10 UNION ALL SELECT 2, 'bob', 20"
        checkWriter()
        sql "INSERT INTO t_partial (score, id) SELECT 15, 1"
        checkWriter()
        order_qt_partial_update "SELECT * FROM t_partial ORDER BY id"
        compareWithSpark("t_partial")
        sql "INSERT INTO t_first SELECT 1, 'first'"
        checkWriter("Java JNI", "first_row_requires_compaction")
        sql "INSERT INTO t_first SELECT 1, 'second' UNION ALL SELECT 2, 'another'"
        checkWriter("Java JNI", "first_row_requires_compaction")
        order_qt_first_row "SELECT * FROM t_first ORDER BY id"
        compareWithSpark("t_first")
        sql "INSERT INTO t_aggregation SELECT 1, 10 UNION ALL SELECT 2, 5"
        checkWriter()
        sql "INSERT INTO t_aggregation SELECT 1, 20 UNION ALL SELECT 2, 3"
        checkWriter()
        order_qt_aggregation "SELECT * FROM t_aggregation ORDER BY id"
        compareWithSpark("t_aggregation")

        sql "INSERT OVERWRITE TABLE t_fixed PARTITION(region='east') SELECT 9, 30, 'replacement'"
        checkWriter("Rust FFI", "supported_overwrite")
        order_qt_static_overwrite "SELECT * FROM t_fixed WHERE id < 100 ORDER BY id"
        qt_static_overwrite_count "SELECT COUNT(*) FROM t_fixed"
        compareWithSpark("t_fixed")
        // No commit messages still has to replace the selected partition.
        sql """INSERT OVERWRITE TABLE t_fixed PARTITION(region='east')
            SELECT 9, 30, 'empty' WHERE FALSE"""
        order_qt_empty_partition_overwrite "SELECT * FROM t_fixed WHERE id < 100 ORDER BY id"
        compareWithSpark("t_fixed")
        sql "INSERT OVERWRITE TABLE t_dynamic SELECT 8, 1, 'replacement'"
        checkWriter("Rust FFI", "supported_overwrite")
        order_qt_full_overwrite "SELECT * FROM t_dynamic ORDER BY id"
        compareWithSpark("t_dynamic")
    } finally {
        sql "SET enable_paimon_rust_writer=${originalRust[0][0]}"
        sql "SET force_jni_scanner=${originalScanner[0][0]}"
        sql "SET parallel_pipeline_task_num=${originalParallelism[0][0]}"
        sql "SET enable_profile=${originalProfile[0][0]}"
    }
}
