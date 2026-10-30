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
import org.apache.doris.regression.util.Http

suite("test_paimon_rust_write_append", "p0,external,paimon") {
    if (!context.config.otherConfigs.get("enablePaimonTest")?.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")

    spark_paimon_multi """
        CREATE DATABASE IF NOT EXISTS paimon.test_paimon_rust_write_db;
        DROP TABLE IF EXISTS paimon.test_paimon_rust_write_db.t_append;
        CREATE TABLE paimon.test_paimon_rust_write_db.t_append (
            id BIGINT, payload STRING, amount DECIMAL(18, 2), event_date DATE
        ) USING paimon
        TBLPROPERTIES ('bucket' = '-1', 'file.format' = 'parquet');
        DROP TABLE IF EXISTS paimon.test_paimon_rust_write_db.t_partitioned;
        CREATE TABLE paimon.test_paimon_rust_write_db.t_partitioned (
            id BIGINT, region STRING
        ) USING paimon PARTITIONED BY (region)
        TBLPROPERTIES ('bucket' = '-1', 'file.format' = 'parquet');
        DROP TABLE IF EXISTS paimon.test_paimon_rust_write_db.t_defaults;
        CREATE TABLE paimon.test_paimon_rust_write_db.t_defaults (
            id BIGINT, value INT DEFAULT 42
        ) USING paimon
        TBLPROPERTIES ('bucket' = '-1', 'file.format' = 'parquet');
    """
    sql "DROP CATALOG IF EXISTS test_paimon_rust_write_catalog"
    sql """
        CREATE CATALOG test_paimon_rust_write_catalog PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.region' = 'us-east-1',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            'use_path_style' = 'true'
        )
    """
    sql "SWITCH test_paimon_rust_write_catalog"
    sql "USE test_paimon_rust_write_db"
    def originalRustWriter = sql "SELECT @@enable_paimon_rust_writer"
    def originalJniScanner = sql "SELECT @@force_jni_scanner"
    def originalParallelism = sql "SELECT @@parallel_pipeline_task_num"
    def originalProfile = sql "SELECT @@enable_profile"
    def profiles = new ProfileAction(context)
    def checkWriter = { String backend, String reason, List counters ->
        String queryId = sql("SELECT last_query_id()")[0][0].toString()
        def required = ["PaimonWriteBackend: ${backend}".toString(),
                        "PaimonWriterSelectionReason: ${reason}".toString(),
                        "PaimonTableMetadata: Thrift table descriptor",
                        "PaimonTransactionState: COMMITTED", "Profile Completion State: COMPLETE"]
        required.addAll(counters)
        profiles.getProfile(queryId, required)
    }
    try {
        sql "SET enable_profile=true"
        // Leave room under the BE-wide Rust writer admission cap.
        sql "SET parallel_pipeline_task_num = 4"
        sql "SET force_jni_scanner = true"
        sql "SET enable_paimon_rust_writer = false"
        sql "INSERT INTO t_append SELECT 0, 'java', 1.25, DATE '2026-01-01'"
        checkWriter("Java JNI", "rust_writer_disabled", ["NativePageMemoryPeak"])
        sql "SET enable_paimon_rust_writer = true"
        sql """INSERT INTO t_append VALUES
            (1, 'rust', -2.50, DATE '2026-02-02'), (2, NULL, NULL, NULL)"""
        // One writer must flush while its cumulative input exceeds the 64 MiB budget.
        // Repeated strings keep encoded Parquet small, so raw input must trigger flush.
        sql "SET parallel_pipeline_task_num = 1"
        setBeConfigTemporary([paimon_rust_writer_memory_limit_bytes: 67108864]) {
            sql """
                INSERT INTO t_append
                SELECT number + 10, repeat('x', 4096),
                    CAST(number AS DECIMAL(18, 2)), DATE '2026-03-03'
                FROM numbers("number" = "20000")
            """
            checkWriter("Rust FFI", "supported_append", ["InputBatches", "InputRows", "RustWriteTime",
                    "QueryReservedBytes: 0", "ArrowBufferBytes: 0", "MemoryAccounting"])
        }
        sql "SET parallel_pipeline_task_num = 4"
        // This Rust-specific error also proves the supported table selected FFI.
        test {
            sql """INSERT INTO t_append
                VALUES (3, repeat('x', 1048576), 0.00, DATE '2026-01-01')"""
            exception "Paimon Rust input row exceeds 1 MiB"
        }
        setBeConfigTemporary([paimon_rust_writer_memory_limit_bytes: 1]) {
            test {
                sql "INSERT INTO t_append VALUES (4, 'over-budget', 0.00, DATE '2026-01-01')"
                exception "exhausted its resource budget"
            }
        }
        // Failed input must not change the committed snapshot. Read Rust output
        // through Java in both Doris and Spark, together with the earlier JNI file.
        qt_append_summary """SELECT COUNT(*), SUM(id), SUM(amount), SUM(length(payload))
            FROM t_append"""
        order_qt_append_rows "SELECT * FROM t_append WHERE id < 10"
        assertSparkDorisResultEquals(
                spark_paimon("""SELECT COUNT(*), SUM(id), SUM(amount), SUM(length(payload))
                    FROM paimon.test_paimon_rust_write_db.t_append"""),
                sql("SELECT COUNT(*), SUM(id), SUM(amount), SUM(length(payload)) FROM t_append"))
        assertSparkDorisResultEquals(
                spark_paimon("""SELECT * FROM paimon.test_paimon_rust_write_db.t_append
                    WHERE id < 10 ORDER BY id"""),
                sql("SELECT * FROM t_append WHERE id < 10 ORDER BY id"))

        // Partitioned writes select JNI before input is consumed.
        sql "INSERT INTO t_partitioned SELECT 1, 'east' UNION ALL SELECT 2, 'west'"
        checkWriter("Java JNI", "partitioned_table", [])
        assertSparkDorisResultEquals(
                spark_paimon("""SELECT * FROM paimon.test_paimon_rust_write_db.t_partitioned
                    ORDER BY id"""),
                sql("SELECT * FROM t_partitioned ORDER BY id"))

        // Java applies schema defaults even to explicit NULLs. These tables must
        // select JNI while the Rust file-store writer lacks that behavior.
        sql "INSERT INTO t_defaults SELECT 1, CAST(NULL AS INT) UNION ALL SELECT 2, 7"
        checkWriter("Java JNI", "schema_defaults", [])
        order_qt_schema_defaults "SELECT * FROM t_defaults"
        assertSparkDorisResultEquals(
                spark_paimon("""SELECT * FROM paimon.test_paimon_rust_write_db.t_defaults
                    ORDER BY id"""),
                sql("SELECT * FROM t_defaults ORDER BY id"))
        def hosts = [:]
        def ports = [:]
        getBackendIpHttpPort(hosts, ports)
        boolean sawMetrics = false
        ports.each { id, port ->
            String metrics = Http.GET("http://${hosts[id]}:${port}/metrics", false, false).toString()
            def active = metrics =~ /(?m)^doris_be_paimon_rust_active_writers\s+([0-9.]+)/
            if (active.find()) {
                sawMetrics = true
                assertEquals(0.0d, active.group(1).toDouble())
                def credit = metrics =~ /(?m)^doris_be_paimon_rust_fixed_memory_bytes\s+([0-9.]+)/
                assertTrue(credit.find())
                assertEquals(0.0d, credit.group(1).toDouble())
            }
        }
        assertTrue(sawMetrics)
    } finally {
        sql "SET enable_profile = ${originalProfile[0][0]}"
        sql "SET enable_paimon_rust_writer = ${originalRustWriter[0][0]}"
        sql "SET force_jni_scanner = ${originalJniScanner[0][0]}"
        sql "SET parallel_pipeline_task_num = ${originalParallelism[0][0]}"
    }
}
