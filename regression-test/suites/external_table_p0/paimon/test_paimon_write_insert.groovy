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

suite("test_paimon_write_insert", "p0,external") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String hmsPort = context.config.otherConfigs.get("hive2HmsPort")
    String hdfsPort = context.config.otherConfigs.get("hive2HdfsPort")
    if (externalEnvIp == null || externalEnvIp.isEmpty()
            || hmsPort == null || hmsPort.isEmpty()
            || hdfsPort == null || hdfsPort.isEmpty()) {
        logger.info("Paimon write insert test environment is not fully configured, skip this test")
        return
    }

    String catalogName = "paimon_write_insert_e2e"
    String dbName = "paimon_write_insert_db"
    String warehouse = "hdfs://${externalEnvIp}:${hdfsPort}/user/doris/paimon/write_insert_e2e"

    try {
        sql """switch internal"""
        sql """set enable_fallback_to_original_planner=false"""
        sql """drop catalog if exists ${catalogName}"""
        sql """
            CREATE CATALOG ${catalogName} PROPERTIES (
                'type' = 'paimon',
                'paimon.catalog.type' = 'hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hmsPort}',
                'warehouse' = '${warehouse}'
            )
        """

        sql """switch ${catalogName}"""

        sql """drop database if exists ${dbName} force"""
        sql """create database ${dbName}"""
        sql """use ${dbName}"""

        // ========== Create table types ==========

        // 1. Primary-key table, partitioned, no fixed bucket (dynamic bucket)
        //    bucket=-1 only supports JNI writer (paimon-cpp does not support dynamic bucket)
        sql """drop table if exists pk_partition_no_bucket"""
        sql """
            CREATE TABLE pk_partition_no_bucket (
                id INT,
                value STRING,
                dt STRING
            ) ENGINE = paimon
            PARTITION BY (dt) ()
            PROPERTIES (
                'primary-key' = 'id, dt',
                'bucket' = '-1',
                'file.format' = 'parquet'
            )
        """

        // 2. Append-only table, partitioned, no fixed bucket
        sql """drop table if exists append_partition_no_bucket"""
        sql """
            CREATE TABLE append_partition_no_bucket (
                id INT,
                value STRING,
                dt STRING
            ) ENGINE = paimon
            PARTITION BY (dt) ()
            PROPERTIES (
                'file.format' = 'parquet'
            )
        """

        // 3. Primary-key table, partitioned, with fixed bucket
        sql """drop table if exists pk_partition_bucketed"""
        sql """
            CREATE TABLE pk_partition_bucketed (
                id INT,
                value STRING,
                dt STRING
            ) ENGINE = paimon
            PARTITION BY (dt) ()
            PROPERTIES (
                'primary-key' = 'id, dt',
                'bucket' = '2',
                'file.format' = 'parquet'
            )
        """

        // 4. Append-only table, partitioned, with fixed bucket
        sql """drop table if exists append_partition_bucketed"""
        sql """
            CREATE TABLE append_partition_bucketed (
                id INT,
                value STRING,
                dt STRING
            ) ENGINE = paimon
            PARTITION BY (dt) ()
            PROPERTIES (
                'bucket' = '2',
                'bucket-key' = 'id',
                'file.format' = 'parquet'
            )
        """

        // 5. Unpartitioned primary-key table with multiple data types
        sql """drop table if exists pk_no_partition"""
        sql """
            CREATE TABLE pk_no_partition (
                id INT,
                name STRING,
                score DECIMAL(10, 2),
                create_date DATE
            ) ENGINE = paimon
            PROPERTIES (
                'primary-key' = 'id',
                'bucket' = '2',
                'file.format' = 'parquet'
            )
        """

        // 6. Unpartitioned append-only table with multiple data types
        sql """drop table if exists append_no_partition"""
        sql """
            CREATE TABLE append_no_partition (
                id INT,
                name STRING,
                score DECIMAL(10, 2),
                create_date DATE
            ) ENGINE = paimon
            PROPERTIES (
                'bucket' = '2',
                'bucket-key' = 'id',
                'file.format' = 'parquet'
            )
        """

        // ========== Helper: insert single row and verify count ==========
        def verifyInsert = { boolean enableJni, String tableName, int id, String value, String dt ->
            sql """set enable_paimon_jni_writer=${enableJni}"""
            sql """insert into ${tableName} values (${id}, '${value}', '${dt}')"""
            def result = sql """select count(*) from ${tableName} where id=${id} and dt='${dt}'"""
            assertEquals("1", result[0][0].toString())
        }

        // ========== Test JNI writer (enable_paimon_jni_writer=true) ==========
        // pk_partition_no_bucket (bucket=-1) only supports JNI writer
        verifyInsert(true, "pk_partition_no_bucket", 1, "jni_pk_nb_1", "2026-04-01")
        verifyInsert(true, "pk_partition_no_bucket", 2, "jni_pk_nb_2", "2026-04-02")
        verifyInsert(true, "append_partition_no_bucket", 3, "jni_ap_nb_1", "2026-04-01")
        verifyInsert(true, "append_partition_no_bucket", 4, "jni_ap_nb_2", "2026-04-02")
        verifyInsert(true, "pk_partition_bucketed", 5, "jni_pk_bk_1", "2026-04-01")
        verifyInsert(true, "pk_partition_bucketed", 6, "jni_pk_bk_2", "2026-04-02")
        verifyInsert(true, "append_partition_bucketed", 7, "jni_ap_bk_1", "2026-04-01")
        verifyInsert(true, "append_partition_bucketed", 8, "jni_ap_bk_2", "2026-04-02")

        // ========== Test paimon-cpp writer (enable_paimon_jni_writer=false) ==========
        // pk_partition_no_bucket (bucket=-1) is NOT supported by paimon-cpp, skip it
        verifyInsert(false, "append_partition_no_bucket", 13, "cpp_ap_nb_1", "2026-04-03")
        verifyInsert(false, "append_partition_no_bucket", 14, "cpp_ap_nb_2", "2026-04-04")
        verifyInsert(false, "pk_partition_bucketed", 15, "cpp_pk_bk_1", "2026-04-03")
        verifyInsert(false, "pk_partition_bucketed", 16, "cpp_pk_bk_2", "2026-04-04")
        verifyInsert(false, "append_partition_bucketed", 17, "cpp_ap_bk_1", "2026-04-03")
        verifyInsert(false, "append_partition_bucketed", 18, "cpp_ap_bk_2", "2026-04-04")

        // ========== Verify total row counts in partitioned tables ==========
        def pkNbCount = sql """select count(*) from pk_partition_no_bucket"""
        assertEquals("2", pkNbCount[0][0].toString())

        def apNbCount = sql """select count(*) from append_partition_no_bucket"""
        assertEquals("4", apNbCount[0][0].toString())

        def pkBkCount = sql """select count(*) from pk_partition_bucketed"""
        assertEquals("4", pkBkCount[0][0].toString())

        def apBkCount = sql """select count(*) from append_partition_bucketed"""
        assertEquals("4", apBkCount[0][0].toString())

        // ========== Test multi-row insert on unpartitioned tables ==========
        sql """set enable_paimon_jni_writer=true"""
        sql """
            insert into pk_no_partition values
            (1, 'alice', 95.50, '2026-01-01'),
            (2, 'bob', 87.30, '2026-01-02'),
            (3, 'carol', 92.10, '2026-01-03')
        """
        def jniPkCount = sql """select count(*) from pk_no_partition"""
        assertEquals("3", jniPkCount[0][0].toString())

        sql """set enable_paimon_jni_writer=true"""
        sql """
            insert into append_no_partition values
            (1, 'dave', 78.00, '2026-02-01'),
            (2, 'eve', 88.50, '2026-02-02'),
            (3, 'frank', 91.20, '2026-02-03')
        """
        def jniApCount = sql """select count(*) from append_no_partition"""
        assertEquals("3", jniApCount[0][0].toString())

        sql """set enable_paimon_jni_writer=false"""
        sql """
            insert into pk_no_partition values
            (4, 'grace', 96.80, '2026-01-04'),
            (5, 'hank', 82.40, '2026-01-05')
        """
        def cppPkCount = sql """select count(*) from pk_no_partition"""
        assertEquals("5", cppPkCount[0][0].toString())

        sql """set enable_paimon_jni_writer=false"""
        sql """
            insert into append_no_partition values
            (4, 'ivy', 89.70, '2026-02-04'),
            (5, 'jack', 93.60, '2026-02-05')
        """
        def cppApCount = sql """select count(*) from append_no_partition"""
        assertEquals("5", cppApCount[0][0].toString())

        // ========== Verify data correctness by querying specific rows ==========
        def aliceRow = sql """select name, score, create_date from pk_no_partition where id=1"""
        assertEquals("alice", aliceRow[0][0].toString())
        assertEquals("95.50", aliceRow[0][1].toString())
        assertEquals("2026-01-01", aliceRow[0][2].toString())

        def graceRow = sql """select name, score, create_date from pk_no_partition where id=4"""
        assertEquals("grace", graceRow[0][0].toString())
        assertEquals("96.80", graceRow[0][1].toString())

        // ========== Test primary-key upsert (same id, new value) ==========
        sql """set enable_paimon_jni_writer=true"""
        sql """insert into pk_no_partition values (1, 'alice_updated', 99.00, '2026-01-01')"""
        def upsertResult = sql """select name, score from pk_no_partition where id=1"""
        assertEquals("alice_updated", upsertResult[0][0].toString())
        assertEquals("99.00", upsertResult[0][1].toString())

        def afterUpsertCount = sql """select count(*) from pk_no_partition"""
        assertEquals("5", afterUpsertCount[0][0].toString())

        // ========== Test insert into select ==========
        sql """set enable_paimon_jni_writer=true"""
        sql """drop table if exists append_for_ctas"""
        sql """
            CREATE TABLE append_for_ctas (
                id INT,
                name STRING,
                score DECIMAL(10, 2),
                create_date DATE
            ) ENGINE = paimon
            PROPERTIES (
                'bucket' = '2',
                'bucket-key' = 'id',
                'file.format' = 'parquet'
            )
        """
        sql """insert into append_for_ctas select id, name, score, create_date from pk_no_partition where id <= 3"""
        def ctasCount = sql """select count(*) from append_for_ctas"""
        assertEquals("3", ctasCount[0][0].toString())

    } finally {
        try {
            sql """switch ${catalogName}"""
            sql """drop table if exists ${dbName}.pk_partition_no_bucket"""
            sql """drop table if exists ${dbName}.append_partition_no_bucket"""
            sql """drop table if exists ${dbName}.pk_partition_bucketed"""
            sql """drop table if exists ${dbName}.append_partition_bucketed"""
            sql """drop table if exists ${dbName}.pk_no_partition"""
            sql """drop table if exists ${dbName}.append_no_partition"""
            sql """drop table if exists ${dbName}.append_for_ctas"""
            sql """drop database if exists ${dbName} force"""
        } catch (Exception e) {
            logger.info("Cleanup in catalog ${catalogName} failed: ${e.getMessage()}")
        }
        try {
            sql """switch internal"""
        } catch (Exception e) {
            logger.info("Switch back to internal catalog failed: ${e.getMessage()}")
        }
        sql """drop catalog if exists ${catalogName}"""
    }
}
