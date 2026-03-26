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

suite("test_paimon_table_meta_cache", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    String catalogWithCache = "test_paimon_table_cache_with_cache"
    String catalogNoCache = "test_paimon_table_cache_no_cache"
    String testDb = "paimon_cache_test_db"

    sql """drop catalog if exists ${catalogWithCache}"""
    sql """drop catalog if exists ${catalogNoCache}"""

    sql """
        CREATE CATALOG ${catalogWithCache} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true'
        );
    """

    sql """
        CREATE CATALOG ${catalogNoCache} PROPERTIES (
            'type' = 'paimon',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.path.style.access' = 'true',
            'meta.cache.paimon.table.ttl-second' = '0'
        );
    """

    try {
        spark_paimon "CREATE DATABASE IF NOT EXISTS paimon.${testDb}"

        // ==================== Test 1: DML (INSERT) ====================
        logger.info("========== Test 1: DML (INSERT) ==========")
        spark_paimon "DROP TABLE IF EXISTS paimon.${testDb}.test_insert"
        spark_paimon "CREATE TABLE paimon.${testDb}.test_insert (id INT, name STRING) USING paimon"
        spark_paimon "INSERT INTO paimon.${testDb}.test_insert VALUES (1, 'initial')"

        sql """switch ${catalogWithCache}"""
        def result1 = sql """select * from ${testDb}.test_insert order by id"""
        assertEquals(1, result1.size())

        sql """switch ${catalogNoCache}"""
        def result1NoCache = sql """select * from ${testDb}.test_insert order by id"""
        assertEquals(1, result1NoCache.size())

        spark_paimon "INSERT INTO paimon.${testDb}.test_insert VALUES (2, 'external_insert')"

        sql """switch ${catalogWithCache}"""
        def result2 = sql """select * from ${testDb}.test_insert order by id"""
        assertEquals(1, result2.size())

        sql """switch ${catalogNoCache}"""
        def result2NoCache = sql """select * from ${testDb}.test_insert order by id"""
        assertEquals(2, result2NoCache.size())

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_insert"""
        def result3 = sql """select * from ${testDb}.test_insert order by id"""
        assertEquals(2, result3.size())

        // ==================== Test 2: Schema Change (ADD COLUMN) ====================
        logger.info("========== Test 2: Schema Change (ADD COLUMN) ==========")
        spark_paimon "DROP TABLE IF EXISTS paimon.${testDb}.test_add_column"
        spark_paimon "CREATE TABLE paimon.${testDb}.test_add_column (id INT, name STRING) USING paimon"
        spark_paimon "INSERT INTO paimon.${testDb}.test_add_column VALUES (1, 'test')"

        sql """switch ${catalogWithCache}"""
        def addColDesc1 = sql """desc ${testDb}.test_add_column"""
        assertEquals(2, addColDesc1.size())

        sql """switch ${catalogNoCache}"""
        def addColDesc1NoCache = sql """desc ${testDb}.test_add_column"""
        assertEquals(2, addColDesc1NoCache.size())

        spark_paimon "ALTER TABLE paimon.${testDb}.test_add_column ADD COLUMNS (new_col INT)"

        sql """switch ${catalogWithCache}"""
        def addColDesc2 = sql """desc ${testDb}.test_add_column"""
        assertEquals(2, addColDesc2.size())

        sql """switch ${catalogNoCache}"""
        def addColDesc2NoCache = sql """desc ${testDb}.test_add_column"""
        assertEquals(3, addColDesc2NoCache.size())

        sql """switch ${catalogWithCache}"""
        sql """refresh table ${testDb}.test_add_column"""
        def addColDesc3 = sql """desc ${testDb}.test_add_column"""
        assertEquals(3, addColDesc3.size())
    } finally {
        try {
            spark_paimon "DROP TABLE IF EXISTS paimon.${testDb}.test_insert"
            spark_paimon "DROP TABLE IF EXISTS paimon.${testDb}.test_add_column"
        } catch (Exception e) {
            logger.warn("Failed to drop paimon tables: ${e.message}".toString())
        }
        sql """drop catalog if exists ${catalogWithCache}"""
        sql """drop catalog if exists ${catalogNoCache}"""
    }
}
