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

// Paimon rust reader through a schema-only ALTER: ADD COLUMN with no later data
// commit. The FE pins the data snapshot while keeping the latest schema, and
// ships both in the scan range. Without stripping the fence's scan.snapshot-id
// from the transported schema JSON, paimon-rust's copy_with_time_travel
// re-resolves the selector and swaps the shipped fields for the pinned
// snapshot's older schema, so projecting the added column fails before per-file
// schema evolution can null-fill it — the JNI reader (which keeps the resolved
// schema) succeeds. Both readers must return the historical rows with the added
// column NULL-filled.
suite("test_paimon_rust_reader_schema_evolution", "p0,external,paimon") {
    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disabled paimon test")
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String catalogName = "test_paimon_rust_schema_evo"
    String dbName = "test_paimon_rust_schema_evo_db"
    String tableName = "t_rust_schema_evolution"

    // s3.region is required: paimon-rust's S3 client rejects a missing region,
    // while the JNI reader falls back to the SDK default.
    sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    sql """
        CREATE CATALOG `${catalogName}` PROPERTIES (
            'type' = 'paimon',
            'paimon.catalog.type' = 'filesystem',
            'warehouse' = 's3://warehouse/wh',
            's3.endpoint' = 'http://${externalEnvIp}:${minioPort}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.region' = 'us-east-1',
            'use_path_style' = 'true'
        )
    """
    sql """SWITCH `${catalogName}`"""
    sql """DROP DATABASE IF EXISTS `${dbName}` FORCE"""
    sql """CREATE DATABASE `${dbName}`"""
    sql """USE `${dbName}`"""

    def originalRust = sql("select @@enable_paimon_rust_reader")[0][0]
    def originalForceJni = sql("select @@force_jni_scanner")[0][0]
    def originalV2 = sql("select @@enable_file_scanner_v2")[0][0]

    try {
        sql """
            CREATE TABLE `${tableName}` (
                id INT NOT NULL,
                name STRING NULL
            ) ENGINE=paimon
            PROPERTIES ('primary-key' = 'id')
        """
        sql """INSERT INTO `${tableName}` VALUES (1, 'alice'), (2, 'bob')"""

        // The schema-only ALTER: a new schema is published but no data commit
        // follows it, so the FE's data fence stays on the older snapshot while
        // the resolved schema carries the added column.
        sql """ALTER TABLE `${tableName}` ADD COLUMN added_after STRING NULL AFTER name"""

        sql """SET enable_file_scanner_v2=true"""
        // The table's DataSplits stay logical either way, but pin the JNI/rust
        // reader path explicitly so the differential cannot silently fall back
        // to the native converter.
        sql """SET force_jni_scanner=true"""

        def testQueries = [
                // The added column must project and read back NULL-filled for
                // the historical rows.
                """select id, name, added_after from `${tableName}` order by id""",
                """select * from `${tableName}` order by id""",
                // A predicate on the added column forces a real scan (a
                // conjunct disables the table-level metadata count shortcut).
                """select count(*) from `${tableName}` where added_after is null"""
        ]

        // Baseline through the JNI reader, which keeps the resolved schema.
        sql """SET enable_paimon_rust_reader=false"""
        def jniResults = testQueries.collect { query -> sql(query) }

        sql """SET enable_paimon_rust_reader=true"""
        def rustResults = testQueries.collect { query -> sql(query) }

        assertTrue(rustResults[0].size() > 0)
        for (int i = 0; i < testQueries.size(); i++) {
            assertEquals(jniResults[i].toString(), rustResults[i].toString())
        }
        // Both readers see the two historical rows with the added column
        // NULL-filled.
        assertEquals(2, rustResults[0].size())
        assertTrue(rustResults[0].every { row -> row[2] == null })
        assertEquals(2, rustResults[2][0][0])
    } finally {
        sql """SET enable_paimon_rust_reader=${originalRust}"""
        sql """SET force_jni_scanner=${originalForceJni}"""
        sql """SET enable_file_scanner_v2=${originalV2}"""
        sql """DROP DATABASE IF EXISTS `${dbName}` FORCE"""
        sql """DROP CATALOG IF EXISTS `${catalogName}`"""
    }
}
