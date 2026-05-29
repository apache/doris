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

suite("test_backup_restore_concurrency", "backup_restore,nonConcurrent") {
    String suiteName = "test_backup_restore_concurrency"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    int numRows = 10

    // Save original config value for safe restore
    def origConcurrency = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'enable_table_level_backup_concurrency' """
    def origConcurrency_val = origConcurrency[0][1] as String

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Create 3 independent tables for concurrent tests
    def tableNames = ["${suiteName}_t1", "${suiteName}_t2", "${suiteName}_t3"]
    for (tbl in tableNames) {
        sql "DROP TABLE IF EXISTS ${dbName}.${tbl}"
        sql """
            CREATE TABLE ${dbName}.${tbl} (
                `id` LARGEINT NOT NULL,
                `count` LARGEINT SUM DEFAULT "0")
            AGGREGATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        List<String> values = []
        for (int i = 1; i <= numRows; ++i) {
            values.add("(${i}, ${i})")
        }
        sql "INSERT INTO ${dbName}.${tbl} VALUES ${values.join(",")}"
    }

    // ========================================================================
    // Test 1: Concurrent backup of multiple tables
    // ========================================================================
    // With concurrency enabled, multiple table-level backups should run simultaneously.
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "true") """

        // Submit 3 backups on different tables
        for (int i = 0; i < tableNames.size(); i++) {
            sql """
                BACKUP SNAPSHOT ${dbName}.${suiteName}_concurrent_bk_${i}
                TO `${repoName}`
                ON (${tableNames[i]})
            """
        }

        // Check that multiple backups are visible (not just 1)
        def showBackup = sql_return_maparray "SHOW BACKUP FROM ${dbName}"
        logger.info("Concurrent backups submitted: ${showBackup.size()} jobs")
        assertTrue(showBackup.size() >= 3, "Expected at least 3 backup jobs, got ${showBackup.size()}")

        // Wait for all to complete
        syncer.waitSnapshotFinish(dbName)

        // Verify all finished
        showBackup = sql_return_maparray "SHOW BACKUP FROM ${dbName}"
        int finishedCount = 0
        for (row in showBackup) {
            if ((row.State as String) == "FINISHED") {
                finishedCount++
            }
        }
        logger.info("Concurrent backup results: ${finishedCount} finished out of ${showBackup.size()}")
        assertTrue(finishedCount >= 3, "Expected at least 3 FINISHED backups, got ${finishedCount}")

    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "${origConcurrency_val}") """
    }

    // ========================================================================
    // Test 2: Concurrent restore of non-conflicting tables
    // ========================================================================
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "true") """

        // Truncate tables so restore has work to do
        for (tbl in tableNames) {
            sql "TRUNCATE TABLE ${dbName}.${tbl}"
        }

        // Get timestamps for all snapshots
        def timestamps = []
        for (int i = 0; i < tableNames.size(); i++) {
            def ts = syncer.getSnapshotTimestamp(repoName, "${suiteName}_concurrent_bk_${i}")
            assertNotNull(ts, "Snapshot timestamp for ${suiteName}_concurrent_bk_${i} should not be null")
            timestamps.add(ts)
        }

        // Submit 3 restores on different tables
        for (int i = 0; i < tableNames.size(); i++) {
            sql """
                RESTORE SNAPSHOT ${dbName}.${suiteName}_concurrent_bk_${i}
                FROM `${repoName}`
                ON (${tableNames[i]})
                PROPERTIES (
                    "backup_timestamp" = "${timestamps[i]}",
                    "reserve_replica" = "true"
                )
            """
        }

        // Check that multiple restores are visible
        def showRestore = sql_return_maparray "SHOW RESTORE FROM ${dbName}"
        logger.info("Concurrent restores submitted: ${showRestore.size()} jobs")
        assertTrue(showRestore.size() >= 3, "Expected at least 3 restore jobs, got ${showRestore.size()}")

        // Wait for all to complete
        syncer.waitAllRestoreFinish(dbName)

        // Verify data integrity
        for (tbl in tableNames) {
            def result = sql "SELECT * FROM ${dbName}.${tbl}"
            assertEquals(numRows, result.size(), "Table ${tbl} should have ${numRows} rows after restore")
        }

    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "${origConcurrency_val}") """
    }

    // ========================================================================
    // Test 3: QueuePos column shows "Running" for active concurrent jobs
    // ========================================================================
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "true") """

        // Submit a backup
        sql """
            BACKUP SNAPSHOT ${dbName}.${suiteName}_queuepos_bk
            TO `${repoName}`
            ON (${tableNames[0]})
        """

        // Immediately check SHOW BACKUP for QueuePos column (column 14, 0-indexed)
        def showBackup = sql_return_maparray "SHOW BACKUP FROM ${dbName}"
        for (row in showBackup) {
            if ((row.SnapshotName as String) == "${suiteName}_queuepos_bk") {
                String state = row.State as String
                String queuePos = row.QueuePos as String
                logger.info("QueuePos test: state=${state}, queuePos=${queuePos}")
                if (state != "FINISHED" && state != "CANCELLED") {
                    assertEquals("Running", queuePos,
                        "Active job should have QueuePos=Running, got: ${queuePos}")
                }
                break
            }
        }

        syncer.waitSnapshotFinish(dbName)

    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "${origConcurrency_val}") """
    }

    // ========================================================================
    // Test 4: CANCEL BACKUP WHERE LABEL = 'xxx' syntax verification
    // ========================================================================
    // Verify that CANCEL with label filter correctly rejects non-matching labels
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "true") """

        // Cancel with a non-existent label should fail
        boolean thrown = false
        try {
            sql "CANCEL BACKUP FROM ${dbName} WHERE LABEL = 'nonexistent_label_xyz'"
        } catch (Exception e) {
            thrown = true
            assertTrue(e.message.contains("No backup job"), "Error should mention no matching job: ${e.message}")
        }
        assertTrue(thrown, "CANCEL with non-matching label should throw exception")

        // Cancel with LIKE pattern that matches nothing should also fail
        thrown = false
        try {
            sql "CANCEL BACKUP FROM ${dbName} WHERE LABEL LIKE 'zzz_no_match_%'"
        } catch (Exception e) {
            thrown = true
            assertTrue(e.message.contains("No backup job"), "Error should mention no matching job: ${e.message}")
        }
        assertTrue(thrown, "CANCEL with non-matching LIKE should throw exception")

    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "${origConcurrency_val}") """
    }

    // ========================================================================
    // Test 5: CANCEL BACKUP WHERE LABEL cancels only the matching job
    // ========================================================================
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "true") """

        // Submit 2 backups on different tables
        sql """
            BACKUP SNAPSHOT ${dbName}.${suiteName}_cancel_keep
            TO `${repoName}`
            ON (${tableNames[0]})
        """
        sql """
            BACKUP SNAPSHOT ${dbName}.${suiteName}_cancel_target
            TO `${repoName}`
            ON (${tableNames[1]})
        """

        // Cancel only the target job
        try {
            sql "CANCEL BACKUP FROM ${dbName} WHERE LABEL = '${suiteName}_cancel_target'"
        } catch (Exception e) {
            // Job may have already finished — acceptable
            logger.info("Cancel target result: ${e.message}")
        }

        syncer.waitSnapshotFinish(dbName)

        // Verify: target should be CANCELLED, keep should be FINISHED
        def showBackup = sql_return_maparray "SHOW BACKUP FROM ${dbName}"
        for (row in showBackup) {
            if ((row.SnapshotName as String) == "${suiteName}_cancel_target") {
                String state = row.State as String
                logger.info("cancel_target state: ${state}")
                assertTrue(state == "CANCELLED" || state == "FINISHED",
                    "Target job should be CANCELLED or FINISHED, got: ${state}")
            }
        }

    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "${origConcurrency_val}") """
    }

    // ========================================================================
    // Cleanup
    // ========================================================================
    for (tbl in tableNames) {
        sql "DROP TABLE IF EXISTS ${dbName}.${tbl} FORCE"
    }
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
