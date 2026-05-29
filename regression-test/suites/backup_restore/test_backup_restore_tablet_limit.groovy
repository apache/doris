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

suite("test_backup_restore_tablet_limit", "backup_restore,nonConcurrent") {
    String suiteName = "test_backup_restore_tablet_limit"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"

    // Save original config values for safe restore in finally blocks
    def origMaxTablets = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'max_backup_tablets_per_job' """
    def origMaxTablets_val = origMaxTablets[0][1] as String
    def origConcurrency = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'enable_table_level_backup_concurrency' """
    def origConcurrency_val = origConcurrency[0][1] as String
    def origGlobalLimit = sql """ ADMIN SHOW FRONTEND CONFIG LIKE 'max_concurrent_snapshot_tasks_total' """
    def origGlobalLimit_val = origGlobalLimit[0][1] as String

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"
    def result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size())

    // ========== Test 1: per-job tablet limit rejects backup ==========
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("max_backup_tablets_per_job" = "0") """

        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName}_limit
            TO `${repoName}`
            ON (${tableName})
        """

        syncer.waitSnapshotFinish(dbName)

        def showBackup = sql "SHOW BACKUP FROM ${dbName}"
        logger.info("SHOW BACKUP after per-job limit: ${showBackup}")
        // State should be CANCELLED
        assertTrue(showBackup.size() > 0)
        def lastBackup = showBackup[showBackup.size() - 1]
        assertEquals("CANCELLED", lastBackup[3] as String)
        assertTrue((lastBackup[12] as String).contains("exceeds the limit"))
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("max_backup_tablets_per_job" = "${origMaxTablets_val}") """
    }

    // ========== Test 2: global snapshot limit rejects backup in concurrent mode ==========
    // max_concurrent_snapshot_tasks_total=1 means only 1 total snapshot task allowed globally;
    // any table with >= 2 tablets (this table has 2 buckets) will exceed the limit.
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "true") """
        sql """ ADMIN SET FRONTEND CONFIG ("max_concurrent_snapshot_tasks_total" = "1") """

        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName}_global
            TO `${repoName}`
            ON (${tableName})
        """

        syncer.waitSnapshotFinish(dbName)

        def showBackup = sql "SHOW BACKUP FROM ${dbName}"
        logger.info("SHOW BACKUP after global limit: ${showBackup}")
        assertTrue(showBackup.size() > 0)
        def lastBackup = showBackup[showBackup.size() - 1]
        assertEquals("CANCELLED", lastBackup[3] as String)
        assertTrue((lastBackup[12] as String).contains("would exceed the limit"))
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("max_concurrent_snapshot_tasks_total" = "${origGlobalLimit_val}") """
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "${origConcurrency_val}") """
    }

    // ========== Test 3: global limit bypassed when concurrency disabled ==========
    // With concurrency disabled, max_concurrent_snapshot_tasks_total is ignored entirely
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "false") """
        sql """ ADMIN SET FRONTEND CONFIG ("max_concurrent_snapshot_tasks_total" = "1") """

        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName}_bypass
            TO `${repoName}`
            ON (${tableName})
        """

        syncer.waitSnapshotFinish(dbName)

        def showBackup = sql "SHOW BACKUP FROM ${dbName}"
        logger.info("SHOW BACKUP after bypass: ${showBackup}")
        assertTrue(showBackup.size() > 0)
        def lastBackup = showBackup[showBackup.size() - 1]
        // Should succeed (FINISHED) since global limit is ignored without concurrency
        assertEquals("FINISHED", lastBackup[3] as String)
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("max_concurrent_snapshot_tasks_total" = "${origGlobalLimit_val}") """
        sql """ ADMIN SET FRONTEND CONFIG ("enable_table_level_backup_concurrency" = "${origConcurrency_val}") """
    }

    // ========== Test 4: normal backup and restore with default limits ==========
    def normalSnapshot = "${snapshotName}_normal"
    sql """
        BACKUP SNAPSHOT ${dbName}.${normalSnapshot}
        TO `${repoName}`
        ON (${tableName})
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, normalSnapshot)
    assertTrue(snapshot != null)

    sql "TRUNCATE TABLE ${dbName}.${tableName}"

    sql """
        RESTORE SNAPSHOT ${dbName}.${normalSnapshot}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size())

    // ========== Test 5: restore per-job tablet limit rejects restore ==========
    // Use the snapshot from Test 4 to test restore rejection
    try {
        sql """ ADMIN SET FRONTEND CONFIG ("max_backup_tablets_per_job" = "0") """

        sql "TRUNCATE TABLE ${dbName}.${tableName}"

        sql """
            RESTORE SNAPSHOT ${dbName}.${normalSnapshot}
            FROM `${repoName}`
            ON ( `${tableName}`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true"
            )
        """

        syncer.waitAllRestoreFinish(dbName)

        def showRestore = sql "SHOW RESTORE FROM ${dbName}"
        logger.info("SHOW RESTORE after per-job limit: ${showRestore}")
        assertTrue(showRestore.size() > 0)
        def lastRestore = showRestore[showRestore.size() - 1]
        assertEquals("CANCELLED", lastRestore[4] as String)
        assertTrue((lastRestore[19] as String).contains("exceeds the limit"))
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("max_backup_tablets_per_job" = "${origMaxTablets_val}") """
    }

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
