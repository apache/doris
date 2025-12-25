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

suite("test_backup_restore_medium_edge_cases", "backup_restore") {
    String suiteName = "test_br_edge"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def enableDebugLog = { ->
        try {
            def (host, port) = context.config.feHttpAddress.split(":")
            def url = "http://${host}:${port}/rest/v1/log?add_verbose=org.apache.doris.backup"
            def result = curl("POST", url)
            logger.info("Enabled debug logging for org.apache.doris.backup: ${result}")
        } catch (Exception e) {
            logger.warn("Failed to enable debug logging: ${e.message}")
        }
    }

    def disableDebugLog = { ->
        try {
            def (host, port) = context.config.feHttpAddress.split(":")
            def url = "http://${host}:${port}/rest/v1/log?del_verbose=org.apache.doris.backup"
            def result = curl("POST", url)
            logger.info("Disabled debug logging: ${result}")
        } catch (Exception e) {
            logger.warn("Failed to disable debug logging: ${e.message}")
        }
    }

    try {
        enableDebugLog()

        def syncer = getSyncer()
        syncer.createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

        // Test 1: Restore with table rename and medium change
        logger.info("=== Test 1: Restore with table alias and medium change ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_original (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_original VALUES (1, 'test')"
        
        // Backup
        sql "BACKUP SNAPSHOT ${dbName}.snap1 TO `${repoName}` ON (${tableName}_original)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot = syncer.getSnapshotTimestamp(repoName, "snap1")
        assertTrue(snapshot != null)

        // Restore with alias (new name) and different medium
        sql """
            RESTORE SNAPSHOT ${dbName}.snap1 FROM `${repoName}`
            ON (`${tableName}_original` AS `${tableName}_renamed`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        // Verify new table exists with correct name
        def tables = sql "SHOW TABLES FROM ${dbName} LIKE '${tableName}_renamed'"
        assertEquals(1, tables.size(), "Renamed table should exist")
        
        // Verify data
        def result = sql "SELECT * FROM ${dbName}.${tableName}_renamed"
        assertEquals(1, result.size())
        
        // Verify properties
        def show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_renamed"
        logger.info("Renamed table: ${show_create[0][1]}")
        assertTrue(show_create[0][1].contains("medium_allocation_mode"))

        sql "DROP TABLE ${dbName}.${tableName}_original FORCE"
        sql "DROP TABLE ${dbName}.${tableName}_renamed FORCE"

        // Test 2: Restore with conflicting table (without atomic restore)
        logger.info("=== Test 2: Restore creates new table when target exists ===")
        
        // Create table v1
        sql """
            CREATE TABLE ${dbName}.${tableName}_v1 (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_v1 VALUES (1, 'v1_data')"
        
        // Backup v1
        sql "BACKUP SNAPSHOT ${dbName}.snap_v1 TO `${repoName}` ON (${tableName}_v1)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_v1")
        
        // Modify v1 data
        sql "INSERT INTO ${dbName}.${tableName}_v1 VALUES (2, 'modified')"
        
        // Restore to alias (will create new table)
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_v1 FROM `${repoName}`
            ON (`${tableName}_v1` AS `${tableName}_v2`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        // Verify v1 still has 2 rows (modified data)
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_v1"
        assertEquals(2, result[0][0])
        
        // Verify v2 has 1 row (original backup data)
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_v2"
        assertEquals(1, result[0][0])

        sql "DROP TABLE ${dbName}.${tableName}_v1 FORCE"
        sql "DROP TABLE ${dbName}.${tableName}_v2 FORCE"

        // Test 3: Restore partitioned table with partial data
        logger.info("=== Test 3: Restore empty partitions with medium settings ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_empty (
                `date` DATE,
                `id` INT
            )
            PARTITION BY RANGE(`date`) (
                PARTITION p1 VALUES LESS THAN ('2024-01-01'),
                PARTITION p2 VALUES LESS THAN ('2024-02-01'),
                PARTITION p3 VALUES LESS THAN ('2024-03-01')
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        // Insert data only in p1 and p3 (p2 is empty)
        sql "INSERT INTO ${dbName}.${tableName}_empty VALUES ('2023-12-15', 1)"
        sql "INSERT INTO ${dbName}.${tableName}_empty VALUES ('2024-02-15', 3)"
        
        // Backup (including empty p2)
        sql "BACKUP SNAPSHOT ${dbName}.snap_empty TO `${repoName}` ON (${tableName}_empty)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_empty")
        
        sql "DROP TABLE ${dbName}.${tableName}_empty FORCE"
        
        // Restore with medium settings
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_empty FROM `${repoName}`
            ON (`${tableName}_empty`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        // Verify all 3 partitions exist (even empty one)
        def partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_empty"
        assertEquals(3, partitions.size(), "All 3 partitions should be restored")
        
        // Verify data (only 2 rows)
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_empty"
        assertEquals(2, result[0][0])

        sql "DROP TABLE ${dbName}.${tableName}_empty FORCE"

        // Test 4: Restore single partition table
        logger.info("=== Test 4: Restore single partition table (non-partitioned) ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_single (
                `id` INT,
                `value` INT
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_single VALUES (1, 100), (2, 200), (3, 300)"
        
        // Backup
        sql "BACKUP SNAPSHOT ${dbName}.snap_single TO `${repoName}` ON (${tableName}_single)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_single")
        
        sql "DROP TABLE ${dbName}.${tableName}_single FORCE"
        
        // Restore with SSD
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_single FROM `${repoName}`
            ON (`${tableName}_single`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        // Verify data
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_single"
        assertEquals(3, result[0][0])
        
        // Verify medium settings
        show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_single"
        logger.info("Single partition table: ${show_create[0][1]}")

        sql "DROP TABLE ${dbName}.${tableName}_single FORCE"

        // Test 5: Restore with reserve_replica = false (new replica allocation)
        logger.info("=== Test 5: Restore with custom replica allocation and medium ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_replica (
                `id` INT,
                `value` INT
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_replica VALUES (1, 1000)"
        
        // Backup
        sql "BACKUP SNAPSHOT ${dbName}.snap_replica TO `${repoName}` ON (${tableName}_replica)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_replica")
        
        sql "DROP TABLE ${dbName}.${tableName}_replica FORCE"
        
        // Restore with reserve_replica = false and medium settings
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_replica FROM `${repoName}`
            ON (`${tableName}_replica`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "false",
                "replication_num" = "1",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        // Verify data
        result = sql "SELECT * FROM ${dbName}.${tableName}_replica"
        assertEquals(1, result.size())
        assertEquals(1000, result[0][1])

        sql "DROP TABLE ${dbName}.${tableName}_replica FORCE"
        
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
    } finally {
        disableDebugLog()
    }
}

