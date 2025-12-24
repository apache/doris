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

suite("test_backup_restore_medium_add_partitions", "backup_restore") {
    String suiteName = "test_br_add_part"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def enableDebugLog = { ->
        try {
            def result = sql """
                ADMIN SET FRONTEND CONFIG ("sys_log_verbose_modules" = "org.apache.doris.backup")
            """
            logger.info("Enabled debug logging for org.apache.doris.backup: ${result}")
        } catch (Exception e) {
            logger.warn("Failed to enable debug logging: ${e.message}")
        }
    }

    def disableDebugLog = { ->
        try {
            def result = sql """
                ADMIN SET FRONTEND CONFIG ("sys_log_verbose_modules" = "")
            """
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

        // Test 1: Restore missing partitions to existing table (triggers resetPartitionForRestore)
        logger.info("=== Test 1: Restore missing partitions with medium settings ===")
        
        // Create table with 3 partitions
        sql """
            CREATE TABLE ${dbName}.${tableName}_add (
                `date` DATE,
                `id` INT,
                `value` INT
            )
            DUPLICATE KEY(`date`, `id`)
            PARTITION BY RANGE(`date`) (
                PARTITION p1 VALUES LESS THAN ('2024-01-01'),
                PARTITION p2 VALUES LESS THAN ('2024-02-01'),
                PARTITION p3 VALUES LESS THAN ('2024-03-01')
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        // Insert data
        sql "INSERT INTO ${dbName}.${tableName}_add VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_add VALUES ('2024-01-15', 2, 200)"
        sql "INSERT INTO ${dbName}.${tableName}_add VALUES ('2024-02-15', 3, 300)"
        
        // Backup all 3 partitions
        sql "BACKUP SNAPSHOT ${dbName}.snap_add TO `${repoName}` ON (${tableName}_add)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_add")
        assertTrue(snapshot != null, "Snapshot should be created")

        // Drop only p2 and p3 partitions (keep p1)
        sql "ALTER TABLE ${dbName}.${tableName}_add DROP PARTITION p2"
        sql "ALTER TABLE ${dbName}.${tableName}_add DROP PARTITION p3"
        
        // Verify only p1 remains
        def partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_add"
        assertEquals(1, partitions.size(), "Should only have p1 partition")
        
        // Now restore p2 and p3 to the existing table with SSD + strict
        // This will trigger resetPartitionForRestore() for each partition!
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_add FROM `${repoName}`
            ON (`${tableName}_add` PARTITION (p2, p3))
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        // Verify all 3 partitions now exist
        partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_add"
        logger.info("Partitions after restore: ${partitions}")
        assertEquals(3, partitions.size(), "Should have all 3 partitions after restore")
        
        // Verify data (should have all 3 rows)
        def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_add"
        assertEquals(3, result[0][0], "Should have 3 rows")
        
        // Verify partition properties have medium_allocation_mode
        def show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_add"
        logger.info("Table after adding partitions: ${show_create[0][1]}")
        assertTrue(show_create[0][1].contains("medium_allocation_mode"),
                "Table should have medium_allocation_mode")

        sql "DROP TABLE ${dbName}.${tableName}_add FORCE"

        // Test 2: Atomic restore of same table (typical atomic restore scenario)
        logger.info("=== Test 2: Atomic restore to rollback table to old version ===")
        
        // Create table v1 with HDD
        sql """
            CREATE TABLE ${dbName}.${tableName}_atomic (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_atomic VALUES (1, 'v1_data')"
        
        // Backup v1
        sql "BACKUP SNAPSHOT ${dbName}.snap_atomic_v1 TO `${repoName}` ON (${tableName}_atomic)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_atomic_v1")
        assertTrue(snapshot != null, "Snapshot should be created")
        
        // Modify table to v2 (simulate bad changes)
        sql "TRUNCATE TABLE ${dbName}.${tableName}_atomic"
        sql "INSERT INTO ${dbName}.${tableName}_atomic VALUES (2, 'v2_bad_data')"
        sql "INSERT INTO ${dbName}.${tableName}_atomic VALUES (3, 'v2_more_bad')"
        
        // Verify v2 has 2 rows
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_atomic"
        assertEquals(2, result[0][0], "Should have v2 data before restore")
        
        // Atomic restore v1 (rollback to old version with SSD medium)
        // This tests atomic restore replacing an existing table with medium settings
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_atomic_v1 FROM `${repoName}`
            ON (`${tableName}_atomic`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "atomic_restore" = "true",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        // Verify atomic restore replaced data (v2 -> v1)
        result = sql "SELECT * FROM ${dbName}.${tableName}_atomic ORDER BY id"
        assertEquals(1, result.size(), "Atomic restore should replace data with v1")
        assertEquals(1, result[0][0], "Should have v1 id")
        assertEquals("v1_data", result[0][1].toString(), "Should have v1 data")
        
        // Verify medium settings were applied
        show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_atomic"
        logger.info("Table after atomic restore: ${show_create[0][1]}")
        assertTrue(show_create[0][1].contains("medium_allocation_mode"), 
                "Should have medium_allocation_mode property")

        sql "DROP TABLE ${dbName}.${tableName}_atomic FORCE"

        // Test 3: Restore partitions with different medium combinations
        logger.info("=== Test 3: Mix of HDD and SSD partitions ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_mix (
                `date` DATE,
                `id` INT
            )
            PARTITION BY RANGE(`date`) (
                PARTITION p1 VALUES LESS THAN ('2024-01-01'),
                PARTITION p2 VALUES LESS THAN ('2024-02-01')
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_mix VALUES ('2023-12-15', 10)"
        sql "INSERT INTO ${dbName}.${tableName}_mix VALUES ('2024-01-15', 20)"
        
        // Backup
        sql "BACKUP SNAPSHOT ${dbName}.snap_mix TO `${repoName}` ON (${tableName}_mix)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_mix")
        
        // Drop p2, keep p1
        sql "ALTER TABLE ${dbName}.${tableName}_mix DROP PARTITION p2"
        
        // Restore p2 with HDD + adaptive
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_mix FROM `${repoName}`
            ON (`${tableName}_mix` PARTITION (p2))
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        // Verify both partitions exist
        partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_mix"
        assertEquals(2, partitions.size(), "Should have 2 partitions")
        
        // Verify data
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_mix"
        assertEquals(2, result[0][0], "Should have 2 rows")

        sql "DROP TABLE ${dbName}.${tableName}_mix FORCE"
    
        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
    } finally {
        disableDebugLog()
    }
}

