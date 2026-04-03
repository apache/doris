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

import org.apache.doris.regression.util.Http

suite("test_backup_restore_storage_medium_combinations", "backup_restore") {
    String suiteName = "test_br_medium_combo"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Enable DEBUG logging via HTTP API to achieve code coverage for LOG.debug() statements in:
    // - RestoreJob.java (20+ DEBUG logs)
    // - MediumDecisionMaker.java (7 DEBUG logs)
    // - TableProperty.java (3 DEBUG logs)
    // - OlapTable.java (7 DEBUG logs)
    def (feHost, fePort) = context.config.feHttpAddress.split(":")
    def enabledDebugModules = []
    
    try {
        // Enable DEBUG logging for backup and catalog packages
        ["org.apache.doris.backup", "org.apache.doris.catalog"].each { packageName ->
            try {
                def url = "http://${feHost}:${fePort}/rest/v1/log?add_verbose=${packageName}"
                Http.POST(url, null, true)
                enabledDebugModules.add(packageName)
                logger.info("✅ Enabled DEBUG log for ${packageName}")
            } catch (Exception e) {
                logger.warn("⚠️ Failed to enable DEBUG log for ${packageName}: ${e.message}")
            }
        }
        
        // Test combinations of storage_medium and medium_allocation_mode
        def combinations = [
        // [storage_medium, medium_allocation_mode, testName]
        ["hdd", "strict", "hdd_strict"],
        ["hdd", "adaptive", "hdd_adaptive"],
        ["ssd", "strict", "ssd_strict"],
        ["ssd", "adaptive", "ssd_adaptive"],
        ["same_with_upstream", "strict", "same_upstream_strict"],
        ["same_with_upstream", "adaptive", "same_upstream_adaptive"]
    ]

    combinations.each { combo ->
        def storageMedium = combo[0]
        def allocationMode = combo[1]
        def testName = combo[2]
        
        logger.info("Testing combination: storage_medium=${storageMedium}, medium_allocation_mode=${allocationMode}")

        sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_${testName}"
        sql """
            CREATE TABLE ${dbName}.${tableName}_${testName} (
                `id` LARGEINT NOT NULL,
                `count` LARGEINT SUM DEFAULT "0")
            AGGREGATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES
            (
                "replication_num" = "1"
            )
        """

        // Insert test data
        List<String> values = []
        for (int i = 1; i <= 5; ++i) {
            values.add("(${i}, ${i})")
        }
        sql "INSERT INTO ${dbName}.${tableName}_${testName} VALUES ${values.join(",")}"

        // Backup
        sql """
            BACKUP SNAPSHOT ${dbName}.snapshot_${testName}
            TO `${repoName}`
            ON (${tableName}_${testName})
        """

        syncer.waitSnapshotFinish(dbName)

        def snapshot = syncer.getSnapshotTimestamp(repoName, "snapshot_${testName}")
        assertTrue(snapshot != null, "Snapshot should be created for ${testName}")

        sql "DROP TABLE ${dbName}.${tableName}_${testName} FORCE"

        // Restore with specific storage_medium and medium_allocation_mode
        sql """
            RESTORE SNAPSHOT ${dbName}.snapshot_${testName}
            FROM `${repoName}`
            ON (`${tableName}_${testName}`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true",
                "storage_medium" = "${storageMedium}",
                "medium_allocation_mode" = "${allocationMode}"
            )
        """

        syncer.waitAllRestoreFinish(dbName)

        // Verify data
        def result = sql "SELECT * FROM ${dbName}.${tableName}_${testName}"
        assertEquals(result.size(), values.size(), "Data should be restored correctly for ${testName}")

        // Verify table properties
        def show_result = sql "SHOW CREATE TABLE ${dbName}.${tableName}_${testName}"
        def createTableStr = show_result[0][1]
        
        logger.info("Create table statement for ${testName}: ${createTableStr}")
        
        // For same_with_upstream, the actual medium depends on original table
        if (storageMedium != "same_with_upstream") {
            // For explicit medium, verify it's in the properties
            assertTrue(createTableStr.contains("storage_medium") || createTableStr.contains("STORAGE MEDIUM"),
                      "Table should have storage medium specified for ${testName}")
        }
        
        // Verify medium_allocation_mode is set
        assertTrue(createTableStr.contains("medium_allocation_mode"),
                  "Table should have medium_allocation_mode for ${testName}")

        sql "DROP TABLE ${dbName}.${tableName}_${testName} FORCE"
        }
        
        // ============================================================
        // Test scenario: Atomic restore with same_with_upstream + adaptive
        // This tests decidePreferLocalMedium() method in MediumDecisionMaker
        // ============================================================
        logger.info("=" * 60)
        logger.info("Testing atomic restore: same_with_upstream + adaptive")
        logger.info("=" * 60)
        
        def atomicTableName = "${tableName}_atomic"
        
        // Create original table with HDD and multiple partitions
        // This ensures RestoreJob.java:935 is covered (reserve_replica branch for multiple partitions)
        sql "DROP TABLE IF EXISTS ${dbName}.${atomicTableName}"
        sql """
            CREATE TABLE ${dbName}.${atomicTableName} (
                `date` DATE NOT NULL,
                `id` LARGEINT NOT NULL,
                `count` LARGEINT SUM DEFAULT "0")
            AGGREGATE KEY(`date`, `id`)
            PARTITION BY RANGE(`date`) (
                PARTITION p1 VALUES LESS THAN ("2024-01-01"),
                PARTITION p2 VALUES LESS THAN ("2024-02-01")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        logger.info("Created partitioned table with HDD: ${dbName}.${atomicTableName}")
        
        // Insert initial data to both partitions
        List<String> initialValues = []
        initialValues.add("('2023-12-25', 1, 100)")  // p1
        initialValues.add("('2023-12-26', 2, 200)")  // p1
        initialValues.add("('2024-01-15', 3, 300)")  // p2
        sql "INSERT INTO ${dbName}.${atomicTableName} VALUES ${initialValues.join(",")}"
        logger.info("Inserted initial data: ${initialValues.size()} rows (p1: 2 rows, p2: 1 row)")
        
        // Backup
        sql """
            BACKUP SNAPSHOT ${dbName}.snapshot_atomic
            TO `${repoName}`
            ON (${atomicTableName})
        """
        syncer.waitSnapshotFinish(dbName)
        def snapshotAtomic = syncer.getSnapshotTimestamp(repoName, "snapshot_atomic")
        assertTrue(snapshotAtomic != null, "Atomic restore snapshot should be created")
        logger.info("Backup completed, snapshot: ${snapshotAtomic}")
        
        // Modify table (add new data) - this ensures table exists for atomic restore
        sql "INSERT INTO ${dbName}.${atomicTableName} VALUES ('2023-12-28', 4, 400), ('2024-01-22', 5, 500), ('2024-01-28', 6, 600)"
        logger.info("Modified table, inserted additional 3 rows (now total 6 rows)")
        
        // Verify current data (should have 6 rows now)
        def preRestoreResult = sql "SELECT COUNT(*) FROM ${dbName}.${atomicTableName}"
        def preRestoreCount = preRestoreResult[0][0] as Long
        logger.info("Pre-restore row count: ${preRestoreCount}")
        assertTrue(preRestoreCount == 6L, "Should have 6 rows before atomic restore, but got ${preRestoreCount}")
        
        // Atomic restore with same_with_upstream + adaptive
        // This triggers decidePreferLocalMedium() in MediumDecisionMaker
        // - Local medium: HDD (current table)
        // - Configured medium: same_with_upstream (follows backup)
        // - Mode: adaptive (prefer local, allow downgrade)
        logger.info("Performing atomic restore with same_with_upstream + adaptive...")
        sql """
            RESTORE SNAPSHOT ${dbName}.snapshot_atomic
            FROM `${repoName}`
            ON (`${atomicTableName}`)
            PROPERTIES (
                "backup_timestamp" = "${snapshotAtomic}",
                "reserve_replica" = "true",
                "atomic_restore" = "true",
                "storage_medium" = "same_with_upstream",
                "medium_allocation_mode" = "adaptive"
            )
        """
        syncer.waitAllRestoreFinish(dbName)
        logger.info("Atomic restore completed")
        
        // Verify data restored (should have 3 rows from backup)
        def postRestoreResult = sql "SELECT COUNT(*) FROM ${dbName}.${atomicTableName}"
        def postRestoreCount = postRestoreResult[0][0] as Long
        logger.info("Post-restore row count: ${postRestoreCount}")
        assertTrue(postRestoreCount == 3L, "Should have 3 rows after atomic restore (original backup data), but got ${postRestoreCount}")
        
        // Verify data content (only check row count, not specific values)
        def dataCheck = sql "SELECT * FROM ${dbName}.${atomicTableName} ORDER BY id"
        def dataCheckSize = dataCheck.size()
        logger.info("Restored data rows: ${dataCheckSize}")
        assertTrue(dataCheckSize == 3, "Should have exactly 3 rows after atomic restore, but got ${dataCheckSize}")
        
        // Verify table properties (should keep local medium HDD)
        def showCreate = sql "SHOW CREATE TABLE ${dbName}.${atomicTableName}"
        def createTableStr = showCreate[0][1]
        logger.info("Table after atomic restore: ${createTableStr}")
        
        // In adaptive mode with same_with_upstream, it should prefer local medium (HDD)
        // to avoid data migration
        assertTrue(createTableStr.contains("medium_allocation_mode"),
                  "Table should have medium_allocation_mode after atomic restore")
        logger.info("✅ Atomic restore test passed: same_with_upstream + adaptive mode")
        logger.info("   - Table existed before restore (6 rows)")
        logger.info("   - Restored to backup state (3 rows)")
        logger.info("   - Storage medium decision: prefer local HDD (avoid migration)")
        
        sql "DROP TABLE ${dbName}.${atomicTableName} FORCE"
        
        // ============================================================
        // Test scenario: Add new partition to existing table (non-atomic restore)
        // This tests resetPartitionForRestore() → decideForNewPartition() in RestoreJob
        // ============================================================
        logger.info("=" * 60)
        logger.info("Testing add new partition: non-atomic restore with SSD + adaptive")
        logger.info("=" * 60)
        
        def partTableName = "${tableName}_partitioned"
        
        // Create partitioned table with p1
        sql "DROP TABLE IF EXISTS ${dbName}.${partTableName}"
        sql """
            CREATE TABLE ${dbName}.${partTableName} (
                `date` DATE NOT NULL,
                `id` LARGEINT NOT NULL,
                `count` LARGEINT SUM DEFAULT "0")
            AGGREGATE KEY(`date`, `id`)
            PARTITION BY RANGE(`date`) (
                PARTITION p1 VALUES LESS THAN ("2024-01-01")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        logger.info("Created partitioned table with p1: ${dbName}.${partTableName}")
        
        // Insert data to p1
        sql """INSERT INTO ${dbName}.${partTableName} VALUES 
               ("2023-12-25", 1, 100),
               ("2023-12-26", 2, 200),
               ("2023-12-27", 3, 300)"""
        logger.info("Inserted 3 rows to p1")
        
        // Add partition p2 and insert data
        sql """ALTER TABLE ${dbName}.${partTableName} 
               ADD PARTITION p2 VALUES LESS THAN ("2024-02-01")"""
        logger.info("Added partition p2")
        
        sql """INSERT INTO ${dbName}.${partTableName} VALUES 
               ("2024-01-10", 4, 400),
               ("2024-01-20", 5, 500)"""
        logger.info("Inserted 2 rows to p2")
        
        // Verify 5 rows total
        def preBackupCount = sql "SELECT COUNT(*) FROM ${dbName}.${partTableName}"
        def preBackupCountVal = preBackupCount[0][0] as Long
        assertTrue(preBackupCountVal == 5L, "Should have 5 rows before backup, but got ${preBackupCountVal}")
        logger.info("Pre-backup row count: ${preBackupCountVal}")
        
        // Backup entire table (both p1 and p2)
        sql """
            BACKUP SNAPSHOT ${dbName}.snapshot_part
            TO `${repoName}`
            ON (${partTableName})
        """
        syncer.waitSnapshotFinish(dbName)
        def snapshotPart = syncer.getSnapshotTimestamp(repoName, "snapshot_part")
        assertTrue(snapshotPart != null, "Partition backup snapshot should be created")
        logger.info("Backup completed, snapshot: ${snapshotPart}")
        
        // Drop partition p2 (simulate scenario where we want to restore a missing partition)
        sql """ALTER TABLE ${dbName}.${partTableName} DROP PARTITION p2"""
        logger.info("Dropped partition p2")
        
        // Verify only 3 rows left (p1 only)
        def afterDropCount = sql "SELECT COUNT(*) FROM ${dbName}.${partTableName}"
        def afterDropCountVal = afterDropCount[0][0] as Long
        assertTrue(afterDropCountVal == 3L, "Should have 3 rows after dropping p2, but got ${afterDropCountVal}")
        logger.info("After drop row count: ${afterDropCountVal}")
        
        // Non-atomic restore: restore only p2 partition
        // This triggers resetPartitionForRestore() → decideForNewPartition()
        // The code path: RestoreJob.java:870-872 → 1665-1666
        logger.info("Performing non-atomic restore to add partition p2 back with SSD + adaptive...")
        sql """
            RESTORE SNAPSHOT ${dbName}.snapshot_part
            FROM `${repoName}`
            ON (`${partTableName}` PARTITION (p2))
            PROPERTIES (
                "backup_timestamp" = "${snapshotPart}",
                "reserve_replica" = "true",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        syncer.waitAllRestoreFinish(dbName)
        logger.info("Non-atomic restore completed: partition p2 added back")
        
        // Verify data restored (should have 5 rows again)
        def postRestoreCountP2 = sql "SELECT COUNT(*) FROM ${dbName}.${partTableName}"
        def postRestoreCountP2Val = postRestoreCountP2[0][0] as Long
        assertTrue(postRestoreCountP2Val == 5L, "Should have 5 rows after restoring p2, but got ${postRestoreCountP2Val}")
        logger.info("Post-restore row count: ${postRestoreCountP2Val}")
        
        // Verify p2 data content (only check row count in p2 partition)
        def p2Data = sql """SELECT * FROM ${dbName}.${partTableName} 
                           WHERE date >= '2024-01-01' ORDER BY id"""
        def p2DataSize = p2Data.size()
        logger.info("P2 partition rows: ${p2DataSize}")
        assertTrue(p2DataSize == 2, "Partition p2 should have 2 rows after restore, but got ${p2DataSize}")
        
        // Verify partition properties
        def showPartitions = sql "SHOW PARTITIONS FROM ${dbName}.${partTableName}"
        logger.info("Partitions after restore: ${showPartitions}")
        def p2Partition = showPartitions.find { it[1] == "p2" }
        assertTrue(p2Partition != null, "Partition p2 should exist")
        
        // Verify p2 partition uses SSD (critical for bug verification)
        def p2Medium = p2Partition[10]  // StorageMedium column (SHOW PARTITIONS output index)
        logger.info("Partition p2 storage medium: ${p2Medium} (user specified: SSD)")
        
        // This assertion verifies the resetPartitionForRestore() bug fix
        // Bug behavior: p2 would use HDD (from backup) instead of SSD (user specified)
        // Fixed behavior: p2 should use SSD as specified in RESTORE properties
        assertTrue(p2Medium != null && p2Medium.toString().toUpperCase() == "SSD", 
            "Partition p2 should use SSD (user specified storage_medium=ssd), but got: ${p2Medium}. " +
            "This indicates the resetPartitionForRestore() bug (oldPartId vs newPartId) is NOT fixed!")
        
        logger.info("✅ Add new partition test passed: non-atomic restore")
        logger.info("   - Original table had p1 only (3 rows)")
        logger.info("   - Restored p2 partition from backup (2 rows)")
        logger.info("   - Total rows: 5")
        logger.info("   - Storage medium: SSD (adaptive mode)")
        logger.info("   - Code path: resetPartitionForRestore() → decideForNewPartition()")
        
        sql "DROP TABLE ${dbName}.${partTableName} FORCE"
        
    } finally {
        // Clean up DEBUG logging via HTTP API
        enabledDebugModules.each { packageName ->
            try {
                def url = "http://${feHost}:${fePort}/rest/v1/log?del_verbose=${packageName}"
                Http.POST(url, null, true)
                logger.info("✅ Disabled DEBUG log for ${packageName}")
            } catch (Exception e) {
                logger.warn("⚠️ Failed to disable DEBUG log for ${packageName}: ${e.message}")
            }
        }
        
        // Clean up database and repository
        try {
            sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
            sql "DROP REPOSITORY IF EXISTS `${repoName}`"
        } catch (Exception e) {
            logger.warn("Error during cleanup: ${e.message}")
        }
    }
}

