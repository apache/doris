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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite("test_backup_restore_state_and_error_docker", "docker,backup_restore") {
    // Configure cluster for state and error testing
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    
    // Configure BEs with different disk types
    options.beDisks = [
        "HDD=2",      // First BE: 2 HDD disks
        "SSD=1",      // Second BE: 1 SSD disk
        "HDD=1,SSD=1" // Third BE: 1 HDD + 1 SSD (mixed)
    ]
    
    options.enableDebugPoints()
    
    // Enable debug logging for org.apache.doris.backup package
    // Setting both sys_log_verbose_modules (for the package) and sys_log_level (to ensure DEBUG is enabled)
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup',
        'sys_log_level=DEBUG'
    ]

    docker(options) {
        String suiteName = "test_br_state"
        String repoName = "${suiteName}_repo"
        String dbName = "${suiteName}_db"
        String tableName = "${suiteName}_table"

        def syncer = getSyncer()
        syncer.createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

        def feHttpAddress = context.config.feHttpAddress.split(":")
        def feHost = feHttpAddress[0]
        def fePort = feHttpAddress[1] as int

        // Test 1: Verify all state transitions (PENDING → CREATING → ... → FINISHED)
        logger.info("=== Test 1: Complete state machine transitions ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_states (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_states VALUES (1, 'state_test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_states TO `${repoName}` ON (${tableName}_states)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_states")
        assertTrue(snapshot != null)
        
        sql "DROP TABLE ${dbName}.${tableName}_states FORCE"
        
        // Start restore and monitor state transitions
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_states FROM `${repoName}`
            ON (`${tableName}_states`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        // Monitor state transitions
        def seenStates = [] as Set
        def maxAttempts = 30
        def attempt = 0
        
        while (attempt < maxAttempts) {
            def restore_status = sql "SHOW RESTORE FROM ${dbName}"
            if (restore_status.size() > 0) {
                def state = restore_status[0][4]
                seenStates.add(state)
                logger.info("Current restore state: ${state}")
                
                if (state == "FINISHED" || state == "CANCELLED") {
                    break
                }
            }
            Thread.sleep(1000)
            attempt++
        }
        
        logger.info("Observed states during restore: ${seenStates}")
        // Should see at least PENDING/SNAPSHOTING and FINISHED
        assertTrue(seenStates.size() >= 2, "Should observe multiple states")
        assertTrue(seenStates.contains("FINISHED"), "Should reach FINISHED state")
        
        def result = sql "SELECT * FROM ${dbName}.${tableName}_states"
        assertEquals(1, result.size())
        
        sql "DROP TABLE ${dbName}.${tableName}_states FORCE"

        // Test 2: Timeout scenario
        logger.info("=== Test 2: Restore timeout handling ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_timeout (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_timeout VALUES (2, 'timeout_test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_timeout TO `${repoName}` ON (${tableName}_timeout)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_timeout")
        
        sql "DROP TABLE ${dbName}.${tableName}_timeout FORCE"
        
        // Set very short timeout
        try {
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_timeout FROM `${repoName}`
                ON (`${tableName}_timeout`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot}",
                    "timeout" = "1",
                    "storage_medium" = "hdd",
                    "medium_allocation_mode" = "strict"
                )
            """
            
            // Wait for timeout
            Thread.sleep(3000)
            
            def restore_status = sql "SHOW RESTORE FROM ${dbName}"
            if (restore_status.size() > 0) {
                logger.info("Restore status after timeout: State=${restore_status[0][4]}, Msg=${restore_status[0][11]}")
                // Should be CANCELLED or show timeout message
            }
            
        } catch (Exception e) {
            logger.info("Expected: timeout scenario handled: ${e.message}")
        }
        
        try {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_timeout FORCE"
        } catch (Exception e) {
            logger.info("Cleanup: ${e.message}")
        }

        // Test 3: Cancel restore during different states
        logger.info("=== Test 3: Cancel restore at different stages ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_cancel (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 5
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        // Insert more data to make restore slower
        for (int i = 0; i < 50; i++) {
            sql "INSERT INTO ${dbName}.${tableName}_cancel VALUES (${i}, 'cancel_test_${i}')"
        }
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_cancel TO `${repoName}` ON (${tableName}_cancel)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_cancel")
        
        sql "DROP TABLE ${dbName}.${tableName}_cancel FORCE"
        
        // Start restore
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_cancel FROM `${repoName}`
            ON (`${tableName}_cancel`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        // Cancel immediately
        Thread.sleep(500)
        
        try {
            sql "CANCEL RESTORE FROM ${dbName}"
            logger.info("Cancel restore requested")
            
            Thread.sleep(2000)
            
            def restore_status = sql "SHOW RESTORE FROM ${dbName}"
            if (restore_status.size() > 0) {
                def state = restore_status[0][4]
                logger.info("Restore state after cancel: ${state}")
                // Should be CANCELLED or FINISHED (if too fast)
            }
            
        } catch (Exception e) {
            logger.info("Cancel operation: ${e.message}")
        }
        
        try {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_cancel FORCE"
        } catch (Exception e) {
            logger.info("Cleanup: ${e.message}")
        }

        // Test 4: Restore with table state conflict
        logger.info("=== Test 4: Restore table state validation ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_state_conflict (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_state_conflict VALUES (3, 'state_test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_state TO `${repoName}` ON (${tableName}_state_conflict)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_state")
        
        // Don't drop table - restore to existing table
        // This will test partition-level restore
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_state FROM `${repoName}`
            ON (`${tableName}_state_conflict`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        def result2 = sql "SELECT * FROM ${dbName}.${tableName}_state_conflict"
        assertEquals(1, result2.size())
        
        sql "DROP TABLE ${dbName}.${tableName}_state_conflict FORCE"

        // Test 5: Atomic restore error handling
        logger.info("=== Test 5: Atomic restore with errors ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_atomic_err (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_atomic_err VALUES (4, 'version1')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_atomic_err TO `${repoName}` ON (${tableName}_atomic_err)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_atomic_err")
        
        // Simulate error during atomic restore
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "RestoreJob.commit.atomic_restore_table.throw_exception")
            
            try {
                sql """
                    RESTORE SNAPSHOT ${dbName}.snap_atomic_err FROM `${repoName}`
                    ON (`${tableName}_atomic_err`)
                    PROPERTIES (
                        "backup_timestamp" = "${snapshot}",
                        "atomic_restore" = "true",
                        "storage_medium" = "ssd",
                        "medium_allocation_mode" = "adaptive"
                    )
                """
                
                Thread.sleep(5000)
                
                def restore_status = sql "SHOW RESTORE FROM ${dbName}"
                if (restore_status.size() > 0) {
                    logger.info("Atomic restore with error: State=${restore_status[0][4]}, Msg=${restore_status[0][11]}")
                }
                
            } catch (Exception e) {
                logger.info("Expected: atomic restore error handled: ${e.message}")
            }
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "RestoreJob.commit.atomic_restore_table.throw_exception")
        }
        
        // Original table should still exist and be intact
        result = sql "SELECT * FROM ${dbName}.${tableName}_atomic_err"
        assertEquals(1, result.size())
        assertEquals("version1", result[0][1])
        
        sql "DROP TABLE ${dbName}.${tableName}_atomic_err FORCE"

        // Test 6: Restore with reserve_replica and reserve_colocate
        logger.info("=== Test 6: Restore with preserve properties ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_preserve (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "2"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_preserve VALUES (5, 'preserve_test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_preserve TO `${repoName}` ON (${tableName}_preserve)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_preserve")
        
        sql "DROP TABLE ${dbName}.${tableName}_preserve FORCE"
        
        // Restore with replication_num=1 but reserve_replica=true
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_preserve FROM `${repoName}`
            ON (`${tableName}_preserve`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "replication_num" = "1",
                "reserve_replica" = "true",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT * FROM ${dbName}.${tableName}_preserve"
        assertEquals(1, result.size())
        
        // Check if replication preserved
        def show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_preserve"
        logger.info("Table with reserve_replica: ${show_create[0][1]}")
        
        sql "DROP TABLE ${dbName}.${tableName}_preserve FORCE"

        // Test 7: Restore with force_replace (schema changes)
        logger.info("=== Test 7: Restore with force_replace ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_force (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_force VALUES (6, 'original')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_force TO `${repoName}` ON (${tableName}_force)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_force")
        
        // Modify table schema
        sql "ALTER TABLE ${dbName}.${tableName}_force ADD COLUMN new_col INT DEFAULT 0"
        
        // Wait for alter to complete
        Thread.sleep(2000)
        
        // Restore with force_replace=false should work for existing partitions
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_force FROM `${repoName}`
            ON (`${tableName}_force`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "force_replace" = "false",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT * FROM ${dbName}.${tableName}_force"
        assertTrue(result.size() >= 1)
        
        sql "DROP TABLE ${dbName}.${tableName}_force FORCE"

        // Test 8: Restore partitioned table with missing partitions
        logger.info("=== Test 8: Restore with partition additions ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_part_add (
                `date` DATE,
                `id` INT,
                `value` INT
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
        
        sql "INSERT INTO ${dbName}.${tableName}_part_add VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_part_add VALUES ('2024-01-15', 2, 200)"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_part_add TO `${repoName}` ON (${tableName}_part_add)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_part_add")
        
        // Drop only one partition
        sql "ALTER TABLE ${dbName}.${tableName}_part_add DROP PARTITION p2"
        
        // Restore should add missing partition back
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_part_add FROM `${repoName}`
            ON (`${tableName}_part_add`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        def partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_part_add"
        logger.info("Partitions after restore: ${partitions}")
        assertEquals(2, partitions.size(), "Should have 2 partitions after restore")
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_part_add"
        assertEquals(2, result[0][0], "Should have all data")
        
        sql "DROP TABLE ${dbName}.${tableName}_part_add FORCE"

        // Test 9: Same table multiple restore operations
        logger.info("=== Test 9: Multiple restore operations on same table ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_multi_op (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_multi_op VALUES (7, 'version1')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_multi_v1 TO `${repoName}` ON (${tableName}_multi_op)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot_v1 = syncer.getSnapshotTimestamp(repoName, "snap_multi_v1")
        
        sql "INSERT INTO ${dbName}.${tableName}_multi_op VALUES (8, 'version2')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_multi_v2 TO `${repoName}` ON (${tableName}_multi_op)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot_v2 = syncer.getSnapshotTimestamp(repoName, "snap_multi_v2")
        
        sql "INSERT INTO ${dbName}.${tableName}_multi_op VALUES (9, 'version3')"
        
        // Restore to v1
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_multi_v1 FROM `${repoName}`
            ON (`${tableName}_multi_op`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot_v1}",
                "atomic_restore" = "true",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_multi_op"
        assertEquals(1, result[0][0], "Should have v1 data only")
        
        // Restore to v2
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_multi_v2 FROM `${repoName}`
            ON (`${tableName}_multi_op`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot_v2}",
                "atomic_restore" = "true",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_multi_op"
        assertEquals(2, result[0][0], "Should have v2 data")
        
        sql "DROP TABLE ${dbName}.${tableName}_multi_op FORCE"

        // Test 10: Restore with same_with_upstream and complex partitions
        logger.info("=== Test 10: same_with_upstream with complex partitions ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_complex (
                `date` DATE,
                `id` INT,
                `value` INT
            )
            PARTITION BY RANGE(`date`) (
                PARTITION p1 VALUES LESS THAN ('2024-01-01'),
                PARTITION p2 VALUES LESS THAN ('2024-02-01'),
                PARTITION p3 VALUES LESS THAN ('2024-03-01'),
                PARTITION p4 VALUES LESS THAN ('2024-04-01')
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "SSD"
            )
        """
        
        for (int i = 1; i <= 4; i++) {
            def date = String.format("2023-%02d-15", 12 + i - 1)
            if (i > 1) {
                date = String.format("2024-%02d-15", i - 1)
            }
            sql "INSERT INTO ${dbName}.${tableName}_complex VALUES ('${date}', ${i}, ${i * 100})"
        }
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_complex TO `${repoName}` ON (${tableName}_complex)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_complex")
        
        sql "DROP TABLE ${dbName}.${tableName}_complex FORCE"
        
        // Restore with same_with_upstream
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_complex FROM `${repoName}`
            ON (`${tableName}_complex`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "same_with_upstream",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_complex"
        assertEquals(4, result[0][0])
        
        partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_complex"
        assertEquals(4, partitions.size())
        
        show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_complex"
        logger.info("Complex table with same_with_upstream: ${show_create[0][1]}")
        
        sql "DROP TABLE ${dbName}.${tableName}_complex FORCE"

        // Test 8: Table type validation - temporary partitions
        logger.info("=== Test 8: Restore table with temporary partitions (should fail) ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_temp_part (
                `id` INT,
                `value` STRING
            )
            PARTITION BY RANGE(`id`) (
                PARTITION p1 VALUES LESS THAN ("10")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_temp_part VALUES (1, 'test')"
        
        // Add a temporary partition
        sql """
            ALTER TABLE ${dbName}.${tableName}_temp_part 
            ADD TEMPORARY PARTITION tp1 VALUES LESS THAN ("20")
        """
        
        // Backup with temp partition
        sql "BACKUP SNAPSHOT ${dbName}.snap_temp_part TO `${repoName}` ON (${tableName}_temp_part)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot8 = syncer.getSnapshotTimestamp(repoName, "snap_temp_part")
        
        sql "DROP TABLE ${dbName}.${tableName}_temp_part FORCE"
        
        // Create table with temp partition again
        sql """
            CREATE TABLE ${dbName}.${tableName}_temp_part (
                `id` INT,
                `value` STRING
            )
            PARTITION BY RANGE(`id`) (
                PARTITION p1 VALUES LESS THAN ("10")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql """
            ALTER TABLE ${dbName}.${tableName}_temp_part 
            ADD TEMPORARY PARTITION tp1 VALUES LESS THAN ("20")
        """
        
        // Try to restore - should fail because table has temp partitions
        test {
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_temp_part FROM `${repoName}`
                ON (`${tableName}_temp_part`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot8}"
                )
            """
            exception "Do not support restoring table with temp partitions"
        }
        
        logger.info("✓ Test 8 passed: Correctly rejected restore with temp partitions")
        sql "DROP TABLE ${dbName}.${tableName}_temp_part FORCE"

        // Test 9: Table type validation - VIEW type conflict
        logger.info("=== Test 9: Table type conflict - VIEW vs OLAP ===")
        
        // Create a normal table and backup
        sql """
            CREATE TABLE ${dbName}.${tableName}_type_test (
                `id` INT,
                `name` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_type_test VALUES (1, 'test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_type_test TO `${repoName}` ON (${tableName}_type_test)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot9 = syncer.getSnapshotTimestamp(repoName, "snap_type_test")
        
        sql "DROP TABLE ${dbName}.${tableName}_type_test FORCE"
        
        // Create a VIEW with same name
        sql """
            CREATE TABLE ${dbName}.${tableName}_base (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        
        sql """
            CREATE VIEW ${dbName}.${tableName}_type_test AS 
            SELECT * FROM ${dbName}.${tableName}_base
        """
        
        // Try to restore OLAP table when VIEW exists with same name - should fail
        test {
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_type_test FROM `${repoName}`
                ON (`${tableName}_type_test`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot9}"
                )
            """
            exception "Only support restore OLAP table"
        }
        
        logger.info("✓ Test 9 passed: Correctly rejected restore OLAP table when VIEW exists")
        sql "DROP VIEW ${dbName}.${tableName}_type_test"
        sql "DROP TABLE ${dbName}.${tableName}_base FORCE"

        // Test 10: Table state validation - non-NORMAL state
        logger.info("=== Test 10: Table state validation - SCHEMA_CHANGE state ===")
        
        // Create table and backup
        sql """
            CREATE TABLE ${dbName}.${tableName}_state_test (
                `id` INT,
                `name` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_state_test VALUES (1, 'test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_state_test TO `${repoName}` ON (${tableName}_state_test)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot10 = syncer.getSnapshotTimestamp(repoName, "snap_state_test")
        
        // Start a schema change to put table in SCHEMA_CHANGE state
        sql "ALTER TABLE ${dbName}.${tableName}_state_test ADD COLUMN new_col INT DEFAULT '0'"
        
        // Wait a bit for schema change to start
        Thread.sleep(1000)
        
        // Try to restore while table is in SCHEMA_CHANGE state - should fail
        // Note: This test is best-effort as schema change might complete quickly
        def schemaChangeState = sql "SHOW ALTER TABLE COLUMN FROM ${dbName}"
        if (schemaChangeState.size() > 0 && schemaChangeState[0][9] != "FINISHED") {
            test {
                sql """
                    RESTORE SNAPSHOT ${dbName}.snap_state_test FROM `${repoName}`
                    ON (`${tableName}_state_test`)
                    PROPERTIES (
                        "backup_timestamp" = "${snapshot10}"
                    )
                """
                exception "state is not NORMAL"
            }
            logger.info("✓ Test 10 passed: Correctly rejected restore when table state is not NORMAL")
        } else {
            logger.info("⚠ Test 10 skipped: Schema change completed too quickly")
        }
        
        // Wait for schema change to complete
        def maxWait = 30
        def waited = 0
        while (waited < maxWait) {
            def alterResult = sql "SHOW ALTER TABLE COLUMN FROM ${dbName}"
            if (alterResult.size() == 0 || alterResult[0][9] == "FINISHED") {
                break
            }
            Thread.sleep(1000)
            waited++
        }
        
        sql "DROP TABLE ${dbName}.${tableName}_state_test FORCE"

        // Test 11: Backup/Restore with VIEW - type mismatch in backup metadata
        logger.info("=== Test 11: VIEW in backup, different type in local ===")
        
        // Create base table and view
        sql """
            CREATE TABLE ${dbName}.${tableName}_view_base (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_view_base VALUES (1, 'view_test')"
        
        sql """
            CREATE VIEW ${dbName}.${tableName}_view AS 
            SELECT * FROM ${dbName}.${tableName}_view_base
        """
        
        // Backup the view
        sql "BACKUP SNAPSHOT ${dbName}.snap_view TO `${repoName}` ON (${tableName}_view_base, ${tableName}_view)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot11 = syncer.getSnapshotTimestamp(repoName, "snap_view")
        
        sql "DROP VIEW ${dbName}.${tableName}_view"
        
        // Create an OLAP table with same name as the view
        sql """
            CREATE TABLE ${dbName}.${tableName}_view (
                `id` INT,
                `data` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        
        // Try to restore - should fail due to type mismatch
        test {
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_view FROM `${repoName}`
                ON (`${tableName}_view`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot11}"
                )
            """
            exception "with the same name but a different type of backup meta"
        }
        
        logger.info("✓ Test 11 passed: Correctly detected VIEW type mismatch")
        sql "DROP TABLE ${dbName}.${tableName}_view FORCE"
        sql "DROP TABLE ${dbName}.${tableName}_view_base FORCE"

        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
    }
}

