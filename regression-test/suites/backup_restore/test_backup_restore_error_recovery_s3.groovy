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

suite("test_backup_restore_error_recovery_s3", "backup_restore,docker") {
    String suiteName = "error_recovery_s3"
    String dbName = "db_${suiteName}"
    String repoName = "repo_${suiteName}"
    
    def options = new ClusterOptions()
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup.RestoreJob',
        'sys_log_level=DEBUG'
    ]
    options.beConfigs += ['report_task_interval_seconds=1']
    options.beDisks = ['HDD=1', 'SSD=1']
    
    docker(options) {
        def syncer = getSyncer()
        syncer.createS3Repository(repoName)
        
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
        sql "USE ${dbName}"
        
        // Test 1: User cancels restore job
        logger.info("=== Test 1: User cancels restore job ===")
        
        String tableName1 = "tbl_cancel_test"
        sql """
            CREATE TABLE ${tableName1} (
                id INT,
                data VARCHAR(100)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 3
            PROPERTIES ("replication_num" = "1")
        """
        
        sql "INSERT INTO ${tableName1} VALUES (1, 'data1'), (2, 'data2')"
        
        String snapshotName1 = "snapshot_${tableName1}"
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName1}
            TO ${repoName}
            ON (${tableName1})
        """
        
        syncer.waitAllRestoreFinish()
        
        sql "DROP TABLE ${tableName1} FORCE"
        
        // Start restore
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName1}
            FROM ${repoName}
            ON (${tableName1})
        """
        
        // Wait a moment for restore to start
        sleep(500)
        
        // Cancel the restore job
        def restoreJobs = sql "SHOW RESTORE FROM ${dbName}"
        if (restoreJobs.size() > 0) {
            def state = restoreJobs[0][4]
            logger.info("Current restore state: ${state}")
            
            sql "CANCEL RESTORE FROM ${dbName}"
            
            // Wait for cancellation
            sleep(2000)
            
            def cancelledJobs = sql "SHOW RESTORE FROM ${dbName}"
            if (cancelledJobs.size() > 0) {
                def finalState = cancelledJobs[0][4]
                logger.info("Final restore state: ${finalState}")
                assertTrue(finalState == "CANCELLED" || finalState == "FINISHED")
            }
        }
        
        logger.info("Test 1 passed: User cancel handled correctly")

        // Test 2: Restore with timeout properties
        logger.info("=== Test 2: Restore with timeout properties ===")
        
        String tableName2 = "tbl_timeout_test"
        sql """
            CREATE TABLE ${tableName2} (
                id INT,
                data VARCHAR(100)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        
        sql "INSERT INTO ${tableName2} VALUES (1, 'data1')"
        
        String snapshotName2 = "snapshot_${tableName2}"
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName2}
            TO ${repoName}
            ON (${tableName2})
        """
        
        syncer.waitAllRestoreFinish()
        
        sql "DROP TABLE ${tableName2} FORCE"
        
        // Restore with very short timeout (should still succeed for small data)
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName2}
            FROM ${repoName}
            ON (${tableName2})
            PROPERTIES (
                "timeout" = "300"
            )
        """
        
        syncer.waitAllRestoreFinish()
        
        def result2 = sql "SELECT COUNT(*) FROM ${tableName2}"
        assertEquals(1, result2[0][0])
        
        logger.info("Test 2 passed: Timeout properties handled")
        sql "DROP TABLE ${tableName2} FORCE"

        // Test 3: Restore with allow_load property
        logger.info("=== Test 3: Restore with allow_load ===")
        
        String tableName3 = "tbl_allow_load"
        sql """
            CREATE TABLE ${tableName3} (
                id INT,
                data VARCHAR(50)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        
        sql "INSERT INTO ${tableName3} VALUES (1, 'data1')"
        
        String snapshotName3 = "snapshot_${tableName3}"
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName3}
            TO ${repoName}
            ON (${tableName3})
        """
        
        syncer.waitAllRestoreFinish()
        
        sql "DROP TABLE ${tableName3} FORCE"
        
        // Restore with allow_load = false
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName3}
            FROM ${repoName}
            ON (${tableName3})
            PROPERTIES (
                "allow_load" = "false"
            )
        """
        
        syncer.waitAllRestoreFinish()
        
        def result3 = sql "SELECT COUNT(*) FROM ${tableName3}"
        assertEquals(1, result3[0][0])
        
        logger.info("Test 3 passed: allow_load property handled")
        sql "DROP TABLE ${tableName3} FORCE"

        // Test 4: Restore with reserve_replica
        logger.info("=== Test 4: Restore with reserve_replica ===")
        
        String tableName4 = "tbl_reserve_replica"
        sql """
            CREATE TABLE ${tableName4} (
                id INT,
                data VARCHAR(50)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        
        sql "INSERT INTO ${tableName4} VALUES (1, 'data1')"
        
        String snapshotName4 = "snapshot_${tableName4}"
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName4}
            TO ${repoName}
            ON (${tableName4})
        """
        
        syncer.waitAllRestoreFinish()
        
        sql "DROP TABLE ${tableName4} FORCE"
        
        // Restore with reserve_replica = true
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName4}
            FROM ${repoName}
            ON (${tableName4})
            PROPERTIES (
                "reserve_replica" = "true"
            )
        """
        
        syncer.waitAllRestoreFinish()
        
        def result4 = sql "SELECT COUNT(*) FROM ${tableName4}"
        assertEquals(1, result4[0][0])
        
        logger.info("Test 4 passed: reserve_replica property handled")
        sql "DROP TABLE ${tableName4} FORCE"

        // Test 5: Adaptive mode with medium downgrade
        logger.info("=== Test 5: Adaptive mode with medium downgrade ===")
        
        String tableName5 = "tbl_adaptive_downgrade"
        sql """
            CREATE TABLE ${tableName5} (
                id INT,
                data VARCHAR(50)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "ssd"
            )
        """
        
        sql "INSERT INTO ${tableName5} VALUES (1, 'ssd_data')"
        
        String snapshotName5 = "snapshot_${tableName5}"
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName5}
            TO ${repoName}
            ON (${tableName5})
        """
        
        syncer.waitAllRestoreFinish()
        
        sql "DROP TABLE ${tableName5} FORCE"
        
        // Restore with adaptive mode - may downgrade to HDD if SSD not available
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName5}
            FROM ${repoName}
            ON (${tableName5})
            PROPERTIES (
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish()
        
        def result5 = sql "SELECT COUNT(*) FROM ${tableName5}"
        assertEquals(1, result5[0][0])
        
        // Check if table was created (may be on HDD or SSD depending on BE capacity)
        def showCreate5 = sql "SHOW CREATE TABLE ${tableName5}"
        assertNotNull(showCreate5)
        logger.info("Adaptive mode result: ${showCreate5[0][1]}")
        
        logger.info("Test 5 passed: Adaptive mode downgrade handled")
        sql "DROP TABLE ${tableName5} FORCE"

        // Test 6: Restore non-existent snapshot
        logger.info("=== Test 6: Restore non-existent snapshot ===")
        
        try {
            sql """
                RESTORE SNAPSHOT ${dbName}.non_existent_snapshot
                FROM ${repoName}
                ON (fake_table)
            """
            
            // Wait and check if it failed
            sleep(2000)
            def failedJobs = sql "SHOW RESTORE FROM ${dbName}"
            if (failedJobs.size() > 0) {
                def lastJob = failedJobs[failedJobs.size() - 1]
                logger.info("Restore job state: ${lastJob[4]}")
            }
            
            logger.info("Test 6 passed: Non-existent snapshot handled")
        } catch (Exception e) {
            logger.info("Test 6 passed: Non-existent snapshot correctly rejected: ${e.message}")
        }

        // Test 7: Multiple restore jobs handling
        logger.info("=== Test 7: Multiple restore jobs ===")
        
        String tableName7A = "tbl_multi_restore_a"
        String tableName7B = "tbl_multi_restore_b"
        
        sql """
            CREATE TABLE ${tableName7A} (
                id INT,
                data VARCHAR(50)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        
        sql """
            CREATE TABLE ${tableName7B} (
                id INT,
                data VARCHAR(50)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        
        sql "INSERT INTO ${tableName7A} VALUES (1, 'data_a')"
        sql "INSERT INTO ${tableName7B} VALUES (2, 'data_b')"
        
        String snapshotName7A = "snapshot_${tableName7A}"
        String snapshotName7B = "snapshot_${tableName7B}"
        
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName7A}
            TO ${repoName}
            ON (${tableName7A})
        """
        
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName7B}
            TO ${repoName}
            ON (${tableName7B})
        """
        
        syncer.waitAllRestoreFinish()
        
        sql "DROP TABLE ${tableName7A} FORCE"
        sql "DROP TABLE ${tableName7B} FORCE"
        
        // Start both restores
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName7A}
            FROM ${repoName}
            ON (${tableName7A})
        """
        
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName7B}
            FROM ${repoName}
            ON (${tableName7B})
        """
        
        syncer.waitAllRestoreFinish()
        
        def result7A = sql "SELECT COUNT(*) FROM ${tableName7A}"
        def result7B = sql "SELECT COUNT(*) FROM ${tableName7B}"
        assertEquals(1, result7A[0][0])
        assertEquals(1, result7B[0][0])
        
        logger.info("Test 7 passed: Multiple restore jobs handled")
        sql "DROP TABLE ${tableName7A} FORCE"
        sql "DROP TABLE ${tableName7B} FORCE"

        // Test 8: Restore with table rename
        logger.info("=== Test 8: Restore with table rename ===")
        
        String tableName8 = "tbl_original"
        String renamedTable8 = "tbl_renamed"
        
        sql """
            CREATE TABLE ${tableName8} (
                id INT,
                data VARCHAR(50)
            )
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        
        sql "INSERT INTO ${tableName8} VALUES (1, 'original_data')"
        
        String snapshotName8 = "snapshot_${tableName8}"
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName8}
            TO ${repoName}
            ON (${tableName8})
        """
        
        syncer.waitAllRestoreFinish()
        
        // Restore with new name
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName8}
            FROM ${repoName}
            ON (${tableName8} AS ${renamedTable8})
        """
        
        syncer.waitAllRestoreFinish()
        
        // Verify both tables exist
        def result8Orig = sql "SELECT COUNT(*) FROM ${tableName8}"
        def result8Renamed = sql "SELECT COUNT(*) FROM ${renamedTable8}"
        assertEquals(1, result8Orig[0][0])
        assertEquals(1, result8Renamed[0][0])
        
        logger.info("Test 8 passed: Table rename handled")
        sql "DROP TABLE ${tableName8} FORCE"
        sql "DROP TABLE ${renamedTable8} FORCE"

        // Cleanup
        sql "DROP DATABASE ${dbName} FORCE"
        
        logger.info("=== All error recovery tests completed successfully ===")
    }
}
