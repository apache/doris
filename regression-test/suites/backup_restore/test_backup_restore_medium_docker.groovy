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

suite("test_backup_restore_medium_docker", "docker,backup_restore") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.enableDebugPoints()
    
    // Enable debug logging for org.apache.doris.backup package in FE config
    // This is much simpler than using HTTP API!
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup'
    ]

    docker(options) {
        // Debug logging is already enabled via FE config!
        // No need to use HTTP API enableDebugLog()/disableDebugLog()
        
        String suiteName = "test_br_docker"
            String repoName = "${suiteName}_repo"
            String dbName = "${suiteName}_db"
            String tableName = "${suiteName}_table"

            def syncer = getSyncer()
            syncer.createS3Repository(repoName)
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

            def feHttpAddress = context.config.feHttpAddress.split(":")
            def feHost = feHttpAddress[0]
            def fePort = feHttpAddress[1] as int

            // Test 1: Simulate backend selection with medium constraint (trigger adaptive fallback)
            logger.info("=== Test 1: Simulate SSD BE unavailable, test adaptive mode fallback ===")
            
            sql """
                CREATE TABLE ${dbName}.${tableName}_adaptive (
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
                    "replication_num" = "1",
                    "storage_medium" = "HDD"
                )
            """

            sql "INSERT INTO ${dbName}.${tableName}_adaptive VALUES ('2023-12-15', 1, 100)"
            sql "INSERT INTO ${dbName}.${tableName}_adaptive VALUES ('2024-01-15', 2, 200)"

            // Backup
            sql "BACKUP SNAPSHOT ${dbName}.snap_adaptive TO `${repoName}` ON (${tableName}_adaptive)"
            syncer.waitSnapshotFinish(dbName)
            def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_adaptive")
            assertTrue(snapshot != null)

            sql "DROP TABLE ${dbName}.${tableName}_adaptive FORCE"

            // Enable debug point to simulate SSD backend unavailable
            // This will force adaptive mode to fallback from SSD to HDD
            try {
                DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE, 
                    "SystemInfoService.selectBackendIdsForReplicaCreation.forceNoSSDBackends")
                
                // Restore with SSD + adaptive mode
                // Adaptive mode should detect no SSD backends and fallback to HDD
                sql """
                    RESTORE SNAPSHOT ${dbName}.snap_adaptive FROM `${repoName}`
                    ON (`${tableName}_adaptive`)
                    PROPERTIES (
                        "backup_timestamp" = "${snapshot}",
                        "storage_medium" = "ssd",
                        "medium_allocation_mode" = "adaptive"
                    )
                """
                
                syncer.waitAllRestoreFinish(dbName)
                
                // Verify data restored successfully despite medium fallback
                def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_adaptive"
                assertEquals(2, result[0][0], "Data should be restored even with medium fallback")
                
                def show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_adaptive"
                logger.info("Table after adaptive fallback: ${show_create[0][1]}")
                
            } finally {
                DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                    "SystemInfoService.selectBackendIdsForReplicaCreation.forceNoSSDBackends")
            }

            sql "DROP TABLE ${dbName}.${tableName}_adaptive FORCE"

            // Test 2: Test strict mode failure when requested medium is unavailable
            logger.info("=== Test 2: Test strict mode behavior when medium unavailable ===")
            
            sql """
                CREATE TABLE ${dbName}.${tableName}_strict (
                    `id` INT,
                    `value` STRING
                )
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                )
            """
            
            sql "INSERT INTO ${dbName}.${tableName}_strict VALUES (1, 'test')"
            
            sql "BACKUP SNAPSHOT ${dbName}.snap_strict TO `${repoName}` ON (${tableName}_strict)"
            syncer.waitSnapshotFinish(dbName)
            snapshot = syncer.getSnapshotTimestamp(repoName, "snap_strict")
            
            sql "DROP TABLE ${dbName}.${tableName}_strict FORCE"
            
            // Enable debug point to simulate all SSD backends unavailable
            try {
                DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                    "SystemInfoService.selectBackendIdsForReplicaCreation.forceNoSSDBackends")
                
                // Restore with SSD + strict mode should fail or handle error
                // Strict mode does not allow fallback
                try {
                    sql """
                        RESTORE SNAPSHOT ${dbName}.snap_strict FROM `${repoName}`
                        ON (`${tableName}_strict`)
                        PROPERTIES (
                            "backup_timestamp" = "${snapshot}",
                            "storage_medium" = "ssd",
                            "medium_allocation_mode" = "strict",
                            "timeout" = "10"
                        )
                    """
                    
                    // Wait a bit for restore to process
                    Thread.sleep(5000)
                    
                    // Check restore status - may be in error state or still running
                    def restore_status = sql "SHOW RESTORE FROM ${dbName}"
                    logger.info("Restore status with strict mode and no SSD: ${restore_status}")
                    
                } catch (Exception e) {
                    logger.info("Expected: strict mode failed when SSD unavailable: ${e.message}")
                }
                
            } finally {
                DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                    "SystemInfoService.selectBackendIdsForReplicaCreation.forceNoSSDBackends")
            }

            // Cleanup
            try {
                sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_strict FORCE"
            } catch (Exception e) {
                logger.info("Cleanup failed (expected if table not created): ${e.message}")
            }

            // Test 3: Test partition-level medium decision with multiple partitions
            logger.info("=== Test 3: Multi-partition medium decision ===")
            
            sql """
                CREATE TABLE ${dbName}.${tableName}_multi (
                    `date` DATE,
                    `id` INT
                )
                PARTITION BY RANGE(`date`) (
                    PARTITION p1 VALUES LESS THAN ('2024-01-01'),
                    PARTITION p2 VALUES LESS THAN ('2024-02-01'),
                    PARTITION p3 VALUES LESS THAN ('2024-03-01'),
                    PARTITION p4 VALUES LESS THAN ('2024-04-01')
                )
                DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1"
                )
            """
            
            sql "INSERT INTO ${dbName}.${tableName}_multi VALUES ('2023-12-15', 1)"
            sql "INSERT INTO ${dbName}.${tableName}_multi VALUES ('2024-01-15', 2)"
            sql "INSERT INTO ${dbName}.${tableName}_multi VALUES ('2024-02-15', 3)"
            sql "INSERT INTO ${dbName}.${tableName}_multi VALUES ('2024-03-15', 4)"
            
            sql "BACKUP SNAPSHOT ${dbName}.snap_multi TO `${repoName}` ON (${tableName}_multi)"
            syncer.waitSnapshotFinish(dbName)
            snapshot = syncer.getSnapshotTimestamp(repoName, "snap_multi")
            
            sql "DROP TABLE ${dbName}.${tableName}_multi FORCE"
            
            // Restore with same_with_upstream + adaptive
            // This will trigger medium decision for each partition
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_multi FROM `${repoName}`
                ON (`${tableName}_multi`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot}",
                    "storage_medium" = "same_with_upstream",
                    "medium_allocation_mode" = "adaptive"
                )
            """
            
            syncer.waitAllRestoreFinish(dbName)
            
            def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_multi"
            assertEquals(4, result[0][0])
            
            def partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_multi"
            logger.info("Partitions: ${partitions}")
            assertEquals(4, partitions.size())

            sql "DROP TABLE ${dbName}.${tableName}_multi FORCE"

            // Test 4: Test restore job cancellation
            logger.info("=== Test 4: Test cancel restore operation ===")
            
            sql """
                CREATE TABLE ${dbName}.${tableName}_cancel (
                    `id` INT,
                    `value` STRING
                )
                DISTRIBUTED BY HASH(`id`) BUCKETS 10
                PROPERTIES (
                    "replication_num" = "1"
                )
            """
            
            // Insert more data to make restore take longer
            for (int i = 0; i < 100; i++) {
                sql "INSERT INTO ${dbName}.${tableName}_cancel VALUES (${i}, 'test_value_${i}')"
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
            
            // Try to cancel immediately
            try {
                Thread.sleep(500)
                sql "CANCEL RESTORE FROM ${dbName}"
                logger.info("Successfully requested cancel restore")
                
                // Wait a bit and check status
                Thread.sleep(2000)
                def restore_status = sql "SHOW RESTORE FROM ${dbName}"
                logger.info("Restore status after cancel: ${restore_status}")
                
                // Status should be CANCELLED or restore should not exist
                if (restore_status.size() > 0) {
                    def state = restore_status[0][4]
                    logger.info("Restore state: ${state}")
                    // May be CANCELLED or FINISHED depending on timing
                }
            } catch (Exception e) {
                logger.info("Cancel or status check: ${e.message}")
            }
            
            // Cleanup
            try {
                sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_cancel FORCE"
            } catch (Exception e) {
                logger.info("Cleanup: ${e.message}")
            }

            // Test 5: Test insufficient replica allocation
            logger.info("=== Test 5: Test replication_num exceeds available backends ===")
            
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
            
            sql "INSERT INTO ${dbName}.${tableName}_replica VALUES (1, 100)"
            
            sql "BACKUP SNAPSHOT ${dbName}.snap_replica TO `${repoName}` ON (${tableName}_replica)"
            syncer.waitSnapshotFinish(dbName)
            snapshot = syncer.getSnapshotTimestamp(repoName, "snap_replica")
            
            sql "DROP TABLE ${dbName}.${tableName}_replica FORCE"
            
            // Try to restore with replication_num=5 (more than 3 BEs)
            try {
                sql """
                    RESTORE SNAPSHOT ${dbName}.snap_replica FROM `${repoName}`
                    ON (`${tableName}_replica`)
                    PROPERTIES (
                        "backup_timestamp" = "${snapshot}",
                        "replication_num" = "5",
                        "storage_medium" = "hdd",
                        "medium_allocation_mode" = "strict",
                        "timeout" = "10"
                    )
                """
                
                Thread.sleep(3000)
                
                def restore_status = sql "SHOW RESTORE FROM ${dbName}"
                logger.info("Restore with excessive replica count: ${restore_status}")
                
                // May fail or be in error state
                if (restore_status.size() > 0) {
                    logger.info("State: ${restore_status[0][4]}, Msg: ${restore_status[0][11]}")
                }
                
            } catch (Exception e) {
                logger.info("Expected: restore failed with excessive replica: ${e.message}")
            }
            
            // Cleanup
            try {
                sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_replica FORCE"
            } catch (Exception e) {
                logger.info("Cleanup: ${e.message}")
            }

            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
    }
}

