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

suite("test_backup_restore_replica_timeout_docker", "docker,backup_restore") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.enableDebugPoints()
    
    // Enable debug logging for org.apache.doris.backup package
    // Setting both sys_log_verbose_modules (for the package) and sys_log_level (to ensure DEBUG is enabled)
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup',
        'sys_log_level=DEBUG'
    ]

    docker(options) {
        String suiteName = "test_br_timeout"
        String repoName = "${suiteName}_repo"
        String dbName = "${suiteName}_db"
        String tableName = "${suiteName}_table"

        def syncer = getSyncer()
        syncer.createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

        def feHttpAddress = context.config.feHttpAddress.split(":")
        def feHost = feHttpAddress[0]
        def fePort = feHttpAddress[1] as int

        // Test 1: Simulate create replica timeout scenario
        logger.info("=== Test 1: Create replica timeout during restore ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_timeout (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 10
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        // Insert enough data
        for (int i = 0; i < 100; i++) {
            sql "INSERT INTO ${dbName}.${tableName}_timeout VALUES (${i}, 'test_${i}')"
        }
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_timeout TO `${repoName}` ON (${tableName}_timeout)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_timeout")
        assertTrue(snapshot != null)
        
        sql "DROP TABLE ${dbName}.${tableName}_timeout FORCE"
        
        // Use debug point to slow down create replica
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "RestoreJob.waitingAllReplicasCreated.slow_down")
            
            // Start restore
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_timeout FROM `${repoName}`
                ON (`${tableName}_timeout`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot}",
                    "storage_medium" = "hdd",
                    "medium_allocation_mode" = "adaptive",
                    "timeout" = "30"
                )
            """
            
            // Wait and check status
            Thread.sleep(35000)  // Wait for timeout
            
            def restore_status = sql "SHOW RESTORE FROM ${dbName}"
            if (restore_status.size() > 0) {
                logger.info("Restore status after timeout: ${restore_status[0]}")
                def state = restore_status[0][4]
                logger.info("State: ${state}")
                // May be CANCELLED or still running
            }
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "RestoreJob.waitingAllReplicasCreated.slow_down")
        }
        
        // Cleanup
        try {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_timeout FORCE"
        } catch (Exception e) {
            logger.info("Cleanup: ${e.message}")
        }

        // Test 2: Test with reserve_replica to cover that branch
        logger.info("=== Test 2: Restore with reserve_replica enabled ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_reserve (
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
                "replication_num" = "2"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_reserve VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_reserve VALUES ('2024-01-15', 2, 200)"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_reserve TO `${repoName}` ON (${tableName}_reserve)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_reserve")
        
        sql "DROP TABLE ${dbName}.${tableName}_reserve FORCE"
        
        // Restore with reserve_replica=true and storage_medium
        // This will trigger the "if (reserveReplica)" branch in the code
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_reserve FROM `${repoName}`
            ON (`${tableName}_reserve`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_reserve"
        assertEquals(2, result[0][0])
        
        def partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_reserve"
        logger.info("Partitions with reserve_replica: ${partitions}")
        assertEquals(2, partitions.size())
        
        sql "DROP TABLE ${dbName}.${tableName}_reserve FORCE"

        // Test 3: Test exception handling during partition decision
        logger.info("=== Test 3: Exception handling during medium decision ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_exception (
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
        
        sql "INSERT INTO ${dbName}.${tableName}_exception VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_exception VALUES ('2024-01-15', 2, 200)"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_exception TO `${repoName}` ON (${tableName}_exception)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_exception")
        
        sql "DROP TABLE ${dbName}.${tableName}_exception FORCE"
        
        // Try to trigger exception by using strict mode with insufficient resources
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "MediumDecisionMaker.decideForNewPartition.throw_exception")
            
            try {
                sql """
                    RESTORE SNAPSHOT ${dbName}.snap_exception FROM `${repoName}`
                    ON (`${tableName}_exception`)
                    PROPERTIES (
                        "backup_timestamp" = "${snapshot}",
                        "storage_medium" = "ssd",
                        "medium_allocation_mode" = "strict"
                    )
                """
                
                Thread.sleep(5000)
                
                def restore_status = sql "SHOW RESTORE FROM ${dbName}"
                if (restore_status.size() > 0) {
                    logger.info("Restore with exception: ${restore_status[0]}")
                }
                
            } catch (Exception e) {
                logger.info("Expected: exception during medium decision: ${e.message}")
            }
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "MediumDecisionMaker.decideForNewPartition.throw_exception")
        }
        
        try {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_exception FORCE"
        } catch (Exception e) {
            logger.info("Cleanup: ${e.message}")
        }

        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
    }
}

