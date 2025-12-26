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

suite("test_backup_restore_error_scenarios_docker", "docker,backup_restore") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.enableDebugPoints()
    
    // Enable debug logging
    options.feConfigs += ['sys_log_verbose_modules=org.apache.doris.backup']

    docker(options) {
        String suiteName = "test_br_errors"
        String repoName = "${suiteName}_repo"
        String dbName = "${suiteName}_db"
        String tableName = "${suiteName}_table"

        def syncer = getSyncer()
        syncer.createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

        def feHttpAddress = context.config.feHttpAddress.split(":")
        def feHost = feHttpAddress[0]
        def fePort = feHttpAddress[1] as int

        // Test 1: Simulate download failure and retry
        logger.info("=== Test 1: Download failure and retry ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_download (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_download VALUES (1, 'test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_download TO `${repoName}` ON (${tableName}_download)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_download")
        
        sql "DROP TABLE ${dbName}.${tableName}_download FORCE"
        
        // Simulate download failure first time
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "RestoreJob.downloadSnapshots.firstAttemptFails")
            
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_download FROM `${repoName}`
                ON (`${tableName}_download`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot}",
                    "storage_medium" = "hdd",
                    "medium_allocation_mode" = "adaptive"
                )
            """
            
            // Should retry and eventually succeed
            syncer.waitAllRestoreFinish(dbName)
            
            def result = sql "SELECT * FROM ${dbName}.${tableName}_download"
            assertEquals(1, result.size(), "Should succeed after retry")
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "RestoreJob.downloadSnapshots.firstAttemptFails")
        }
        
        sql "DROP TABLE ${dbName}.${tableName}_download FORCE"

        // Test 2: All backends exhausted scenario
        logger.info("=== Test 2: All backend types exhausted ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_exhausted (
                `id` INT,
                `value` INT
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_exhausted VALUES (1, 100)"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_exhausted TO `${repoName}` ON (${tableName}_exhausted)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_exhausted")
        
        sql "DROP TABLE ${dbName}.${tableName}_exhausted FORCE"
        
        // Force all backend types unavailable
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "SystemInfoService.selectBackendIdsForReplicaCreation.forceAllBackendsUnavailable")
            
            // With adaptive mode, should try to find ANY available backend
            try {
                sql """
                    RESTORE SNAPSHOT ${dbName}.snap_exhausted FROM `${repoName}`
                    ON (`${tableName}_exhausted`)
                    PROPERTIES (
                        "backup_timestamp" = "${snapshot}",
                        "storage_medium" = "ssd",
                        "medium_allocation_mode" = "adaptive",
                        "timeout" = "10"
                    )
                """
                
                Thread.sleep(5000)
                
                def restore_status = sql "SHOW RESTORE FROM ${dbName}"
                logger.info("Restore with all BEs unavailable: ${restore_status}")
                
                if (restore_status.size() > 0) {
                    logger.info("State: ${restore_status[0][4]}, Msg: ${restore_status[0][11]}")
                }
                
            } catch (Exception e) {
                logger.info("Expected: restore failed with no available backends: ${e.message}")
            }
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "SystemInfoService.selectBackendIdsForReplicaCreation.forceAllBackendsUnavailable")
        }
        
        try {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_exhausted FORCE"
        } catch (Exception e) {
            logger.info("Cleanup: ${e.message}")
        }

        // Test 3: Repository access error
        logger.info("=== Test 3: Repository access error ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_repo (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_repo VALUES (1, 'data')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_repo TO `${repoName}` ON (${tableName}_repo)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_repo")
        
        sql "DROP TABLE ${dbName}.${tableName}_repo FORCE"
        
        // Simulate repository read error
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "Repository.readMetadata.simulateIOError")
            
            try {
                sql """
                    RESTORE SNAPSHOT ${dbName}.snap_repo FROM `${repoName}`
                    ON (`${tableName}_repo`)
                    PROPERTIES (
                        "backup_timestamp" = "${snapshot}",
                        "storage_medium" = "hdd",
                        "medium_allocation_mode" = "strict"
                    )
                """
                
                Thread.sleep(3000)
                
                def restore_status = sql "SHOW RESTORE FROM ${dbName}"
                logger.info("Restore with repo error: ${restore_status}")
                
            } catch (Exception e) {
                logger.info("Expected: restore failed with repo error: ${e.message}")
            }
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "Repository.readMetadata.simulateIOError")
        }
        
        try {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_repo FORCE"
        } catch (Exception e) {
            logger.info("Cleanup: ${e.message}")
        }

        // Test 4: Progressive downgrade (SSD -> HDD -> ANY)
        logger.info("=== Test 4: Progressive downgrade test ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_downgrade (
                `id` INT,
                `value` INT
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_downgrade VALUES (1, 1), (2, 2)"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_downgrade TO `${repoName}` ON (${tableName}_downgrade)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_downgrade")
        
        sql "DROP TABLE ${dbName}.${tableName}_downgrade FORCE"
        
        // First: No SSD, should downgrade to HDD
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "SystemInfoService.selectBackendIdsForReplicaCreation.forceNoSSDBackends")
            
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_downgrade FROM `${repoName}`
                ON (`${tableName}_downgrade`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot}",
                    "storage_medium" = "ssd",
                    "medium_allocation_mode" = "adaptive"
                )
            """
            
            syncer.waitAllRestoreFinish(dbName)
            
            def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_downgrade"
            assertEquals(2, result[0][0], "Should succeed with downgrade to HDD")
            
            logger.info("Successfully downgraded from SSD to HDD")
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "SystemInfoService.selectBackendIdsForReplicaCreation.forceNoSSDBackends")
        }
        
        sql "DROP TABLE ${dbName}.${tableName}_downgrade FORCE"

        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
    }
}

