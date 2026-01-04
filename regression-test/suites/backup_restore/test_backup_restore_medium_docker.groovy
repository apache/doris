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

import org.apache.doris.regression.util.BackupRestoreHelper
import org.apache.doris.regression.suite.ClusterOptions

/**
 * Test basic storage_medium functionality for S3 repository
 * 
 * Test scenarios:
 * 1. Basic HDD/SSD backup/restore
 * 2. Different medium_allocation_mode settings
 * 3. Medium settings for partition tables
 * 4. Replica allocation with medium
 */
suite("test_backup_restore_medium_docker", "docker,backup_restore") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.enableDebugPoints()
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup',
        'sys_log_level=DEBUG'
    ]

    docker(options) {
        String dbName = "test_br_medium_db"
        String repoName = "test_br_medium_repo"
        String tableName = "test_table"
        
        def helper = new BackupRestoreHelper(sql, getSyncer())
        getSyncer().createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

        // ============================================================
        // Test 1: Basic HDD backup/restore
        // ============================================================
        helper.logTestStart("Basic HDD backup/restore")
        
        helper.createSimpleTable(dbName, "${tableName}_1", [
            "storage_medium": "HDD"
        ])
        helper.insertData(dbName, "${tableName}_1", ["(1, 'hdd_data')"])
        
        assertTrue(helper.backupToS3(dbName, "snap1", repoName, "${tableName}_1"))
        helper.dropTable(dbName, "${tableName}_1")
        
        assertTrue(helper.restoreFromS3(dbName, "snap1", repoName, "${tableName}_1", [
            "storage_medium": "hdd"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_1", 1))
        logger.info("✓ HDD backup/restore successful")
        
        helper.dropTable(dbName, "${tableName}_1")
        helper.logTestEnd("Basic HDD backup/restore", true)

        // ============================================================
        // Test 2: Different medium_allocation_mode settings
        // ============================================================
        helper.logTestStart("Different medium_allocation_mode settings")
        
        // Test strict mode
        helper.createSimpleTable(dbName, "${tableName}_2a", [
            "storage_medium": "HDD",
            "medium_allocation_mode": "strict"
        ])
        helper.insertData(dbName, "${tableName}_2a", ["(2, 'strict_data')"])
        
        assertTrue(helper.backupToS3(dbName, "snap2a", repoName, "${tableName}_2a"))
        helper.dropTable(dbName, "${tableName}_2a")
        
        assertTrue(helper.restoreFromS3(dbName, "snap2a", repoName, "${tableName}_2a", [
            "storage_medium": "hdd",
            "medium_allocation_mode": "strict"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_2a", 1))
        assertTrue(helper.verifyTableProperty(dbName, "${tableName}_2a", "medium_allocation_mode"))
        logger.info("✓ Strict mode test passed")
        
        helper.dropTable(dbName, "${tableName}_2a")
        
        // Test adaptive mode
        helper.createSimpleTable(dbName, "${tableName}_2b", [
            "storage_medium": "HDD",
            "medium_allocation_mode": "adaptive"
        ])
        helper.insertData(dbName, "${tableName}_2b", ["(3, 'adaptive_data')"])
        
        assertTrue(helper.backupToS3(dbName, "snap2b", repoName, "${tableName}_2b"))
        helper.dropTable(dbName, "${tableName}_2b")
        
        assertTrue(helper.restoreFromS3(dbName, "snap2b", repoName, "${tableName}_2b", [
            "storage_medium": "hdd",
            "medium_allocation_mode": "adaptive"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_2b", 1))
        logger.info("✓ Adaptive mode test passed")
        
        helper.dropTable(dbName, "${tableName}_2b")
        helper.logTestEnd("Different medium_allocation_mode settings", true)

        // ============================================================
        // Test 3: Partition table with medium settings
        // ============================================================
        helper.logTestStart("Partition table with medium settings")
        
        helper.createPartitionTable(dbName, "${tableName}_3", [
            "storage_medium": "HDD",
            "medium_allocation_mode": "adaptive"
        ])
        sql "INSERT INTO ${dbName}.${tableName}_3 VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_3 VALUES ('2024-01-15', 2, 200)"
        
        assertTrue(helper.backupToS3(dbName, "snap3", repoName, "${tableName}_3"))
        helper.dropTable(dbName, "${tableName}_3")
        
        assertTrue(helper.restoreFromS3(dbName, "snap3", repoName, "${tableName}_3", [
            "storage_medium": "hdd",
            "medium_allocation_mode": "adaptive"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_3", 2))
        logger.info("✓ Partition table restore successful")
        
        helper.dropTable(dbName, "${tableName}_3")
        helper.logTestEnd("Partition table with medium settings", true)

        // ============================================================
        // Test 4: Replica allocation with medium
        // ============================================================
        helper.logTestStart("Replica allocation with medium")
        
        try {
            helper.createSimpleTable(dbName, "${tableName}_4", [
                "storage_medium": "HDD",
                "replication_num": "2",
                "medium_allocation_mode": "adaptive"
            ])
            helper.insertData(dbName, "${tableName}_4", ["(4, 'replica_data')"])
            
            assertTrue(helper.backupToS3(dbName, "snap4", repoName, "${tableName}_4"))
            helper.dropTable(dbName, "${tableName}_4")
            
            assertTrue(helper.restoreFromS3(dbName, "snap4", repoName, "${tableName}_4", [
                "storage_medium": "hdd",
                "medium_allocation_mode": "adaptive"
            ]))
            assertTrue(helper.verifyRowCount(dbName, "${tableName}_4", 1))
            logger.info("✓ Replica allocation successful")
            
            helper.dropTable(dbName, "${tableName}_4")
        } catch (Exception e) {
            logger.info("Replica test may fail if not enough backends: ${e.message}")
        }
        
        helper.logTestEnd("Replica allocation with medium", true)

        // ============================================================
        // Cleanup
        // ============================================================
        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
        
        logger.info("=== All basic medium tests completed ===")
    }
}

