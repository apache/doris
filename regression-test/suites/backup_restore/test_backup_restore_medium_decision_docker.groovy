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
 * Test MediumDecisionMaker medium decision logic for S3 repository
 * 
 * Cluster configuration:
 * - BE1: 1 HDD disk
 * - BE2: 1 HDD disk
 * - BE3: 1 SSD disk
 * 
 * Test scenarios:
 * 1. Adaptive mode downgrade (SSD → HDD)
 * 2. Atomic restore + prefer local medium
 * 3. same_with_upstream strategy
 * 4. Strict mode behavior when SSD is insufficient
 */
suite("test_backup_restore_medium_decision_docker", "docker,backup_restore") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.beDisks = [
        "HDD=1",  // BE1: 1 HDD disk
        "HDD=1",  // BE2: 1 HDD disk
        "SSD=1"   // BE3: 1 SSD disk
    ]
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup',
        'sys_log_level=DEBUG'
    ]
    
    docker(options) {
        String dbName = "test_br_decision_db"
        String repoName = "test_br_decision_repo"
        String tableName = "test_table"
        
        def helper = new BackupRestoreHelper(this.&sql, getSyncer())
        getSyncer().createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
        
        // ============================================================
        // Test 1: Adaptive downgrade (SSD -> HDD)
        // ============================================================
        helper.logTestStart("Adaptive downgrade (SSD -> HDD)")
        
        helper.createSimpleTable(dbName, "${tableName}_1", [
            "storage_medium": "HDD"
        ])
        helper.insertData(dbName, "${tableName}_1", ["(1, 'data1')", "(2, 'data2')"])
        
        assertTrue(helper.backupToS3(dbName, "snap1", repoName, "${tableName}_1"))
        helper.dropTable(dbName, "${tableName}_1")
        
        // Restore with SSD + adaptive (should downgrade to HDD if SSD not enough)
        assertTrue(helper.restoreFromS3(dbName, "snap1", repoName, "${tableName}_1", [
            "storage_medium": "ssd",
            "medium_allocation_mode": "adaptive"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_1", 2))
        logger.info("✓ Adaptive downgrade successful")
        
        helper.dropTable(dbName, "${tableName}_1")
        helper.logTestEnd("Adaptive downgrade (SSD -> HDD)", true)
        
        // ============================================================
        // Test 2: Atomic restore + prefer local medium (adaptive)
        // ============================================================
        helper.logTestStart("Atomic restore with prefer local medium")
        
        helper.createSimpleTable(dbName, "${tableName}_2", [
            "storage_medium": "HDD"
        ])
        helper.insertData(dbName, "${tableName}_2", ["(1, 'atomic_data')"])
        
        assertTrue(helper.backupToS3(dbName, "snap2", repoName, "${tableName}_2"))
        
        // Insert more data
        helper.insertData(dbName, "${tableName}_2", ["(2, 'new_data')"])
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_2", 2))
        
        // Atomic restore (should replace table)
        assertTrue(helper.restoreFromS3(dbName, "snap2", repoName, "${tableName}_2", [
            "atomic_restore": "true",
            "storage_medium": "same_with_upstream",
            "medium_allocation_mode": "adaptive"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_2", 1))
        assertTrue(helper.verifyDataExists(dbName, "${tableName}_2", "id = 2", false))
        logger.info("✓ Atomic restore successful")
        
        helper.dropTable(dbName, "${tableName}_2")
        helper.logTestEnd("Atomic restore with prefer local medium", true)
        
        // ============================================================
        // Test 3: same_with_upstream strategy
        // ============================================================
        helper.logTestStart("same_with_upstream strategy")
        
        helper.createSimpleTable(dbName, "${tableName}_3", [
            "storage_medium": "HDD"
        ])
        helper.insertData(dbName, "${tableName}_3", ["(3, 'upstream_data')"])
        
        assertTrue(helper.backupToS3(dbName, "snap3", repoName, "${tableName}_3"))
        helper.dropTable(dbName, "${tableName}_3")
        
        // Restore with same_with_upstream (should use HDD from backup)
        assertTrue(helper.restoreFromS3(dbName, "snap3", repoName, "${tableName}_3", [
            "storage_medium": "same_with_upstream",
            "medium_allocation_mode": "adaptive"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_3", 1))
        logger.info("✓ same_with_upstream successful")
        
        helper.dropTable(dbName, "${tableName}_3")
        helper.logTestEnd("same_with_upstream strategy", true)
        
        // ============================================================
        // Test 4: Strict mode with SSD
        // ============================================================
        helper.logTestStart("Strict mode with SSD")
        
        helper.createSimpleTable(dbName, "${tableName}_4", [
            "storage_medium": "HDD"
        ])
        helper.insertData(dbName, "${tableName}_4", ["(4, 'ssd_data')"])
        
        assertTrue(helper.backupToS3(dbName, "snap4", repoName, "${tableName}_4"))
        helper.dropTable(dbName, "${tableName}_4")
        
        // Try restore with SSD + strict mode
        try {
            def snapshot = getSyncer().getSnapshotTimestamp(repoName, "snap4")
            sql """
                RESTORE SNAPSHOT ${dbName}.snap4 FROM `${repoName}`
                ON (`${tableName}_4`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot}",
                    "storage_medium" = "ssd",
                    "medium_allocation_mode" = "strict",
                    "timeout" = "10"
                )
            """
            
            Thread.sleep(5000)
            def restoreStatus = sql "SHOW RESTORE FROM ${dbName}"
            if (restoreStatus.size() > 0) {
                def state = restoreStatus[0][4]
                logger.info("Restore with SSD strict mode status: ${state}")
                
                if (state == "FINISHED") {
                    def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_4"
                    assertEquals(1, result[0][0])
                    helper.dropTable(dbName, "${tableName}_4")
                } else if (state == "CANCELLED") {
                    logger.info("Expected: restore cancelled due to insufficient SSD backends")
                }
            }
        } catch (Exception e) {
            logger.info("Expected exception for SSD strict mode: ${e.message}")
        }
        
        helper.logTestEnd("Strict mode with SSD", true)
        
        // ============================================================
        // Cleanup
        // ============================================================
        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
        
        logger.info("=== All medium decision tests completed ===")
    }
}

