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

suite("test_backup_restore_syncer_api", "backup_restore") {
    /**
     * Test Syncer API for backup/restore operations
     * 
     * PURPOSE: Cover Thrift RPC code path in FrontendServiceImpl.restoreSnapshot
     * 
     * CRITICAL: This test is REQUIRED to cover the following code that CANNOT be
     * covered by SQL RESTORE commands:
     *   - FrontendServiceImpl.restoreSnapshot: request.isSetStorageMedium()
     *   - FrontendServiceImpl.restoreSnapshot: request.isSetMediumAllocationMode()
     *   - TRestoreSnapshotRequest: storage_medium field
     *   - TRestoreSnapshotRequest: medium_allocation_mode field
     * 
     * SQL RESTORE commands do NOT call FrontendServiceImpl.restoreSnapshot.
     * Only Syncer.restoreSnapshot() API calls this method via Thrift RPC.
     * 
     * Test scenarios:
     * 1. Basic Syncer API restore (baseline)
     * 2. Syncer API restore with storage_medium parameter
     * 3. Syncer API restore with medium_allocation_mode parameter
     * 4. Syncer API restore with both parameters
     */
    
    String suiteName = "test_backup_restore_syncer_api"
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"
    
    def syncer = getSyncer()
    def helper = new BackupRestoreHelper(sql, syncer)
    
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    
    try {
        // ==================================================================
        // Scenario 1: Basic Syncer API restore (baseline)
        // ==================================================================
        logger.info("=== Scenario 1: Basic Syncer API restore ===")
        
        // Create table and insert data
        helper.createSimpleTable(dbName, tableName, ["storage_medium": "hdd"])
        helper.insertData(dbName, tableName, 1, 5)
        helper.verifyRowCount(dbName, tableName, 5)
        
        // Backup using local repository
        assertTrue(helper.backupToLocal(dbName, "${snapshotName}_1", tableName))
        
        // Truncate and verify
        helper.truncateTable(dbName, tableName)
        helper.verifyRowCount(dbName, tableName, 0)
        
        // Restore using Syncer API (tests Thrift RPC path)
        assertTrue(helper.restoreFromLocal(dbName, "${snapshotName}_1", tableName))
        helper.verifyRowCount(dbName, tableName, 5)
        
        logger.info("✓ Scenario 1 passed: Basic Syncer API restore works")
        
        // ==================================================================
        // Scenario 2: Syncer API restore with storage_medium
        // ==================================================================
        logger.info("=== Scenario 2: Syncer API restore with storage_medium ===")
        
        sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_2"
        helper.createSimpleTable(dbName, "${tableName}_2", ["storage_medium": "hdd"])
        helper.insertData(dbName, "${tableName}_2", 1, 10)
        
        // Backup
        assertTrue(helper.backupToLocal(dbName, "${snapshotName}_2", "${tableName}_2"))
        
        // Truncate
        helper.truncateTable(dbName, "${tableName}_2")
        helper.verifyRowCount(dbName, "${tableName}_2", 0)
        
        // Restore with storage_medium parameter via Syncer API
        // This will test: FrontendServiceImpl.restoreSnapshot -> request.isSetStorageMedium()
        assertTrue(helper.restoreFromLocal(dbName, "${snapshotName}_2", "${tableName}_2", 
                   ["storage_medium": "hdd"]))
        helper.verifyRowCount(dbName, "${tableName}_2", 10)
        
        logger.info("✓ Scenario 2 passed: Syncer API with storage_medium parameter works")
        
        // ==================================================================
        // Scenario 3: Syncer API restore with medium_allocation_mode
        // ==================================================================
        logger.info("=== Scenario 3: Syncer API restore with medium_allocation_mode ===")
        
        sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_3"
        helper.createSimpleTable(dbName, "${tableName}_3", ["storage_medium": "hdd"])
        helper.insertData(dbName, "${tableName}_3", 1, 15)
        
        // Backup
        assertTrue(helper.backupToLocal(dbName, "${snapshotName}_3", "${tableName}_3"))
        
        // Truncate
        helper.truncateTable(dbName, "${tableName}_3")
        helper.verifyRowCount(dbName, "${tableName}_3", 0)
        
        // Restore with medium_allocation_mode parameter via Syncer API
        // This will test: FrontendServiceImpl.restoreSnapshot -> request.isSetMediumAllocationMode()
        assertTrue(helper.restoreFromLocal(dbName, "${snapshotName}_3", "${tableName}_3", 
                   ["medium_allocation_mode": "adaptive"]))
        helper.verifyRowCount(dbName, "${tableName}_3", 15)
        
        logger.info("✓ Scenario 3 passed: Syncer API with medium_allocation_mode parameter works")
        
        // ==================================================================
        // Scenario 4: Syncer API restore with both parameters
        // ==================================================================
        logger.info("=== Scenario 4: Syncer API restore with both parameters ===")
        
        sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_4"
        helper.createSimpleTable(dbName, "${tableName}_4", ["storage_medium": "hdd"])
        helper.insertData(dbName, "${tableName}_4", 1, 20)
        
        // Backup
        assertTrue(helper.backupToLocal(dbName, "${snapshotName}_4", "${tableName}_4"))
        
        // Truncate
        helper.truncateTable(dbName, "${tableName}_4")
        helper.verifyRowCount(dbName, "${tableName}_4", 0)
        
        // Restore with both parameters via Syncer API
        // This will test both code paths in FrontendServiceImpl.restoreSnapshot
        assertTrue(helper.restoreFromLocal(dbName, "${snapshotName}_4", "${tableName}_4", 
                   ["storage_medium": "hdd", "medium_allocation_mode": "strict"]))
        helper.verifyRowCount(dbName, "${tableName}_4", 20)
        
        logger.info("✓ Scenario 4 passed: Syncer API with both parameters works")
        
        logger.info("=== All Syncer API tests passed ===")
        
    } finally {
        sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
    }
}
