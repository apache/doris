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

suite("test_backup_restore_medium_decision_docker", "docker") {
    String suiteName = "test_br_medium_decision_docker"
    String repoName = "${suiteName}_repo"
    String dbName = "${suiteName}_db"
    
    def options = new org.apache.doris.regression.suite.ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    
    // Enable debug logging for org.apache.doris.backup package
    // Setting both sys_log_verbose_modules (for the package) and sys_log_level (to ensure DEBUG is enabled)
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup',
        'sys_log_level=DEBUG'
    ]
    
    // Configure BEs with different disk types
    options.beDisks = [
        "HDD=1",  // First BE: 1 HDD disk
        "HDD=1",  // Second BE: 1 HDD disk
        "SSD=1"   // Third BE: 1 SSD disk
    ]
    
    docker(options) {
        def syncer = getSyncer()
        syncer.createS3Repository(repoName)
        
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
        
        // Test 1: Adaptive mode downgrade - SSD not available, downgrade to HDD
        logger.info("=== Test 1: Adaptive mode downgrade (SSD -> HDD) ===")
        
        sql """
            CREATE TABLE ${dbName}.table_adaptive_downgrade (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.table_adaptive_downgrade VALUES (1, 'data1'), (2, 'data2')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_downgrade TO `${repoName}` ON (table_adaptive_downgrade)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot1 = syncer.getSnapshotTimestamp(repoName, "snap_downgrade")
        
        sql "DROP TABLE ${dbName}.table_adaptive_downgrade FORCE"
        
        // Restore with SSD + adaptive mode
        // Since we only have 1 SSD BE, it may not have enough capacity
        // This should trigger MediumDecisionMaker.decideForNewPartition with downgrade
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_downgrade FROM `${repoName}`
            ON (`table_adaptive_downgrade`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot1}",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        def result = sql "SELECT COUNT(*) FROM ${dbName}.table_adaptive_downgrade"
        assertEquals(2, result[0][0])
        
        sql "DROP TABLE ${dbName}.table_adaptive_downgrade FORCE"
        
        // Test 2: Atomic restore with same_with_upstream + adaptive (prefer local)
        logger.info("=== Test 2: Atomic restore - prefer local medium (adaptive) ===")
        
        sql """
            CREATE TABLE ${dbName}.table_atomic_prefer_local (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.table_atomic_prefer_local VALUES (1, 'v1')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_atomic1 TO `${repoName}` ON (table_atomic_prefer_local)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot2 = syncer.getSnapshotTimestamp(repoName, "snap_atomic1")
        
        sql "INSERT INTO ${dbName}.table_atomic_prefer_local VALUES (2, 'v2')"
        
        // Atomic restore with same_with_upstream + adaptive
        // This should trigger MediumDecisionMaker.decideForAtomicRestore
        // -> decidePreferLocalMedium (local HDD available, should prefer it)
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_atomic1 FROM `${repoName}`
            ON (`table_atomic_prefer_local`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot2}",
                "atomic_restore" = "true",
                "storage_medium" = "same_with_upstream",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.table_atomic_prefer_local"
        assertEquals(1, result[0][0])
        
        sql "DROP TABLE ${dbName}.table_atomic_prefer_local FORCE"
        
        // Test 3: Atomic restore with same_with_upstream + strict
        logger.info("=== Test 3: Atomic restore - local medium strict mode ===")
        
        sql """
            CREATE TABLE ${dbName}.table_atomic_strict (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.table_atomic_strict VALUES (1, 'data')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_strict TO `${repoName}` ON (table_atomic_strict)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot3 = syncer.getSnapshotTimestamp(repoName, "snap_strict")
        
        sql "INSERT INTO ${dbName}.table_atomic_strict VALUES (2, 'more')"
        
        // Atomic restore with same_with_upstream + strict
        // This should trigger MediumDecisionMaker.decideWithLocalMediumStrict
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_strict FROM `${repoName}`
            ON (`table_atomic_strict`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot3}",
                "atomic_restore" = "true",
                "storage_medium" = "same_with_upstream",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.table_atomic_strict"
        assertEquals(1, result[0][0])
        
        sql "DROP TABLE ${dbName}.table_atomic_strict FORCE"
        
        // Test 4: Atomic restore with explicit medium (configured medium)
        logger.info("=== Test 4: Atomic restore - explicit configured medium ===")
        
        sql """
            CREATE TABLE ${dbName}.table_explicit_medium (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.table_explicit_medium VALUES (1, 'original')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_explicit TO `${repoName}` ON (table_explicit_medium)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot4 = syncer.getSnapshotTimestamp(repoName, "snap_explicit")
        
        sql "INSERT INTO ${dbName}.table_explicit_medium VALUES (2, 'modified')"
        
        // Atomic restore with explicit HDD (different from local, but same)
        // This should trigger MediumDecisionMaker.decideWithConfiguredMedium
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_explicit FROM `${repoName}`
            ON (`table_explicit_medium`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot4}",
                "atomic_restore" = "true",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.table_explicit_medium"
        assertEquals(1, result[0][0])
        
        sql "DROP TABLE ${dbName}.table_explicit_medium FORCE"
        
        // Test 5: Table-level medium decision
        logger.info("=== Test 5: Table-level medium decision ===")
        
        sql """
            CREATE TABLE ${dbName}.table_level_medium (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.table_level_medium VALUES (1, 'test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_table_level TO `${repoName}` ON (table_level_medium)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot5 = syncer.getSnapshotTimestamp(repoName, "snap_table_level")
        
        sql "DROP TABLE ${dbName}.table_level_medium FORCE"
        
        // Restore with same_with_upstream (should use upstream table-level medium)
        // This triggers MediumDecisionMaker.decideForTableLevel
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_table_level FROM `${repoName}`
            ON (`table_level_medium`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot5}",
                "storage_medium" = "same_with_upstream",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.table_level_medium"
        assertEquals(1, result[0][0])
        
        sql "DROP TABLE ${dbName}.table_level_medium FORCE"
        
        // Test 6: Restore new table with inherited upstream medium
        logger.info("=== Test 6: New table with inherited upstream medium ===")
        
        sql """
            CREATE TABLE ${dbName}.table_inherited (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.table_inherited VALUES (1, 'data')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_inherited TO `${repoName}` ON (table_inherited)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot6 = syncer.getSnapshotTimestamp(repoName, "snap_inherited")
        
        sql "DROP TABLE ${dbName}.table_inherited FORCE"
        
        // Restore with same_with_upstream + adaptive
        // This triggers MediumDecisionMaker.decideForNewPartition with upstream inheritance
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_inherited FROM `${repoName}`
            ON (`table_inherited`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot6}",
                "storage_medium" = "same_with_upstream",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.table_inherited"
        assertEquals(1, result[0][0])
        
        sql "DROP TABLE ${dbName}.table_inherited FORCE"
        
        // Test 7: Explicit SSD with strict mode (may fail if not enough SSD)
        logger.info("=== Test 7: Explicit SSD with strict mode ===")
        
        sql """
            CREATE TABLE ${dbName}.table_ssd_strict (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.table_ssd_strict VALUES (1, 'test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_ssd TO `${repoName}` ON (table_ssd_strict)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot7 = syncer.getSnapshotTimestamp(repoName, "snap_ssd")
        
        sql "DROP TABLE ${dbName}.table_ssd_strict FORCE"
        
        // Try to restore with SSD + strict mode
        try {
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_ssd FROM `${repoName}`
                ON (`table_ssd_strict`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot7}",
                    "storage_medium" = "ssd",
                    "medium_allocation_mode" = "strict"
                )
            """
            
            Thread.sleep(5000)
            
            def restoreStatus = sql "SHOW RESTORE FROM ${dbName}"
            if (restoreStatus.size() > 0) {
                def state = restoreStatus[0][4]
                logger.info("Restore with SSD strict mode status: ${state}")
                
                if (state == "FINISHED") {
                    result = sql "SELECT COUNT(*) FROM ${dbName}.table_ssd_strict"
                    assertEquals(1, result[0][0])
                    sql "DROP TABLE ${dbName}.table_ssd_strict FORCE"
                } else if (state == "CANCELLED") {
                    logger.info("Expected: restore cancelled due to insufficient SSD backends in strict mode")
                }
            }
        } catch (Exception e) {
            logger.info("Expected exception for SSD strict mode: ${e.message}")
        }
        
        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
    }
}

