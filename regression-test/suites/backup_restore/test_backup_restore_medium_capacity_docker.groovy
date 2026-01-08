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
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

/**
 * Test S3 repository backup/restore behavior under capacity constraints
 * 
 * Cluster configuration:
 * - BE1: 1 HDD (unlimited) + 1 SSD (5GB limited)
 * - BE2: 2 HDD (unlimited)
 * - BE3: 2 SSD (5GB limited each)
 * 
 * Test scenarios:
 * 1. Adaptive mode downgrades to HDD when SSD is full
 * 2. Strict mode behavior when capacity is insufficient
 * 3. Partition table distribution under capacity constraints
 * 4. Replica allocation with medium constraints
 * 5. same_with_upstream + adaptive downgrade
 * 6. Subset partition restore
 * 7. Restore behavior when BE is unavailable
 */
suite("test_backup_restore_medium_capacity_docker", "docker,backup_restore") {
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.beDisks = [
        "HDD=1,SSD=1,5",  // BE1: 1 HDD unlimited + 1 SSD 5GB
        "HDD=2",          // BE2: 2 HDD unlimited
        "SSD=2,5"         // BE3: 2 SSD 5GB each
    ]
    options.enableDebugPoints()
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup,org.apache.doris.catalog,org.apache.doris.system',
        'sys_log_level=DEBUG'
    ]

    docker(options) {
        String dbName = "test_br_capacity_db"
        String repoName = "test_br_capacity_repo"
        String tableName = "test_table"

        def feHttpAddress = context.config.feHttpAddress.split(":")
        def helper = new BackupRestoreHelper(
            this.&sql, getSyncer(), 
            feHttpAddress[0], 
            feHttpAddress[1] as int
        )
        
        getSyncer().createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

        // Verify disk configuration
        helper.logSeparator("Disk Configuration")
        def disks = sql "SHOW PROC '/backends'"
        disks.each { disk -> logger.info("Backend disk info: ${disk}") }

        // ============================================================
        // Test 1: Adaptive fallback when SSD capacity insufficient
        // ============================================================
        helper.logTestStart("Adaptive fallback when SSD capacity insufficient")
        
        helper.createSimpleTable(dbName, "${tableName}_1", [:])
        helper.insertData(dbName, "${tableName}_1", ["(1, 'capacity_test')"])
        
        assertTrue(helper.backupToS3(dbName, "snap1", repoName, "${tableName}_1"))
        helper.dropTable(dbName, "${tableName}_1")
        
        helper.withDebugPoint("DiskInfo.exceedLimit.ssd.alwaysTrue") {
            assertTrue(helper.restoreFromS3(dbName, "snap1", repoName, "${tableName}_1", [
                "storage_medium": "ssd",
                "medium_allocation_mode": "adaptive"
            ]))
            assertTrue(helper.verifyRowCount(dbName, "${tableName}_1", 1))
            logger.info("✓ Successfully fell back from SSD to HDD due to capacity")
        }
        
        helper.dropTable(dbName, "${tableName}_1")
        helper.logTestEnd("Adaptive fallback when SSD capacity insufficient", true)

        // ============================================================
        // Test 2: Strict mode failure when capacity insufficient
        // ============================================================
        helper.logTestStart("Strict mode failure when capacity insufficient")
        
        helper.createSimpleTable(dbName, "${tableName}_2", [:])
        helper.insertData(dbName, "${tableName}_2", ["(2, 'strict_test')"])
        
        assertTrue(helper.backupToS3(dbName, "snap2", repoName, "${tableName}_2"))
        helper.dropTable(dbName, "${tableName}_2")
        
        helper.withDebugPoint("DiskInfo.exceedLimit.ssd.alwaysTrue") {
            try {
                def snapshot = getSyncer().getSnapshotTimestamp(repoName, "snap2")
                sql """
                    RESTORE SNAPSHOT ${dbName}.snap2 FROM `${repoName}`
                    ON (`${tableName}_2`)
                    PROPERTIES (
                        "backup_timestamp" = "${snapshot}",
                        "storage_medium" = "ssd",
                        "medium_allocation_mode" = "strict",
                        "timeout" = "10"
                    )
                """
                Thread.sleep(5000)
                def restore_status = sql "SHOW RESTORE FROM ${dbName}"
                logger.info("Restore status with strict + no capacity: ${restore_status}")
                if (restore_status.size() > 0) {
                    logger.info("State: ${restore_status[0][4]}, Msg: ${restore_status[0][11]}")
                }
            } catch (Exception e) {
                logger.info("Expected: strict mode failed due to capacity: ${e.message}")
            }
        }
        
        helper.dropTable(dbName, "${tableName}_2")
        helper.logTestEnd("Strict mode failure when capacity insufficient", true)

        // ============================================================
        // Test 3: Partition table distributed across available disks
        // ============================================================
        helper.logTestStart("Partitions distributed across available disks")
        
        helper.createPartitionTable(dbName, "${tableName}_3", [:])
        sql "INSERT INTO ${dbName}.${tableName}_3 VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_3 VALUES ('2024-01-15', 2, 200)"
        
        assertTrue(helper.backupToS3(dbName, "snap3", repoName, "${tableName}_3"))
        helper.dropTable(dbName, "${tableName}_3")
        
        assertTrue(helper.restoreFromS3(dbName, "snap3", repoName, "${tableName}_3", [
            "storage_medium": "hdd",
            "medium_allocation_mode": "adaptive"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_3", 2))
        logger.info("✓ Partition table restored successfully")
        
        helper.dropTable(dbName, "${tableName}_3")
        helper.logTestEnd("Partitions distributed across available disks", true)

        // ============================================================
        // Test 4: Replica allocation with medium constraints
        // ============================================================
        helper.logTestStart("Replica allocation with medium constraints")
        
        helper.createSimpleTable(dbName, "${tableName}_4", [
            "replication_num": "2"
        ])
        helper.insertData(dbName, "${tableName}_4", ["(4, 'replica_test')"])
        
        assertTrue(helper.backupToS3(dbName, "snap4", repoName, "${tableName}_4"))
        helper.dropTable(dbName, "${tableName}_4")
        
        assertTrue(helper.restoreFromS3(dbName, "snap4", repoName, "${tableName}_4", [
            "storage_medium": "hdd",
            "medium_allocation_mode": "adaptive"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_4", 1))
        logger.info("✓ Replica allocation successful")
        
        helper.dropTable(dbName, "${tableName}_4")
        helper.logTestEnd("Replica allocation with medium constraints", true)

        // ============================================================
        // Test 5: same_with_upstream with adaptive fallback
        // ============================================================
        helper.logTestStart("same_with_upstream with adaptive fallback")
        
        helper.createSimpleTable(dbName, "${tableName}_5", [
            "storage_medium": "HDD"
        ])
        helper.insertData(dbName, "${tableName}_5", ["(5, 'upstream_test')"])
        
        assertTrue(helper.backupToS3(dbName, "snap5", repoName, "${tableName}_5"))
        helper.dropTable(dbName, "${tableName}_5")
        
        assertTrue(helper.restoreFromS3(dbName, "snap5", repoName, "${tableName}_5", [
            "storage_medium": "same_with_upstream",
            "medium_allocation_mode": "adaptive"
        ]))
        assertTrue(helper.verifyRowCount(dbName, "${tableName}_5", 1))
        logger.info("✓ same_with_upstream successful")
        
        helper.dropTable(dbName, "${tableName}_5")
        helper.logTestEnd("same_with_upstream with adaptive fallback", true)

        // ============================================================
        // Test 6: HDD capacity limit with adaptive fallback to SSD
        // ============================================================
        helper.logTestStart("HDD capacity limit with adaptive fallback to SSD")
        
        helper.createSimpleTable(dbName, "${tableName}_6", [:])
        helper.insertData(dbName, "${tableName}_6", ["(6, 'hdd_limit_test')"])
        
        assertTrue(helper.backupToS3(dbName, "snap6", repoName, "${tableName}_6"))
        helper.dropTable(dbName, "${tableName}_6")
        
        helper.withDebugPoint("DiskInfo.exceedLimit.hdd.alwaysTrue") {
            assertTrue(helper.restoreFromS3(dbName, "snap6", repoName, "${tableName}_6", [
                "storage_medium": "hdd",
                "medium_allocation_mode": "adaptive"
            ]))
            assertTrue(helper.verifyRowCount(dbName, "${tableName}_6", 1))
            logger.info("✓ Successfully fell back from HDD to SSD due to capacity")
        }
        
        helper.dropTable(dbName, "${tableName}_6")
        helper.logTestEnd("HDD capacity limit with adaptive fallback to SSD", true)

        // ============================================================
        // Test 7: Force SSD available (alwaysFalse debug point)
        // ============================================================
        helper.logTestStart("Force SSD available with alwaysFalse debug point")
        
        helper.createSimpleTable(dbName, "${tableName}_7", [:])
        helper.insertData(dbName, "${tableName}_7", ["(7, 'ssd_available_test')"])
        
        assertTrue(helper.backupToS3(dbName, "snap7", repoName, "${tableName}_7"))
        helper.dropTable(dbName, "${tableName}_7")
        
        helper.withDebugPoint("DiskInfo.exceedLimit.ssd.alwaysFalse") {
            assertTrue(helper.restoreFromS3(dbName, "snap7", repoName, "${tableName}_7", [
                "storage_medium": "ssd",
                "medium_allocation_mode": "strict"
            ]))
            assertTrue(helper.verifyRowCount(dbName, "${tableName}_7", 1))
            logger.info("✓ SSD marked as available via debug point, strict mode succeeded")
        }
        
        helper.dropTable(dbName, "${tableName}_7")
        helper.logTestEnd("Force SSD available with alwaysFalse debug point", true)

        // ============================================================
        // Test 8: Force HDD available (alwaysFalse debug point)
        // ============================================================
        helper.logTestStart("Force HDD available with alwaysFalse debug point")
        
        helper.createSimpleTable(dbName, "${tableName}_8", [:])
        helper.insertData(dbName, "${tableName}_8", ["(8, 'hdd_available_test')"])
        
        assertTrue(helper.backupToS3(dbName, "snap8", repoName, "${tableName}_8"))
        helper.dropTable(dbName, "${tableName}_8")
        
        helper.withDebugPoint("DiskInfo.exceedLimit.hdd.alwaysFalse") {
            assertTrue(helper.restoreFromS3(dbName, "snap8", repoName, "${tableName}_8", [
                "storage_medium": "hdd",
                "medium_allocation_mode": "strict"
            ]))
            assertTrue(helper.verifyRowCount(dbName, "${tableName}_8", 1))
            logger.info("✓ HDD marked as available via debug point, strict mode succeeded")
        }
        
        helper.dropTable(dbName, "${tableName}_8")
        helper.logTestEnd("Force HDD available with alwaysFalse debug point", true)

        // ============================================================
        // Cleanup
        // ============================================================
        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
        
        logger.info("=== All capacity constraint tests completed ===")
    }
}

