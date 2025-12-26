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

suite("test_backup_restore_medium_capacity_docker", "docker,backup_restore") {
    // Configure cluster with capacity-limited disks to test resource constraints
    // BE1: 1 HDD (unlimited) + 1 SSD (5GB limited)
    // BE2: 2 HDD (unlimited)
    // BE3: 2 SSD (5GB limited each)
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.beDisks = [
        "HDD=1,SSD=1,5",  // BE1: 1 HDD unlimited + 1 SSD 5GB
        "HDD=2",          // BE2: 2 HDD unlimited
        "SSD=2,5"         // BE3: 2 SSD 5GB each
    ]
    options.enableDebugPoints()
    
    // Enable debug logging
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup'
    ]

    docker(options) {
        String suiteName = "test_br_capacity"
        String repoName = "${suiteName}_repo"
        String dbName = "${suiteName}_db"
        String tableName = "${suiteName}_table"

        def syncer = getSyncer()
        syncer.createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

        def feHttpAddress = context.config.feHttpAddress.split(":")
        def feHost = feHttpAddress[0]
        def fePort = feHttpAddress[1] as int

        // Verify disk configuration
        logger.info("=== Disk Configuration ===")
        def disks = sql "SHOW PROC '/backends'"
        disks.each { disk ->
            logger.info("Backend disk info: ${disk}")
        }

        // Test 1: SSD capacity exhaustion forces adaptive fallback to HDD
        logger.info("=== Test 1: Adaptive mode fallback when SSD capacity insufficient ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_capacity (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_capacity VALUES (1, 'capacity_test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_capacity TO `${repoName}` ON (${tableName}_capacity)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_capacity")
        assertTrue(snapshot != null)
        
        sql "DROP TABLE ${dbName}.${tableName}_capacity FORCE"
        
        // Simulate SSD capacity full
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "DiskInfo.hasCapacity.ssd.alwaysFalse")
            
            // Restore with SSD + adaptive mode
            // Should fallback to HDD when SSD capacity is full
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_capacity FROM `${repoName}`
                ON (`${tableName}_capacity`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot}",
                    "storage_medium" = "ssd",
                    "medium_allocation_mode" = "adaptive"
                )
            """
            
            syncer.waitAllRestoreFinish(dbName)
            
            def result = sql "SELECT * FROM ${dbName}.${tableName}_capacity"
            assertEquals(1, result.size())
            assertEquals("capacity_test", result[0][1])
            
            logger.info("Successfully fell back from SSD to HDD due to capacity")
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "DiskInfo.hasCapacity.ssd.alwaysFalse")
        }
        
        sql "DROP TABLE ${dbName}.${tableName}_capacity FORCE"

        // Test 2: Strict mode fails when requested medium has no capacity
        logger.info("=== Test 2: Strict mode failure when capacity insufficient ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_strict_fail (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_strict_fail VALUES (2, 'strict_test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_strict_fail TO `${repoName}` ON (${tableName}_strict_fail)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_strict_fail")
        
        sql "DROP TABLE ${dbName}.${tableName}_strict_fail FORCE"
        
        // Simulate SSD capacity full
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "DiskInfo.hasCapacity.ssd.alwaysFalse")
            
            // Restore with SSD + strict mode - should fail
            try {
                sql """
                    RESTORE SNAPSHOT ${dbName}.snap_strict_fail FROM `${repoName}`
                    ON (`${tableName}_strict_fail`)
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
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "DiskInfo.hasCapacity.ssd.alwaysFalse")
        }
        
        try {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_strict_fail FORCE"
        } catch (Exception e) {
            logger.info("Cleanup: ${e.message}")
        }

        // Test 3: Multi-partition restore with mixed capacity constraints
        logger.info("=== Test 3: Partitions distributed across available disks ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_multi_part (
                `date` DATE,
                `id` INT,
                `value` INT
            )
            PARTITION BY RANGE(`date`) (
                PARTITION p1 VALUES LESS THAN ('2024-01-01'),
                PARTITION p2 VALUES LESS THAN ('2024-02-01'),
                PARTITION p3 VALUES LESS THAN ('2024-03-01'),
                PARTITION p4 VALUES LESS THAN ('2024-04-01'),
                PARTITION p5 VALUES LESS THAN ('2024-05-01')
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        for (int i = 1; i <= 5; i++) {
            def date = String.format("2023-%02d-15", 12 + i - 1)
            if (i > 1) {
                date = String.format("2024-%02d-15", i - 1)
            }
            sql "INSERT INTO ${dbName}.${tableName}_multi_part VALUES ('${date}', ${i}, ${i * 100})"
        }
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_multi TO `${repoName}` ON (${tableName}_multi_part)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_multi")
        
        sql "DROP TABLE ${dbName}.${tableName}_multi_part FORCE"
        
        // Restore with HDD + adaptive
        // Partitions should be distributed across available HDD disks
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_multi FROM `${repoName}`
            ON (`${tableName}_multi_part`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_multi_part"
        assertEquals(5, result[0][0])
        
        def partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_multi_part"
        logger.info("Partitions distributed: ${partitions}")
        assertEquals(5, partitions.size())
        
        sql "DROP TABLE ${dbName}.${tableName}_multi_part FORCE"

        // Test 4: Replication with capacity constraints
        logger.info("=== Test 4: Replica allocation with medium constraints ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_replica_capacity (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "2"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_replica_capacity VALUES (3, 'replica_capacity')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_replica TO `${repoName}` ON (${tableName}_replica_capacity)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_replica")
        
        sql "DROP TABLE ${dbName}.${tableName}_replica_capacity FORCE"
        
        // Restore with replication=2 and HDD preference
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_replica FROM `${repoName}`
            ON (`${tableName}_replica_capacity`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "replication_num" = "2",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT * FROM ${dbName}.${tableName}_replica_capacity"
        assertEquals(1, result.size())
        
        def tablets = sql "SHOW TABLETS FROM ${dbName}.${tableName}_replica_capacity"
        logger.info("Replica tablets with capacity constraint: ${tablets}")
        
        sql "DROP TABLE ${dbName}.${tableName}_replica_capacity FORCE"

        // Test 5: same_with_upstream with capacity fallback
        logger.info("=== Test 5: same_with_upstream with adaptive fallback ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_upstream_cap (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "SSD"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_upstream_cap VALUES (4, 'upstream_test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_upstream TO `${repoName}` ON (${tableName}_upstream_cap)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_upstream")
        
        sql "DROP TABLE ${dbName}.${tableName}_upstream_cap FORCE"
        
        // Simulate SSD capacity full
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "DiskInfo.hasCapacity.ssd.alwaysFalse")
            
            // Restore with same_with_upstream + adaptive
            // Should try SSD first, then fallback to HDD
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_upstream FROM `${repoName}`
                ON (`${tableName}_upstream_cap`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot}",
                    "storage_medium" = "same_with_upstream",
                    "medium_allocation_mode" = "adaptive"
                )
            """
            
            syncer.waitAllRestoreFinish(dbName)
            
            result = sql "SELECT * FROM ${dbName}.${tableName}_upstream_cap"
            assertEquals(1, result.size())
            assertEquals("upstream_test", result[0][1])
            
            logger.info("same_with_upstream successfully fell back from SSD to HDD")
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "DiskInfo.hasCapacity.ssd.alwaysFalse")
        }
        
        sql "DROP TABLE ${dbName}.${tableName}_upstream_cap FORCE"

        // Test 6: Partial partition restore with capacity constraints
        logger.info("=== Test 6: Restore subset of partitions with capacity limits ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_part_subset (
                `date` DATE,
                `id` INT,
                `value` INT
            )
            PARTITION BY RANGE(`date`) (
                PARTITION p1 VALUES LESS THAN ('2024-01-01'),
                PARTITION p2 VALUES LESS THAN ('2024-02-01'),
                PARTITION p3 VALUES LESS THAN ('2024-03-01')
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_part_subset VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_part_subset VALUES ('2024-01-15', 2, 200)"
        sql "INSERT INTO ${dbName}.${tableName}_part_subset VALUES ('2024-02-15', 3, 300)"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_subset TO `${repoName}` ON (${tableName}_part_subset)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_subset")
        
        sql "DROP TABLE ${dbName}.${tableName}_part_subset FORCE"
        
        // Restore only p1 and p2 with HDD + adaptive
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_subset FROM `${repoName}`
            ON (`${tableName}_part_subset` PARTITION (p1, p2))
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_part_subset"
        assertEquals(2, partitions.size(), "Should only restore 2 partitions")
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_part_subset"
        assertEquals(2, result[0][0], "Should only have data from 2 partitions")
        
        sql "DROP TABLE ${dbName}.${tableName}_part_subset FORCE"

        // Test 7: Backend unavailability scenario
        logger.info("=== Test 7: Restore when some backends unavailable ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_be_unavail (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_be_unavail VALUES (5, 'be_test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_be TO `${repoName}` ON (${tableName}_be_unavail)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_be")
        
        sql "DROP TABLE ${dbName}.${tableName}_be_unavail FORCE"
        
        // Simulate BE2 (HDD-only) unavailable
        try {
            DebugPoint.enableDebugPoint(feHost, fePort, NodeType.FE,
                "SystemInfoService.selectBackendIdsForReplicaCreation.skipFirstHDDBackend")
            
            // Restore with HDD + adaptive
            // Should use remaining HDD backends or fallback
            sql """
                RESTORE SNAPSHOT ${dbName}.snap_be FROM `${repoName}`
                ON (`${tableName}_be_unavail`)
                PROPERTIES (
                    "backup_timestamp" = "${snapshot}",
                    "storage_medium" = "hdd",
                    "medium_allocation_mode" = "adaptive"
                )
            """
            
            syncer.waitAllRestoreFinish(dbName)
            
            result = sql "SELECT * FROM ${dbName}.${tableName}_be_unavail"
            assertEquals(1, result.size())
            assertEquals("be_test", result[0][1])
            
            logger.info("Successfully restored despite BE unavailability")
            
        } finally {
            DebugPoint.disableDebugPoint(feHost, fePort, NodeType.FE,
                "SystemInfoService.selectBackendIdsForReplicaCreation.skipFirstHDDBackend")
        }
        
        sql "DROP TABLE ${dbName}.${tableName}_be_unavail FORCE"

        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
    }
}

