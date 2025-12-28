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

suite("test_backup_restore_medium_disk_types_docker", "docker,backup_restore") {
    // Configure cluster with real HDD and SSD disks
    // BE1: 2 HDD disks
    // BE2: 2 SSD disks  
    // BE3: 1 HDD + 1 SSD disk (mixed)
    def options = new ClusterOptions()
    options.feNum = 1
    options.beNum = 3
    options.beDisks = [
        "HDD=2",      // First BE: 2 HDD disks
        "SSD=2",      // Second BE: 2 SSD disks
        "HDD=1,SSD=1" // Third BE: 1 HDD + 1 SSD (mixed)
    ]
    
    // Enable debug logging for org.apache.doris.backup package
    // Setting both sys_log_verbose_modules (for the package) and sys_log_level (to ensure DEBUG is enabled)
    options.feConfigs += [
        'sys_log_verbose_modules=org.apache.doris.backup',
        'sys_log_level=DEBUG'
    ]

    docker(options) {
        String suiteName = "test_br_disk"
        String repoName = "${suiteName}_repo"
        String dbName = "${suiteName}_db"
        String tableName = "${suiteName}_table"

        def syncer = getSyncer()
        syncer.createS3Repository(repoName)
        sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

        // Verify BE disk configuration
        def backends = sql "SHOW BACKENDS"
        logger.info("=== Backends configuration ===")
        backends.each { be ->
            logger.info("Backend ${be[1]}: ${be}")
        }

        // Test 1: Restore with HDD strict mode - should only use HDD backends
        logger.info("=== Test 1: HDD strict mode - verify HDD backend selection ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_hdd_strict (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_hdd_strict VALUES (1, 'test_hdd')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_hdd TO `${repoName}` ON (${tableName}_hdd_strict)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_hdd")
        assertTrue(snapshot != null)
        
        sql "DROP TABLE ${dbName}.${tableName}_hdd_strict FORCE"
        
        // Restore with HDD + strict mode
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_hdd FROM `${repoName}`
            ON (`${tableName}_hdd_strict`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        def result = sql "SELECT * FROM ${dbName}.${tableName}_hdd_strict"
        assertEquals(1, result.size())
        assertEquals("test_hdd", result[0][1])
        
        // Verify table is on HDD backend
        def tablets = sql "SHOW TABLETS FROM ${dbName}.${tableName}_hdd_strict"
        logger.info("HDD strict tablets: ${tablets}")
        
        sql "DROP TABLE ${dbName}.${tableName}_hdd_strict FORCE"

        // Test 2: Restore with SSD strict mode - should only use SSD backends
        logger.info("=== Test 2: SSD strict mode - verify SSD backend selection ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_ssd_strict (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "SSD"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_ssd_strict VALUES (2, 'test_ssd')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_ssd TO `${repoName}` ON (${tableName}_ssd_strict)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_ssd")
        
        sql "DROP TABLE ${dbName}.${tableName}_ssd_strict FORCE"
        
        // Restore with SSD + strict mode
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_ssd FROM `${repoName}`
            ON (`${tableName}_ssd_strict`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT * FROM ${dbName}.${tableName}_ssd_strict"
        assertEquals(1, result.size())
        assertEquals("test_ssd", result[0][1])
        
        tablets = sql "SHOW TABLETS FROM ${dbName}.${tableName}_ssd_strict"
        logger.info("SSD strict tablets: ${tablets}")
        
        sql "DROP TABLE ${dbName}.${tableName}_ssd_strict FORCE"

        // Test 3: Adaptive mode with SSD preference - may fallback to HDD if needed
        logger.info("=== Test 3: SSD adaptive mode - test fallback behavior ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_ssd_adaptive (
                `id` INT,
                `value` STRING
            )
            PARTITION BY LIST(`id`) (
                PARTITION p1 VALUES IN (1),
                PARTITION p2 VALUES IN (2),
                PARTITION p3 VALUES IN (3),
                PARTITION p4 VALUES IN (4),
                PARTITION p5 VALUES IN (5)
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        for (int i = 1; i <= 5; i++) {
            sql "INSERT INTO ${dbName}.${tableName}_ssd_adaptive VALUES (${i}, 'value_${i}')"
        }
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_adaptive TO `${repoName}` ON (${tableName}_ssd_adaptive)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_adaptive")
        
        sql "DROP TABLE ${dbName}.${tableName}_ssd_adaptive FORCE"
        
        // Restore with SSD + adaptive mode
        // With multiple partitions, some may use SSD, some may fallback to HDD
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_adaptive FROM `${repoName}`
            ON (`${tableName}_ssd_adaptive`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_ssd_adaptive"
        assertEquals(5, result[0][0])
        
        // Check partition distribution
        def partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_ssd_adaptive"
        logger.info("Adaptive mode partitions: ${partitions}")
        assertEquals(5, partitions.size())
        
        sql "DROP TABLE ${dbName}.${tableName}_ssd_adaptive FORCE"

        // Test 4: same_with_upstream mode - inherit original medium
        logger.info("=== Test 4: same_with_upstream mode - inherit from backup ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_upstream (
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
                "storage_medium" = "SSD"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_upstream VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_upstream VALUES ('2024-01-15', 2, 200)"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_upstream TO `${repoName}` ON (${tableName}_upstream)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_upstream")
        
        sql "DROP TABLE ${dbName}.${tableName}_upstream FORCE"
        
        // Restore with same_with_upstream + strict
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_upstream FROM `${repoName}`
            ON (`${tableName}_upstream`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "same_with_upstream",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_upstream"
        assertEquals(2, result[0][0])
        
        def show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_upstream"
        logger.info("same_with_upstream table: ${show_create[0][1]}")
        
        sql "DROP TABLE ${dbName}.${tableName}_upstream FORCE"

        // Test 5: Mixed medium partitions - each partition on appropriate backend
        logger.info("=== Test 5: Mixed HDD/SSD partitions ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_mixed (
                `date` DATE,
                `id` INT,
                `value` INT
            )
            PARTITION BY RANGE(`date`) (
                PARTITION p_hdd1 VALUES LESS THAN ('2024-01-01'),
                PARTITION p_ssd1 VALUES LESS THAN ('2024-02-01'),
                PARTITION p_hdd2 VALUES LESS THAN ('2024-03-01'),
                PARTITION p_ssd2 VALUES LESS THAN ('2024-04-01')
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_mixed VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_mixed VALUES ('2024-01-15', 2, 200)"
        sql "INSERT INTO ${dbName}.${tableName}_mixed VALUES ('2024-02-15', 3, 300)"
        sql "INSERT INTO ${dbName}.${tableName}_mixed VALUES ('2024-03-15', 4, 400)"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_mixed TO `${repoName}` ON (${tableName}_mixed)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_mixed")
        
        sql "DROP TABLE ${dbName}.${tableName}_mixed FORCE"
        
        // Restore with adaptive mode to allow mixing
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_mixed FROM `${repoName}`
            ON (`${tableName}_mixed`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_mixed"
        assertEquals(4, result[0][0])
        
        partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_mixed"
        logger.info("Mixed partitions: ${partitions}")
        assertEquals(4, partitions.size())
        
        // Verify each partition's data
        result = sql "SELECT * FROM ${dbName}.${tableName}_mixed ORDER BY id"
        assertEquals(4, result.size())
        assertEquals(1, result[0][1])
        assertEquals(100, result[0][2])
        assertEquals(4, result[3][1])
        assertEquals(400, result[3][2])
        
        sql "DROP TABLE ${dbName}.${tableName}_mixed FORCE"

        // Test 6: High replication with medium constraint
        logger.info("=== Test 6: Replication across different medium backends ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_replica (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "2"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_replica VALUES (1, 'replica_test')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_replica TO `${repoName}` ON (${tableName}_replica)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_replica")
        
        sql "DROP TABLE ${dbName}.${tableName}_replica FORCE"
        
        // Restore with replication=2 and HDD preference
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_replica FROM `${repoName}`
            ON (`${tableName}_replica`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "replication_num" = "2",
                "storage_medium" = "hdd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT * FROM ${dbName}.${tableName}_replica"
        assertEquals(1, result.size())
        assertEquals("replica_test", result[0][1])
        
        tablets = sql "SHOW TABLETS FROM ${dbName}.${tableName}_replica"
        logger.info("Replica tablets: ${tablets}")
        // Should have 2 replicas
        assertTrue(tablets.size() >= 2, "Should have at least 2 replicas")
        
        sql "DROP TABLE ${dbName}.${tableName}_replica FORCE"

        // Test 7: Atomic restore with medium preference
        logger.info("=== Test 7: Atomic restore with medium settings ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_atomic (
                `id` INT,
                `value` STRING
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "storage_medium" = "HDD"
            )
        """
        
        sql "INSERT INTO ${dbName}.${tableName}_atomic VALUES (1, 'version1')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_atomic_v1 TO `${repoName}` ON (${tableName}_atomic)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot_v1 = syncer.getSnapshotTimestamp(repoName, "snap_atomic_v1")
        
        // Modify data
        sql "INSERT INTO ${dbName}.${tableName}_atomic VALUES (2, 'version2')"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_atomic_v2 TO `${repoName}` ON (${tableName}_atomic)"
        syncer.waitSnapshotFinish(dbName)
        def snapshot_v2 = syncer.getSnapshotTimestamp(repoName, "snap_atomic_v2")
        
        // Atomic restore to v1 with SSD + adaptive
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_atomic_v1 FROM `${repoName}`
            ON (`${tableName}_atomic`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot_v1}",
                "atomic_restore" = "true",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "adaptive"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        result = sql "SELECT * FROM ${dbName}.${tableName}_atomic ORDER BY id"
        assertEquals(1, result.size())
        assertEquals("version1", result[0][1])
        
        show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_atomic"
        logger.info("Atomic restored table: ${show_create[0][1]}")
        
        sql "DROP TABLE ${dbName}.${tableName}_atomic FORCE"

        // Test 8: Restore specific partitions with different mediums
        logger.info("=== Test 8: Restore specific partitions only ===")
        
        sql """
            CREATE TABLE ${dbName}.${tableName}_part_select (
                `date` DATE,
                `id` INT,
                `value` INT
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
        
        sql "INSERT INTO ${dbName}.${tableName}_part_select VALUES ('2023-12-15', 1, 100)"
        sql "INSERT INTO ${dbName}.${tableName}_part_select VALUES ('2024-01-15', 2, 200)"
        sql "INSERT INTO ${dbName}.${tableName}_part_select VALUES ('2024-02-15', 3, 300)"
        sql "INSERT INTO ${dbName}.${tableName}_part_select VALUES ('2024-03-15', 4, 400)"
        
        sql "BACKUP SNAPSHOT ${dbName}.snap_part_sel TO `${repoName}` ON (${tableName}_part_select)"
        syncer.waitSnapshotFinish(dbName)
        snapshot = syncer.getSnapshotTimestamp(repoName, "snap_part_sel")
        
        sql "DROP TABLE ${dbName}.${tableName}_part_select FORCE"
        
        // Restore only p1 and p3 with SSD + strict
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_part_sel FROM `${repoName}`
            ON (`${tableName}_part_select` PARTITION (p1, p3))
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "strict"
            )
        """
        
        syncer.waitAllRestoreFinish(dbName)
        
        partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_part_select"
        logger.info("Selected partitions restored: ${partitions}")
        assertEquals(2, partitions.size(), "Should only have 2 partitions")
        
        result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_part_select"
        assertEquals(2, result[0][0], "Should only have data from 2 partitions")
        
        result = sql "SELECT * FROM ${dbName}.${tableName}_part_select ORDER BY id"
        assertEquals(1, result[0][1])
        assertEquals(3, result[1][1])
        
        sql "DROP TABLE ${dbName}.${tableName}_part_select FORCE"

        sql "DROP DATABASE ${dbName} FORCE"
        sql "DROP REPOSITORY `${repoName}`"
    }
}

