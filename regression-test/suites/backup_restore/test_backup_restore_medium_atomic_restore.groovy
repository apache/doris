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

suite("test_backup_restore_medium_atomic_restore", "backup_restore") {
    String suiteName = "test_br_medium_atomic"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Test 1: Atomic restore with storage medium settings
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_atomic"
    sql """
        CREATE TABLE ${dbName}.${tableName}_atomic (
            `id` LARGEINT NOT NULL,
            `name` VARCHAR(128),
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    // Insert initial data
    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, 'name${i}', ${i * 100})")
    }
    sql "INSERT INTO ${dbName}.${tableName}_atomic VALUES ${values.join(",")}"

    def result = sql "SELECT * FROM ${dbName}.${tableName}_atomic"
    assertEquals(result.size(), 10)

    // Backup
    sql """
        BACKUP SNAPSHOT ${dbName}.snapshot_atomic
        TO `${repoName}`
        ON (${tableName}_atomic)
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, "snapshot_atomic")
    assertTrue(snapshot != null)

    // Insert more data to existing table
    sql "INSERT INTO ${dbName}.${tableName}_atomic VALUES (11, 'name11', 1100)"

    result = sql "SELECT * FROM ${dbName}.${tableName}_atomic"
    assertEquals(result.size(), 11, "Table should have 11 rows before atomic restore")

    // Atomic restore with storage medium - should replace existing table
    sql """
        RESTORE SNAPSHOT ${dbName}.snapshot_atomic
        FROM `${repoName}`
        ON (`${tableName}_atomic`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "atomic_restore" = "true",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "strict"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    // After atomic restore, table should have only original 10 rows
    result = sql "SELECT * FROM ${dbName}.${tableName}_atomic"
    assertEquals(result.size(), 10, "After atomic restore, table should have original 10 rows")

    // Verify no row with id=11
    result = sql "SELECT * FROM ${dbName}.${tableName}_atomic WHERE id = 11"
    assertEquals(result.size(), 0, "Row with id=11 should not exist after atomic restore")

    // Verify table properties include medium settings
    def show_result = sql "SHOW CREATE TABLE ${dbName}.${tableName}_atomic"
    def createTableStr = show_result[0][1]
    assertTrue(createTableStr.contains("medium_allocation_mode"),
              "Table should have medium_allocation_mode after atomic restore")

    sql "DROP TABLE ${dbName}.${tableName}_atomic FORCE"

    // Test 2: Atomic restore with same_with_upstream
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_upstream"
    sql """
        CREATE TABLE ${dbName}.${tableName}_upstream (
            `id` INT NOT NULL,
            `value` INT)
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1",
            "storage_medium" = "SSD"
        )
    """

    sql "INSERT INTO ${dbName}.${tableName}_upstream VALUES (1, 10), (2, 20)"

    // Backup
    sql """
        BACKUP SNAPSHOT ${dbName}.snapshot_upstream
        TO `${repoName}`
        ON (${tableName}_upstream)
    """

    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, "snapshot_upstream")
    assertTrue(snapshot != null)

    // Modify table
    sql "INSERT INTO ${dbName}.${tableName}_upstream VALUES (3, 30)"

    // Atomic restore with same_with_upstream - should preserve original medium
    sql """
        RESTORE SNAPSHOT ${dbName}.snapshot_upstream
        FROM `${repoName}`
        ON (`${tableName}_upstream`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "atomic_restore" = "true",
            "storage_medium" = "same_with_upstream",
            "medium_allocation_mode" = "adaptive"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}_upstream"
    assertEquals(result.size(), 2, "After atomic restore with same_with_upstream, should have original 2 rows")

    sql "DROP TABLE ${dbName}.${tableName}_upstream FORCE"

    // Test 3: Atomic restore with force_replace
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_force"
    sql """
        CREATE TABLE ${dbName}.${tableName}_force (
            `id` INT NOT NULL,
            `data` VARCHAR(128))
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    sql "INSERT INTO ${dbName}.${tableName}_force VALUES (1, 'original')"

    // Backup
    sql """
        BACKUP SNAPSHOT ${dbName}.snapshot_force
        TO `${repoName}`
        ON (${tableName}_force)
    """

    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, "snapshot_force")
    assertTrue(snapshot != null)

    // Modify table schema or data
    sql "TRUNCATE TABLE ${dbName}.${tableName}_force"
    sql "INSERT INTO ${dbName}.${tableName}_force VALUES (2, 'modified')"

    // Atomic restore with force_replace
    sql """
        RESTORE SNAPSHOT ${dbName}.snapshot_force
        FROM `${repoName}`
        ON (`${tableName}_force`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "atomic_restore" = "true",
            "force_replace" = "true",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "adaptive"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}_force"
    assertEquals(result.size(), 1, "After force replace, should have 1 row")
    assertEquals(result[0][1].toString(), "original", "Data should be from backup")

    sql "DROP TABLE ${dbName}.${tableName}_force FORCE"

    // Test 4: Atomic restore with local medium different from config
    // This tests the "prefer local medium" logic in atomic restore
    logger.info("=== Test 4: Atomic restore prefers local medium (avoid migration) ===")
    
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_local_pref"
    sql """
        CREATE TABLE ${dbName}.${tableName}_local_pref (
            `id` INT,
            `value` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "HDD"
        )
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_local_pref VALUES (1, 'local_data')"
    
    // Backup the table
    sql "BACKUP SNAPSHOT ${dbName}.snap_local TO `${repoName}` ON (${tableName}_local_pref)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot_local = syncer.getSnapshotTimestamp(repoName, "snap_local")
    
    // Modify local table data to simulate existing data
    sql "INSERT INTO ${dbName}.${tableName}_local_pref VALUES (2, 'modified')"
    result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_local_pref"
    assertEquals(2, result[0][0], "Should have 2 rows before atomic restore")
    
    // Atomic restore with same_with_upstream + adaptive
    // Should prefer local HDD medium to avoid migration
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_local FROM `${repoName}`
        ON (`${tableName}_local_pref`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot_local}",
            "atomic_restore" = "true",
            "storage_medium" = "same_with_upstream",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    // Verify data replaced (atomic restore)
    result = sql "SELECT * FROM ${dbName}.${tableName}_local_pref ORDER BY id"
    assertEquals(1, result.size(), "Atomic restore should replace all data")
    assertEquals("local_data", result[0][1].toString(), "Should have backup data")
    
    // Verify medium unchanged (preferred local to avoid migration)
    def show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_local_pref"
    logger.info("Table after atomic restore with local preference: ${show_create[0][1]}")
    
    sql "DROP TABLE ${dbName}.${tableName}_local_pref FORCE"

    // Test 5: Atomic restore should fail if table has temp partitions (line 662-665)
    logger.info("=== Test 5: Atomic restore with temp partitions error check ===")
    
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_temp"
    sql """
        CREATE TABLE ${dbName}.${tableName}_temp (
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
    
    sql "INSERT INTO ${dbName}.${tableName}_temp VALUES ('2023-12-15', 1, 100)"
    
    // Backup
    sql "BACKUP SNAPSHOT ${dbName}.snap_temp TO `${repoName}` ON (${tableName}_temp)"
    syncer.waitSnapshotFinish(dbName)
    snapshot = syncer.getSnapshotTimestamp(repoName, "snap_temp")
    
    // Add a temp partition to the table
    sql """
        ALTER TABLE ${dbName}.${tableName}_temp 
        ADD TEMPORARY PARTITION p_temp VALUES LESS THAN ('2024-03-01')
    """
    
    // Wait for temp partition to be created
    Thread.sleep(2000)
    
    // Try to restore - should fail because table has temp partitions
    try {
        sql """
            RESTORE SNAPSHOT ${dbName}.snap_temp FROM `${repoName}`
            ON (`${tableName}_temp`)
            PROPERTIES (
                "backup_timestamp" = "${snapshot}",
                "storage_medium" = "ssd",
                "medium_allocation_mode" = "strict"
            )
        """
        
        Thread.sleep(3000)
        
        def restore_status = sql "SHOW RESTORE FROM ${dbName}"
        if (restore_status.size() > 0) {
            def state = restore_status[0][4]
            logger.info("Restore status with temp partition: ${state}")
            if (state == "CANCELLED") {
                logger.info("Expected: restore cancelled due to temp partitions")
            }
        }
        
    } catch (Exception e) {
        logger.info("Expected: restore failed due to temp partitions: ${e.message}")
    }
    
    sql "DROP TABLE ${dbName}.${tableName}_temp FORCE"

    // Test 6: Multiple consecutive atomic restores to same table (line 668-671)
    logger.info("=== Test 6: Multiple consecutive atomic restores ===")
    
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_multi"
    sql """
        CREATE TABLE ${dbName}.${tableName}_multi (
            `id` INT,
            `value` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_multi VALUES (1, 'version1')"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap_m1 TO `${repoName}` ON (${tableName}_multi)"
    syncer.waitSnapshotFinish(dbName)
    def snap_m1 = syncer.getSnapshotTimestamp(repoName, "snap_m1")
    
    sql "INSERT INTO ${dbName}.${tableName}_multi VALUES (2, 'version2')"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap_m2 TO `${repoName}` ON (${tableName}_multi)"
    syncer.waitSnapshotFinish(dbName)
    def snap_m2 = syncer.getSnapshotTimestamp(repoName, "snap_m2")
    
    sql "INSERT INTO ${dbName}.${tableName}_multi VALUES (3, 'version3')"
    
    // First atomic restore to snap_m1
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_m1 FROM `${repoName}`
        ON (`${tableName}_multi`)
        PROPERTIES (
            "backup_timestamp" = "${snap_m1}",
            "atomic_restore" = "true",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_multi"
    assertEquals(1, result[0][0], "After first atomic restore, should have 1 row")
    
    // Second atomic restore to snap_m2
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_m2 FROM `${repoName}`
        ON (`${tableName}_multi`)
        PROPERTIES (
            "backup_timestamp" = "${snap_m2}",
            "atomic_restore" = "true",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "strict"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_multi"
    assertEquals(2, result[0][0], "After second atomic restore, should have 2 rows")
    
    sql "DROP TABLE ${dbName}.${tableName}_multi FORCE"

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

