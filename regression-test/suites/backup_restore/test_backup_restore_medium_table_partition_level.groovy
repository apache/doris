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

suite("test_backup_restore_medium_table_partition_level", "backup_restore") {
    String suiteName = "test_br_tbl_part"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Test 1: Table-level medium setting affects all partitions
    logger.info("=== Test 1: Table-level medium affects all partitions ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_tbl_level (
            `date` DATE,
            `id` INT,
            `value` INT
        )
        DUPLICATE KEY(`date`, `id`)
        PARTITION BY RANGE(`date`) (
            PARTITION p1 VALUES LESS THAN ('2024-01-01'),
            PARTITION p2 VALUES LESS THAN ('2024-02-01'),
            PARTITION p3 VALUES LESS THAN ('2024-03-01')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "HDD"
        )
    """
    
    // Insert data into different partitions
    sql "INSERT INTO ${dbName}.${tableName}_tbl_level VALUES ('2023-12-15', 1, 100)"
    sql "INSERT INTO ${dbName}.${tableName}_tbl_level VALUES ('2024-01-15', 2, 200)"
    sql "INSERT INTO ${dbName}.${tableName}_tbl_level VALUES ('2024-02-15', 3, 300)"
    
    // Backup
    sql "BACKUP SNAPSHOT ${dbName}.snap_tbl TO `${repoName}` ON (${tableName}_tbl_level)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_tbl")
    assertTrue(snapshot != null, "Snapshot should be created")

    // Restore with table-level medium setting (SSD + strict)
    sql "DROP TABLE ${dbName}.${tableName}_tbl_level FORCE"
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_tbl FROM `${repoName}`
        ON (`${tableName}_tbl_level`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "strict"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    
    // Verify table-level properties
    def show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_tbl_level"
    logger.info("Table after restore: ${show_create[0][1]}")
    assertTrue(show_create[0][1].contains("medium_allocation_mode"),
              "Table should have medium_allocation_mode")
    
    // Verify all partitions exist and have data
    def partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_tbl_level"
    logger.info("Partitions: ${partitions}")
    assertEquals(3, partitions.size(), "Should have 3 partitions")
    
    // Verify data
    def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_tbl_level"
    assertEquals(3, result[0][0], "All 3 rows should be restored")
    
    result = sql "SELECT * FROM ${dbName}.${tableName}_tbl_level ORDER BY id"
    assertEquals(1, result[0][1], "First row should have id=1")
    assertEquals(100, result[0][2], "First row should have value=100")
    assertEquals(2, result[1][1], "Second row should have id=2")
    assertEquals(300, result[2][2], "Third row should have value=300")

    sql "DROP TABLE ${dbName}.${tableName}_tbl_level FORCE"

    // Test 2: Restore specific partitions with medium settings
    logger.info("=== Test 2: Restore specific partitions ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_part_spec (
            `date` DATE,
            `id` INT,
            `value` INT
        )
        DUPLICATE KEY(`date`, `id`)
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
    
    sql "INSERT INTO ${dbName}.${tableName}_part_spec VALUES ('2023-12-15', 10, 1000)"
    sql "INSERT INTO ${dbName}.${tableName}_part_spec VALUES ('2024-01-15', 20, 2000)"
    sql "INSERT INTO ${dbName}.${tableName}_part_spec VALUES ('2024-02-15', 30, 3000)"
    
    // Backup
    sql "BACKUP SNAPSHOT ${dbName}.snap_part TO `${repoName}` ON (${tableName}_part_spec)"
    syncer.waitSnapshotFinish(dbName)
    snapshot = syncer.getSnapshotTimestamp(repoName, "snap_part")
    
    // Restore only partition p1 and p2 with HDD + adaptive
    sql "DROP TABLE ${dbName}.${tableName}_part_spec FORCE"
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_part FROM `${repoName}`
        ON (`${tableName}_part_spec` PARTITION (p1, p2))
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    
    // Verify only 2 partitions restored
    partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_part_spec"
    logger.info("Restored partitions: ${partitions}")
    assertEquals(2, partitions.size(), "Should have 2 partitions (p1, p2)")
    
    // Verify data (should have 2 rows from p1 and p2)
    result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_part_spec"
    assertEquals(2, result[0][0], "Should have 2 rows from p1 and p2")
    
    result = sql "SELECT * FROM ${dbName}.${tableName}_part_spec ORDER BY id"
    assertEquals(10, result[0][1], "Should have row with id=10")
    assertEquals(20, result[1][1], "Should have row with id=20")

    sql "DROP TABLE ${dbName}.${tableName}_part_spec FORCE"

    // Test 3: Restore with same_with_upstream for partitioned table
    logger.info("=== Test 3: Restore partitioned table with same_with_upstream ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_upstream (
            `date` DATE,
            `id` INT,
            `value` INT
        )
        DUPLICATE KEY(`date`, `id`)
        PARTITION BY RANGE(`date`) (
            PARTITION p1 VALUES LESS THAN ('2024-01-01'),
            PARTITION p2 VALUES LESS THAN ('2024-02-01')
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "HDD"
        )
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_upstream VALUES ('2023-12-15', 100, 10000)"
    sql "INSERT INTO ${dbName}.${tableName}_upstream VALUES ('2024-01-15', 200, 20000)"
    
    // Backup
    sql "BACKUP SNAPSHOT ${dbName}.snap_upstream TO `${repoName}` ON (${tableName}_upstream)"
    syncer.waitSnapshotFinish(dbName)
    snapshot = syncer.getSnapshotTimestamp(repoName, "snap_upstream")
    
    // Restore with same_with_upstream (should inherit HDD from upstream)
    sql "DROP TABLE ${dbName}.${tableName}_upstream FORCE"
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_upstream FROM `${repoName}`
        ON (`${tableName}_upstream`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "same_with_upstream",
            "medium_allocation_mode" = "adaptive"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    
    // Verify table restored
    show_create = sql "SHOW CREATE TABLE ${dbName}.${tableName}_upstream"
    logger.info("Table with same_with_upstream: ${show_create[0][1]}")
    // Should inherit HDD from upstream
    assertTrue(show_create[0][1].toLowerCase().contains("hdd") || 
               !show_create[0][1].toLowerCase().contains("ssd"),
              "Should inherit HDD from upstream")
    
    // Verify data
    result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_upstream"
    assertEquals(2, result[0][0], "Should have 2 rows")

    sql "DROP TABLE ${dbName}.${tableName}_upstream FORCE"
    
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

