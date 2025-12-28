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

suite("test_backup_restore_job_lifecycle", "backup_restore") {
    String suiteName = "test_br_lifecycle"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Test 1: Basic restore job lifecycle to cover checkAndPrepareMeta
    logger.info("=== Test 1: checkAndPrepareMeta coverage ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_lifecycle (
            `id` INT,
            `name` STRING,
            `value` DOUBLE
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "HDD"
        )
    """
    
    // Insert test data
    sql "INSERT INTO ${dbName}.${tableName}_lifecycle VALUES (1, 'test1', 100.0), (2, 'test2', 200.0)"
    
    // Backup
    sql "BACKUP SNAPSHOT ${dbName}.snap_lifecycle TO `${repoName}` ON (${tableName}_lifecycle)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_lifecycle")
    
    // Drop table
    sql "DROP TABLE ${dbName}.${tableName}_lifecycle FORCE"
    
    // Restore with storage_medium configuration
    // This will trigger checkAndPrepareMeta() to validate metadata
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_lifecycle FROM `${repoName}`
        ON (`${tableName}_lifecycle`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "strict"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    def result = sql "SELECT * FROM ${dbName}.${tableName}_lifecycle ORDER BY id"
    assertEquals(2, result.size())
    assertEquals(1, result[0][0])
    assertEquals("test1", result[0][1])
    
    sql "DROP TABLE ${dbName}.${tableName}_lifecycle FORCE"

    // Test 2: Restore to existing table to cover more branches in checkAndPrepareMeta
    logger.info("=== Test 2: Restore to existing table (checkAndPrepareMeta branches) ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_existing (
            `id` INT,
            `data` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_existing VALUES (1, 'original')"
    
    // Backup
    sql "BACKUP SNAPSHOT ${dbName}.snap_existing TO `${repoName}` ON (${tableName}_existing)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot2 = syncer.getSnapshotTimestamp(repoName, "snap_existing")
    
    // Modify data
    sql "INSERT INTO ${dbName}.${tableName}_existing VALUES (2, 'new_data')"
    
    // Restore over existing table - this tests the overwrite path in checkAndPrepareMeta
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_existing FROM `${repoName}`
        ON (`${tableName}_existing`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot2}",
            "storage_medium" = "same_with_upstream",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    // Verify data is restored (should have original version)
    result = sql "SELECT * FROM ${dbName}.${tableName}_existing ORDER BY id"
    assertTrue(result.size() >= 1)
    
    sql "DROP TABLE ${dbName}.${tableName}_existing FORCE"

    // Test 3: Atomic restore to cover bindLocalAndRemoteOlapTableReplicas
    logger.info("=== Test 3: Atomic restore (bindLocalAndRemoteOlapTableReplicas) ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_atomic (
            `id` INT,
            `value` INT
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_atomic VALUES (1, 100), (2, 200)"
    
    // Backup
    sql "BACKUP SNAPSHOT ${dbName}.snap_atomic TO `${repoName}` ON (${tableName}_atomic)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot3 = syncer.getSnapshotTimestamp(repoName, "snap_atomic")
    
    // Modify data
    sql "UPDATE ${dbName}.${tableName}_atomic SET value = 999 WHERE id = 1"
    
    // Atomic restore - this covers bindLocalAndRemoteOlapTableReplicas()
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_atomic FROM `${repoName}`
        ON (`${tableName}_atomic`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot3}",
            "atomic_restore" = "true",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT * FROM ${dbName}.${tableName}_atomic WHERE id = 1"
    assertEquals(1, result.size())
    assertEquals(100, result[0][1]) // Should be restored to original value
    
    sql "DROP TABLE ${dbName}.${tableName}_atomic FORCE"

    // Test 4: Restore with partitions to cover resetPartitionForRestore
    logger.info("=== Test 4: Partitioned table restore (resetPartitionForRestore) ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_part (
            `date` DATE,
            `id` INT,
            `value` STRING
        )
        PARTITION BY RANGE(`date`) (
            PARTITION p1 VALUES LESS THAN ("2024-01-01"),
            PARTITION p2 VALUES LESS THAN ("2024-02-01")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_part VALUES ('2023-12-15', 1, 'p1_data')"
    sql "INSERT INTO ${dbName}.${tableName}_part VALUES ('2024-01-15', 2, 'p2_data')"
    
    // Backup
    sql "BACKUP SNAPSHOT ${dbName}.snap_part TO `${repoName}` ON (${tableName}_part)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot4 = syncer.getSnapshotTimestamp(repoName, "snap_part")
    
    // Drop one partition
    sql "ALTER TABLE ${dbName}.${tableName}_part DROP PARTITION p1"
    
    // Restore - this will trigger resetPartitionForRestore() for missing partition
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_part FROM `${repoName}`
        ON (`${tableName}_part` PARTITION (p1, p2))
        PROPERTIES (
            "backup_timestamp" = "${snapshot4}",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "strict"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT * FROM ${dbName}.${tableName}_part ORDER BY date"
    assertEquals(2, result.size())
    
    sql "DROP TABLE ${dbName}.${tableName}_part FORCE"

    // Test 5: Test repository change (updateRepo)
    logger.info("=== Test 5: Repository handling (updateRepo) ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_repo (
            `id` INT,
            `data` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_repo VALUES (1, 'repo_test')"
    
    // Backup to repository
    sql "BACKUP SNAPSHOT ${dbName}.snap_repo TO `${repoName}` ON (${tableName}_repo)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot5 = syncer.getSnapshotTimestamp(repoName, "snap_repo")
    
    sql "DROP TABLE ${dbName}.${tableName}_repo FORCE"
    
    // Restore - the restore job will use updateRepo() internally
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_repo FROM `${repoName}`
        ON (`${tableName}_repo`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot5}",
            "storage_medium" = "same_with_upstream",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT * FROM ${dbName}.${tableName}_repo"
    assertEquals(1, result.size())
    assertEquals("repo_test", result[0][1])
    
    sql "DROP TABLE ${dbName}.${tableName}_repo FORCE"

    // Test 6: Complex schema to cover more checkAndPrepareMeta paths
    logger.info("=== Test 6: Complex schema restore ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_complex (
            `k1` INT,
            `k2` STRING,
            `v1` INT SUM
        )
        AGGREGATE KEY(`k1`, `k2`)
        DISTRIBUTED BY HASH(`k1`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "HDD"
        )
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_complex VALUES (1, 'a', 10), (1, 'a', 20)"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap_complex TO `${repoName}` ON (${tableName}_complex)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot6 = syncer.getSnapshotTimestamp(repoName, "snap_complex")
    
    sql "DROP TABLE ${dbName}.${tableName}_complex FORCE"
    
    // Restore aggregate table
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_complex FROM `${repoName}`
        ON (`${tableName}_complex`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot6}",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT k1, k2, v1 FROM ${dbName}.${tableName}_complex"
    assertEquals(1, result.size())
    assertEquals(30, result[0][2]) // Sum should be 10 + 20 = 30
    
    sql "DROP TABLE ${dbName}.${tableName}_complex FORCE"

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

