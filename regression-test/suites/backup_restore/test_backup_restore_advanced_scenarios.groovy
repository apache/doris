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

suite("test_backup_restore_advanced_scenarios", "backup_restore") {
    String suiteName = "test_br_advanced"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Test 1: waitingAllReplicasCreated - restore with multiple tablets
    logger.info("=== Test 1: Multiple tablets restore (waitingAllReplicasCreated) ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_multi (
            `id` INT,
            `name` STRING,
            `value` INT
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 5
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    
    // Insert data to create multiple tablets
    for (int i = 0; i < 50; i++) {
        sql "INSERT INTO ${dbName}.${tableName}_multi VALUES (${i}, 'name_${i}', ${i * 100})"
    }
    
    sql "BACKUP SNAPSHOT ${dbName}.snap_multi TO `${repoName}` ON (${tableName}_multi)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot = syncer.getSnapshotTimestamp(repoName, "snap_multi")
    
    sql "DROP TABLE ${dbName}.${tableName}_multi FORCE"
    
    // Restore - will create multiple replicas and wait for them
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_multi FROM `${repoName}`
        ON (`${tableName}_multi`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    def result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_multi"
    assertEquals(50, result[0][0])
    
    sql "DROP TABLE ${dbName}.${tableName}_multi FORCE"

    // Test 2: Atomic restore to existing table (covers more checkAndPrepareMeta paths)
    logger.info("=== Test 2: Atomic restore to existing table ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_atomic_exist (
            `id` INT,
            `value` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_atomic_exist VALUES (1, 'before_backup')"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap_atomic TO `${repoName}` ON (${tableName}_atomic_exist)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot2 = syncer.getSnapshotTimestamp(repoName, "snap_atomic")
    
    // Modify data after backup
    sql "UPDATE ${dbName}.${tableName}_atomic_exist SET value = 'after_backup' WHERE id = 1"
    
    // Atomic restore to replace existing table
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_atomic FROM `${repoName}`
        ON (`${tableName}_atomic_exist`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot2}",
            "atomic_restore" = "true",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "strict"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT value FROM ${dbName}.${tableName}_atomic_exist WHERE id = 1"
    assertEquals("before_backup", result[0][0])
    
    sql "DROP TABLE ${dbName}.${tableName}_atomic_exist FORCE"

    // Test 3: Restore with different storage mediums (covers MediumDecisionMaker)
    logger.info("=== Test 3: Different storage mediums ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_medium (
            `id` INT,
            `data` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "HDD"
        )
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_medium VALUES (1, 'test_data')"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap_medium TO `${repoName}` ON (${tableName}_medium)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot3 = syncer.getSnapshotTimestamp(repoName, "snap_medium")
    
    sql "DROP TABLE ${dbName}.${tableName}_medium FORCE"
    
    // Restore with different medium - tests MediumDecisionMaker in checkAndPrepareMeta
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_medium FROM `${repoName}`
        ON (`${tableName}_medium`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot3}",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT * FROM ${dbName}.${tableName}_medium"
    assertEquals(1, result.size())
    
    sql "DROP TABLE ${dbName}.${tableName}_medium FORCE"

    // Test 4: Partitioned table with selective restore
    logger.info("=== Test 4: Selective partition restore ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_selective (
            `date` DATE,
            `id` INT,
            `value` INT
        )
        PARTITION BY RANGE(`date`) (
            PARTITION p1 VALUES LESS THAN ("2024-01-01"),
            PARTITION p2 VALUES LESS THAN ("2024-02-01"),
            PARTITION p3 VALUES LESS THAN ("2024-03-01")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_selective VALUES ('2023-12-15', 1, 100)"
    sql "INSERT INTO ${dbName}.${tableName}_selective VALUES ('2024-01-15', 2, 200)"
    sql "INSERT INTO ${dbName}.${tableName}_selective VALUES ('2024-02-15', 3, 300)"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap_selective TO `${repoName}` ON (${tableName}_selective)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot4 = syncer.getSnapshotTimestamp(repoName, "snap_selective")
    
    // Drop entire table
    sql "DROP TABLE ${dbName}.${tableName}_selective FORCE"
    
    // Restore only some partitions - covers partial restore in checkAndPrepareMeta
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_selective FROM `${repoName}`
        ON (`${tableName}_selective` PARTITION (p1, p2))
        PROPERTIES (
            "backup_timestamp" = "${snapshot4}",
            "storage_medium" = "same_with_upstream",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_selective"
    assertEquals(2, result[0][0]) // Only p1 and p2 should be restored
    
    sql "DROP TABLE ${dbName}.${tableName}_selective FORCE"

    // Test 5: Restore with replica allocation
    logger.info("=== Test 5: Replica allocation handling ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_replica (
            `id` INT,
            `name` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_replica VALUES (1, 'replica_test'), (2, 'replica_test2')"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap_replica TO `${repoName}` ON (${tableName}_replica)"
    syncer.waitSnapshotFinish(dbName)
    def snapshot5 = syncer.getSnapshotTimestamp(repoName, "snap_replica")
    
    sql "DROP TABLE ${dbName}.${tableName}_replica FORCE"
    
    // Restore - tests replica creation and waiting
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_replica FROM `${repoName}`
        ON (`${tableName}_replica`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot5}",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "strict"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT * FROM ${dbName}.${tableName}_replica ORDER BY id"
    assertEquals(2, result.size())
    
    sql "DROP TABLE ${dbName}.${tableName}_replica FORCE"

    // Test 6: Same table multiple restore (covers various restore states)
    logger.info("=== Test 6: Multiple restore operations ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_multi_restore (
            `id` INT,
            `version` INT,
            `data` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    
    // Version 1
    sql "INSERT INTO ${dbName}.${tableName}_multi_restore VALUES (1, 1, 'v1')"
    sql "BACKUP SNAPSHOT ${dbName}.snap_v1 TO `${repoName}` ON (${tableName}_multi_restore)"
    syncer.waitSnapshotFinish(dbName)
    def snapshotV1 = syncer.getSnapshotTimestamp(repoName, "snap_v1")
    
    // Version 2
    sql "INSERT INTO ${dbName}.${tableName}_multi_restore VALUES (2, 2, 'v2')"
    sql "BACKUP SNAPSHOT ${dbName}.snap_v2 TO `${repoName}` ON (${tableName}_multi_restore)"
    syncer.waitSnapshotFinish(dbName)
    def snapshotV2 = syncer.getSnapshotTimestamp(repoName, "snap_v2")
    
    sql "TRUNCATE TABLE ${dbName}.${tableName}_multi_restore"
    
    // Restore version 2
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_v2 FROM `${repoName}`
        ON (`${tableName}_multi_restore`)
        PROPERTIES (
            "backup_timestamp" = "${snapshotV2}",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT COUNT(*) FROM ${dbName}.${tableName}_multi_restore"
    assertEquals(2, result[0][0])
    
    sql "DROP TABLE ${dbName}.${tableName}_multi_restore FORCE"

    // Test 7: Aggregate key table restore
    logger.info("=== Test 7: Aggregate key table with storage medium ===")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_agg (
            `user_id` INT,
            `date` DATE,
            `cost` INT SUM
        )
        AGGREGATE KEY(`user_id`, `date`)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    
    sql "INSERT INTO ${dbName}.${tableName}_agg VALUES (1, '2024-01-01', 100), (1, '2024-01-01', 200)"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap_agg TO `${repoName}` ON (${tableName}_agg)"
    syncer.waitSnapshotFinish(dbName)
    def snapshotAgg = syncer.getSnapshotTimestamp(repoName, "snap_agg")
    
    sql "DROP TABLE ${dbName}.${tableName}_agg FORCE"
    
    // Restore aggregate table
    sql """
        RESTORE SNAPSHOT ${dbName}.snap_agg FROM `${repoName}`
        ON (`${tableName}_agg`)
        PROPERTIES (
            "backup_timestamp" = "${snapshotAgg}",
            "storage_medium" = "same_with_upstream",
            "medium_allocation_mode" = "strict"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SELECT user_id, cost FROM ${dbName}.${tableName}_agg"
    assertEquals(1, result.size())
    assertEquals(300, result[0][1]) // Should be aggregated: 100 + 200
    
    sql "DROP TABLE ${dbName}.${tableName}_agg FORCE"

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

