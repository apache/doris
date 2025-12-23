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

suite("test_backup_restore_medium_with_partitions", "backup_restore") {
    String suiteName = "test_br_medium_part"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_tbl"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Test 1: Partitioned table with strict mode
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_strict"
    sql """
        CREATE TABLE ${dbName}.${tableName}_strict (
            `date` DATE NOT NULL,
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`date`, `id`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
            PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01')),
            PARTITION p202403 VALUES [('2024-03-01'), ('2024-04-01'))
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    // Insert data into different partitions
    sql """INSERT INTO ${dbName}.${tableName}_strict VALUES 
           ('2024-01-15', 1, 100),
           ('2024-02-15', 2, 200),
           ('2024-03-15', 3, 300)"""

    def result = sql "SELECT * FROM ${dbName}.${tableName}_strict"
    assertEquals(result.size(), 3)

    // Backup
    sql """
        BACKUP SNAPSHOT ${dbName}.snapshot_part_strict
        TO `${repoName}`
        ON (${tableName}_strict)
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, "snapshot_part_strict")
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName}_strict FORCE"

    // Restore with strict mode and explicit storage medium
    sql """
        RESTORE SNAPSHOT ${dbName}.snapshot_part_strict
        FROM `${repoName}`
        ON (`${tableName}_strict`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "strict"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}_strict ORDER BY date"
    assertEquals(result.size(), 3)
    assertEquals(result[0][0].toString(), "2024-01-15")
    assertEquals(result[1][0].toString(), "2024-02-15")
    assertEquals(result[2][0].toString(), "2024-03-15")

    // Verify partitions are created correctly
    def partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_strict"
    assertEquals(partitions.size(), 3, "All 3 partitions should be restored")

    sql "DROP TABLE ${dbName}.${tableName}_strict FORCE"

    // Test 2: Restore specific partitions with adaptive mode
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_adaptive"
    sql """
        CREATE TABLE ${dbName}.${tableName}_adaptive (
            `date` DATE NOT NULL,
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`date`, `id`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
            PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01'))
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1",
            "storage_medium" = "SSD"
        )
    """

    sql """INSERT INTO ${dbName}.${tableName}_adaptive VALUES 
           ('2024-01-15', 1, 100),
           ('2024-02-15', 2, 200)"""

    // Backup
    sql """
        BACKUP SNAPSHOT ${dbName}.snapshot_part_adaptive
        TO `${repoName}`
        ON (${tableName}_adaptive)
    """

    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, "snapshot_part_adaptive")
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName}_adaptive FORCE"

    // Restore with adaptive mode - should adapt storage medium based on availability
    sql """
        RESTORE SNAPSHOT ${dbName}.snapshot_part_adaptive
        FROM `${repoName}`
        ON (`${tableName}_adaptive`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "adaptive"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}_adaptive ORDER BY date"
    assertEquals(result.size(), 2)

    // Verify partitions
    partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_adaptive"
    assertEquals(partitions.size(), 2, "Both partitions should be restored")

    sql "DROP TABLE ${dbName}.${tableName}_adaptive FORCE"

    // Test 3: Restore single partition with medium settings
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_single_part"
    sql """
        CREATE TABLE ${dbName}.${tableName}_single_part (
            `date` DATE NOT NULL,
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`date`, `id`)
        PARTITION BY RANGE(`date`)
        (
            PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01')),
            PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01'))
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    sql """INSERT INTO ${dbName}.${tableName}_single_part VALUES 
           ('2024-01-15', 1, 100),
           ('2024-02-15', 2, 200)"""

    // Backup
    sql """
        BACKUP SNAPSHOT ${dbName}.snapshot_single_part
        TO `${repoName}`
        ON (${tableName}_single_part PARTITION (p202401))
    """

    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, "snapshot_single_part")
    assertTrue(snapshot != null)

    // Drop only the backed up partition
    sql "ALTER TABLE ${dbName}.${tableName}_single_part DROP PARTITION p202401"

    // Restore single partition with medium settings
    sql """
        RESTORE SNAPSHOT ${dbName}.snapshot_single_part
        FROM `${repoName}`
        ON (`${tableName}_single_part` PARTITION (p202401))
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "strict"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    // Verify only p202401 data is restored
    result = sql "SELECT * FROM ${dbName}.${tableName}_single_part WHERE date < '2024-02-01'"
    assertEquals(result.size(), 1, "Only p202401 partition data should be present")
    assertEquals(result[0][0].toString(), "2024-01-15")

    // Verify partitions
    partitions = sql "SHOW PARTITIONS FROM ${dbName}.${tableName}_single_part"
    assertEquals(partitions.size(), 2, "Both partitions should exist (one original, one restored)")

    sql "DROP TABLE ${dbName}.${tableName}_single_part FORCE"

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

