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

suite("test_backup_restore_full_lifecycle_s3", "backup_restore") {
    String suiteName = "full_lifecycle_s3"
    String dbName = "db_${suiteName}"
    String repoName = "repo_${suiteName}"
    
    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "USE ${dbName}"

    // Test 1: Single table full lifecycle - HDD strict
    logger.info("=== Test 1: Single table full lifecycle - HDD strict ===")
    
    String tableName1 = "tbl_single_hdd"
    sql """
        CREATE TABLE ${tableName1} (
            id INT,
            name VARCHAR(100),
            value DECIMAL(10,2)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 3
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "hdd"
        )
    """
    
    sql """
        INSERT INTO ${tableName1} VALUES
        (1, 'row1', 10.5),
        (2, 'row2', 20.3),
        (3, 'row3', 30.8)
    """
    
    String snapshotName1 = "snapshot_${tableName1}"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName1}
        TO ${repoName}
        ON (${tableName1})
    """
    
    syncer.waitSnapshotFinish(dbName)
    def snapshot1 = syncer.getSnapshotTimestamp(repoName, snapshotName1)
    
    sql "DROP TABLE ${tableName1} FORCE"
    
    // Restore with strict mode
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName1}
        FROM ${repoName}
        ON (${tableName1})
        PROPERTIES (
            "backup_timestamp" = "${snapshot1}",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "strict"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    def result1 = sql "SELECT * FROM ${tableName1} ORDER BY id"
    assertEquals(3, result1.size())
    assertEquals(1, result1[0][0])
    assertEquals("row1", result1[0][1])
    assertEquals(10.5, result1[0][2] as Double, 0.01)
    
    logger.info("Test 1 passed: Single table HDD strict lifecycle completed")
    sql "DROP TABLE ${tableName1} FORCE"

    // Test 2: Multi-table full lifecycle - SSD adaptive
    logger.info("=== Test 2: Multi-table full lifecycle - SSD adaptive ===")
    
    String tableName2A = "tbl_multi_a"
    String tableName2B = "tbl_multi_b"
    
    sql """
        CREATE TABLE ${tableName2A} (
            id INT,
            data VARCHAR(50)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "ssd"
        )
    """
    
    sql """
        CREATE TABLE ${tableName2B} (
            id INT,
            data VARCHAR(50)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "ssd"
        )
    """
    
    sql "INSERT INTO ${tableName2A} VALUES (1, 'data_a1'), (2, 'data_a2')"
    sql "INSERT INTO ${tableName2B} VALUES (10, 'data_b1'), (20, 'data_b2')"
    
    String snapshotName2 = "snapshot_multi"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName2}
        TO ${repoName}
        ON (${tableName2A}, ${tableName2B})
    """
    
    syncer.waitSnapshotFinish(dbName)
    def snapshot2 = syncer.getSnapshotTimestamp(repoName, snapshotName2)
    
    sql "DROP TABLE ${tableName2A} FORCE"
    sql "DROP TABLE ${tableName2B} FORCE"
    
    // Restore with adaptive mode
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName2}
        FROM ${repoName}
        ON (${tableName2A}, ${tableName2B})
        PROPERTIES (
            "backup_timestamp" = "${snapshot2}",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    def result2A = sql "SELECT COUNT(*) FROM ${tableName2A}"
    def result2B = sql "SELECT COUNT(*) FROM ${tableName2B}"
    assertEquals(2, result2A[0][0])
    assertEquals(2, result2B[0][0])
    
    logger.info("Test 2 passed: Multi-table SSD adaptive lifecycle completed")
    sql "DROP TABLE ${tableName2A} FORCE"
    sql "DROP TABLE ${tableName2B} FORCE"

    // Test 3: Partitioned table full lifecycle - same_with_upstream
    logger.info("=== Test 3: Partitioned table full lifecycle - same_with_upstream ===")
    
    String tableName3 = "tbl_partitioned"
    sql """
        CREATE TABLE ${tableName3} (
            dt DATE,
            id INT,
            value VARCHAR(100)
        )
        PARTITION BY RANGE(dt) (
            PARTITION p20240101 VALUES LESS THAN ("2024-01-02"),
            PARTITION p20240102 VALUES LESS THAN ("2024-01-03"),
            PARTITION p20240103 VALUES LESS THAN ("2024-01-04")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "hdd"
        )
    """
    
    sql """
        INSERT INTO ${tableName3} VALUES
        ('2024-01-01', 1, 'value1'),
        ('2024-01-02', 2, 'value2'),
        ('2024-01-03', 3, 'value3')
    """
    
    String snapshotName3 = "snapshot_${tableName3}"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName3}
        TO ${repoName}
        ON (${tableName3})
    """
    
    syncer.waitSnapshotFinish(dbName)
    def snapshot3 = syncer.getSnapshotTimestamp(repoName, snapshotName3)
    
    sql "DROP TABLE ${tableName3} FORCE"
    
    // Restore with same_with_upstream
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName3}
        FROM ${repoName}
        ON (${tableName3})
        PROPERTIES (
            "backup_timestamp" = "${snapshot3}",
            "storage_medium" = "same_with_upstream",
            "medium_allocation_mode" = "adaptive"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    def result3 = sql "SELECT COUNT(*) FROM ${tableName3}"
    assertEquals(3, result3[0][0])
    
    // Verify partitions
    def partitions = sql "SHOW PARTITIONS FROM ${tableName3}"
    assertEquals(3, partitions.size())
    
    logger.info("Test 3 passed: Partitioned table same_with_upstream lifecycle completed")
    sql "DROP TABLE ${tableName3} FORCE"

    // Test 4: Atomic restore full lifecycle
    logger.info("=== Test 4: Atomic restore full lifecycle ===")
    
    String tableName4 = "tbl_atomic"
    sql """
        CREATE TABLE ${tableName4} (
            id INT,
            data VARCHAR(100)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    
    sql "INSERT INTO ${tableName4} VALUES (1, 'original_data')"
    
    String snapshotName4 = "snapshot_${tableName4}"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName4}
        TO ${repoName}
        ON (${tableName4})
    """
    
    syncer.waitSnapshotFinish(dbName)
    def snapshot4 = syncer.getSnapshotTimestamp(repoName, snapshotName4)
    
    // Insert more data
    sql "INSERT INTO ${tableName4} VALUES (2, 'new_data')"
    
    def beforeRestore = sql "SELECT COUNT(*) FROM ${tableName4}"
    assertEquals(2, beforeRestore[0][0])
    
    // Atomic restore - should replace existing table
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName4}
        FROM ${repoName}
        ON (${tableName4})
        PROPERTIES (
            "backup_timestamp" = "${snapshot4}",
            "atomic_restore" = "true"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    def afterRestore = sql "SELECT * FROM ${tableName4}"
    assertEquals(1, afterRestore.size())
    assertEquals(1, afterRestore[0][0])
    assertEquals("original_data", afterRestore[0][1])
    
    logger.info("Test 4 passed: Atomic restore lifecycle completed")
    sql "DROP TABLE ${tableName4} FORCE"

    // Test 5: Partial partition restore
    logger.info("=== Test 5: Partial partition restore lifecycle ===")
    
    String tableName5 = "tbl_partial_partition"
    sql """
        CREATE TABLE ${tableName5} (
            dt DATE,
            id INT,
            value INT
        )
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES LESS THAN ("2024-01-02"),
            PARTITION p2 VALUES LESS THAN ("2024-01-03"),
            PARTITION p3 VALUES LESS THAN ("2024-01-04")
        )
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """
    
    sql "INSERT INTO ${tableName5} VALUES ('2024-01-01', 1, 100)"
    sql "INSERT INTO ${tableName5} VALUES ('2024-01-02', 2, 200)"
    sql "INSERT INTO ${tableName5} VALUES ('2024-01-03', 3, 300)"
    
    String snapshotName5 = "snapshot_${tableName5}"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName5}
        TO ${repoName}
        ON (${tableName5} PARTITION (p1, p2))
    """
    
    syncer.waitSnapshotFinish(dbName)
    def snapshot5 = syncer.getSnapshotTimestamp(repoName, snapshotName5)
    
    sql "DROP TABLE ${tableName5} FORCE"
    
    // Restore only p1 partition
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName5}
        FROM ${repoName}
        ON (${tableName5} PARTITION (p1))
        PROPERTIES (
            "backup_timestamp" = "${snapshot5}"
        )
    """
    
    syncer.waitAllRestoreFinish(dbName)
    
    def result5 = sql "SELECT * FROM ${tableName5}"
    assertEquals(1, result5.size())
    assertEquals(1, result5[0][1])
    
    logger.info("Test 5 passed: Partial partition restore completed")
    sql "DROP TABLE ${tableName5} FORCE"

    // Cleanup
    sql "DROP DATABASE ${dbName} FORCE"
    
    logger.info("=== All lifecycle tests completed successfully ===")
}
