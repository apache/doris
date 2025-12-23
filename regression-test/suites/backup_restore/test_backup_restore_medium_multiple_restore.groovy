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

suite("test_backup_restore_medium_multiple_restore", "backup_restore") {
    String suiteName = "test_br_multi_restore"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Create and backup table with HDD
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` INT,
            `value` STRING
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "storage_medium" = "HDD"
        )
    """
    
    sql "INSERT INTO ${dbName}.${tableName} VALUES (1, 'test')"
    
    sql "BACKUP SNAPSHOT ${dbName}.snap1 TO `${repoName}` ON (${tableName})"
    syncer.waitSnapshotFinish(dbName)
    def snapshot = syncer.getSnapshotTimestamp(repoName, "snap1")
    assertTrue(snapshot != null, "Snapshot should be created")

    // Test 1: Restore as SSD with strict mode
    logger.info("=== Test 1: Restore with SSD strict mode ===")
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql """
        RESTORE SNAPSHOT ${dbName}.snap1 FROM `${repoName}`
        ON (`${tableName}`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "ssd",
            "medium_allocation_mode" = "strict"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    
    def result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    logger.info("First restore (SSD strict): ${result[0][1]}")
    assertTrue(result[0][1].contains("medium_allocation_mode"),
              "Should have medium_allocation_mode")

    def data = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(1, data.size(), "Should have 1 row after first restore")

    // Test 2: Restore again with HDD adaptive
    logger.info("=== Test 2: Restore with HDD adaptive mode ===")
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql """
        RESTORE SNAPSHOT ${dbName}.snap1 FROM `${repoName}`
        ON (`${tableName}`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    logger.info("Second restore (HDD adaptive): ${result[0][1]}")
    assertTrue(result[0][1].toLowerCase().contains("hdd") || 
               result[0][1].toLowerCase().contains("\"medium\" : \"hdd\""),
              "Should use HDD medium")

    data = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(1, data.size(), "Should have 1 row after second restore")

    // Test 3: Restore with same_with_upstream (original was HDD)
    logger.info("=== Test 3: Restore with same_with_upstream ===")
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql """
        RESTORE SNAPSHOT ${dbName}.snap1 FROM `${repoName}`
        ON (`${tableName}`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "same_with_upstream",
            "medium_allocation_mode" = "adaptive"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    
    result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    logger.info("Third restore (same_with_upstream): ${result[0][1]}")
    // Should inherit HDD from upstream or not specify medium explicitly
    assertTrue(result[0][1].toLowerCase().contains("hdd") || 
               !result[0][1].toLowerCase().contains("\"ssd\""),
              "Should inherit HDD from upstream or use default")

    data = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(1, data.size(), "Should have 1 row after third restore")

    // Test 4: Multiple consecutive restores with different modes
    logger.info("=== Test 4: Consecutive restores with mode changes ===")
    
    // First: strict mode
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql """
        RESTORE SNAPSHOT ${dbName}.snap1 FROM `${repoName}`
        ON (`${tableName}`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "strict"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    assertTrue(result[0][1].contains("\"strict\""),
              "Should have strict mode after first restore")

    // Second: change to adaptive mode
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql """
        RESTORE SNAPSHOT ${dbName}.snap1 FROM `${repoName}`
        ON (`${tableName}`)
        PROPERTIES (
            "backup_timestamp" = "${snapshot}",
            "storage_medium" = "hdd",
            "medium_allocation_mode" = "adaptive"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    result = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
    assertTrue(result[0][1].contains("\"adaptive\""),
              "Should have adaptive mode after second restore")

    data = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(1, data.size(), "Should still have 1 row")

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

