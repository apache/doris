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

import backup_restore.BackupRestoreTestHelper

/**
 * Test atomic restore functionality for S3 repository
 * 
 * Test scenarios:
 * 1. Basic atomic restore (complete table replacement)
 * 2. Atomic restore + storage medium setting
 * 3. Multiple atomic restores (version rollback)
 */
suite("test_backup_restore_medium_atomic_restore", "backup_restore") {
    String dbName = "test_br_atomic_db"
    String repoName = "test_br_atomic_repo_" + UUID.randomUUID().toString().replace("-", "")
    String tableName = "test_table"

    def helper = new BackupRestoreTestHelper(sql, getSyncer())
    getSyncer().createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // ============================================================
    // Test 1: Basic atomic restore
    // ============================================================
    helper.logTestStart("Basic atomic restore")
    
    sql """
        CREATE TABLE ${dbName}.${tableName}_1 (
            `id` LARGEINT NOT NULL,
            `name` VARCHAR(128),
            `count` LARGEINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
    """

    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, 'name${i}', ${i * 100})")
    }
    helper.insertData(dbName, "${tableName}_1", values)
    
    assertTrue(helper.backupToS3(dbName, "snap1", repoName, "${tableName}_1"))

    // Insert more data
    helper.insertData(dbName, "${tableName}_1", ["(11, 'name11', 1100)"])
    assertTrue(helper.verifyRowCount(dbName, "${tableName}_1", 11))

    // Atomic restore (should replace table)
    assertTrue(helper.restoreFromS3(dbName, "snap1", repoName, "${tableName}_1", [
        "reserve_replica": "true",
        "atomic_restore": "true",
        "storage_medium": "hdd",
        "medium_allocation_mode": "strict"
    ]))

    assertTrue(helper.verifyRowCount(dbName, "${tableName}_1", 10))
    assertTrue(helper.verifyDataExists(dbName, "${tableName}_1", "id = 11", false))
    assertTrue(helper.verifyTableProperty(dbName, "${tableName}_1", "medium_allocation_mode"))
    logger.info("✓ Atomic restore successful: new data removed")

    helper.dropTable(dbName, "${tableName}_1")
    helper.logTestEnd("Basic atomic restore", true)

    // ============================================================
    // Test 2: Multiple atomic restores (version rollback)
    // ============================================================
    helper.logTestStart("Multiple atomic restores (version rollback)")
    
    helper.createSimpleTable(dbName, "${tableName}_2", [
        "storage_medium": "HDD"
    ])

    // Version 1: 1 row
    helper.insertData(dbName, "${tableName}_2", ["(1, 'v1')"])
    assertTrue(helper.backupToS3(dbName, "snap_v1", repoName, "${tableName}_2"))

    // Version 2: 2 rows
    helper.insertData(dbName, "${tableName}_2", ["(2, 'v2')"])
    assertTrue(helper.backupToS3(dbName, "snap_v2", repoName, "${tableName}_2"))

    // Add more data
    helper.insertData(dbName, "${tableName}_2", ["(3, 'v3')"])
    assertTrue(helper.verifyRowCount(dbName, "${tableName}_2", 3))

    // Atomic restore to version 1
    assertTrue(helper.restoreFromS3(dbName, "snap_v1", repoName, "${tableName}_2", [
        "atomic_restore": "true",
        "storage_medium": "same_with_upstream"
    ]))
    assertTrue(helper.verifyRowCount(dbName, "${tableName}_2", 1))
    logger.info("✓ Restored to version 1")

    // Atomic restore to version 2
    assertTrue(helper.restoreFromS3(dbName, "snap_v2", repoName, "${tableName}_2", [
        "atomic_restore": "true",
        "storage_medium": "same_with_upstream"
    ]))
    assertTrue(helper.verifyRowCount(dbName, "${tableName}_2", 2))
    logger.info("✓ Restored to version 2")

    helper.dropTable(dbName, "${tableName}_2")
    helper.logTestEnd("Multiple atomic restores (version rollback)", true)

    // ============================================================
    // Cleanup
    // ============================================================
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
    
    logger.info("=== All atomic restore tests completed ===")
}

