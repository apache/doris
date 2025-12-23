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

suite("test_backup_restore_medium_allocation", "backup_restore") {
    String suiteName = "test_br_medium_alloc"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // Test 1: RESTORE with medium_allocation_mode = 'strict'
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_strict"
    sql """
        CREATE TABLE ${dbName}.${tableName}_strict (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1",
            "storage_medium" = "SSD"
        )
        """

    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${dbName}.${tableName}_strict VALUES ${values.join(",")}"
    def result = sql "SELECT * FROM ${dbName}.${tableName}_strict"
    assertEquals(result.size(), values.size())

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}_strict
        TO `${repoName}`
        ON (${tableName}_strict)
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, "${snapshotName}_strict")
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName}_strict FORCE"

    // Restore with strict mode - should use HDD regardless of original medium
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}_strict
        FROM `${repoName}`
        ON ( `${tableName}_strict`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "medium_allocation_mode" = "strict"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}_strict"
    assertEquals(result.size(), values.size())

    // Verify the table was restored with medium_allocation_mode
    def show_result = sql "SHOW CREATE TABLE ${dbName}.${tableName}_strict"
    def createTableStr = show_result[0][1]
    assertTrue(createTableStr.contains("medium_allocation_mode"))

    sql "DROP TABLE ${dbName}.${tableName}_strict FORCE"

    // Test 2: RESTORE with medium_allocation_mode = 'adaptive'
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_adaptive"
    sql """
        CREATE TABLE ${dbName}.${tableName}_adaptive (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${dbName}.${tableName}_adaptive VALUES ${values.join(",")}"
    result = sql "SELECT * FROM ${dbName}.${tableName}_adaptive"
    assertEquals(result.size(), values.size())

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}_adaptive
        TO `${repoName}`
        ON (${tableName}_adaptive)
    """

    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, "${snapshotName}_adaptive")
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName}_adaptive FORCE"

    // Restore with adaptive mode - should preserve original medium
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}_adaptive
        FROM `${repoName}`
        ON ( `${tableName}_adaptive`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "medium_allocation_mode" = "adaptive"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}_adaptive"
    assertEquals(result.size(), values.size())

    sql "DROP TABLE ${dbName}.${tableName}_adaptive FORCE"

    // Test 3: RESTORE with default mode (should be strict)
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_default"
    sql """
        CREATE TABLE ${dbName}.${tableName}_default (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    values = []
    for (int i = 1; i <= 5; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${dbName}.${tableName}_default VALUES ${values.join(",")}"

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}_default
        TO `${repoName}`
        ON (${tableName}_default)
    """

    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, "${snapshotName}_default")
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName}_default FORCE"

    // Restore without specifying medium_allocation_mode - should default to strict
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}_default
        FROM `${repoName}`
        ON ( `${tableName}_default`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}_default"
    assertEquals(result.size(), values.size())

    sql "DROP TABLE ${dbName}.${tableName}_default FORCE"

    // Test 4: RESTORE with invalid medium_allocation_mode
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}_invalid"
    sql """
        CREATE TABLE ${dbName}.${tableName}_invalid (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    sql "INSERT INTO ${dbName}.${tableName}_invalid VALUES (1, 1)"

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}_invalid
        TO `${repoName}`
        ON (${tableName}_invalid)
    """

    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, "${snapshotName}_invalid")
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName}_invalid FORCE"

    // Try to restore with invalid medium_allocation_mode value
    try {
        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName}_invalid
            FROM `${repoName}`
            ON ( `${tableName}_invalid`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true",
                "medium_allocation_mode" = "invalid_mode"
            )
        """
        throw new IllegalStateException("Should throw error for invalid medium_allocation_mode")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Invalid medium_allocation_mode value") ||
                  ex.getMessage().contains("invalid_mode"),
                  "Expected error message about invalid medium_allocation_mode, got: " + ex.getMessage())
    }

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
