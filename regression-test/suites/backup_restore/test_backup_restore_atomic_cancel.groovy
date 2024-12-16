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

suite("test_backup_restore_atomic_cancel") {
    String suiteName = "test_backup_restore_atomic_cancelled"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String tableName1 = "${suiteName}_table_1"
    String viewName = "${suiteName}_view"
    String snapshotName = "${suiteName}_snapshot"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql """
        CREATE TABLE ${dbName}.${tableName1} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    sql "DROP VIEW IF EXISTS ${dbName}.${viewName}"
    sql """
        CREATE VIEW ${dbName}.${viewName}
        AS
        SELECT id, count FROM ${dbName}.${tableName}
        WHERE id > 5
    """

    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"
    sql "sync"
    def result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

    sql "INSERT INTO ${dbName}.${tableName1} VALUES ${values.join(",")}"
    sql "sync"
    result = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(result.size(), values.size());


    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
    """

    syncer.waitSnapshotFinish(dbName)

    // alter view and restore, it must failed because the signatures are not matched

    sql """
        ALTER VIEW ${dbName}.${viewName}
        AS
        SELECT id,count FROM ${dbName}.${tableName}
        WHERE id < 100

    """

    sql "INSERT INTO ${dbName}.${tableName} VALUES (11, 11)"

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "atomic_restore" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    def restore_result = sql_return_maparray """ SHOW RESTORE FROM ${dbName} WHERE Label ="${snapshotName}" """
    restore_result.last()
    logger.info("show restore result: ${restore_result}")
    assertTrue(restore_result.last().State == "CANCELLED")

    sql "sync"
    // Do not affect any tables.
    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size() + 1);

    result = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(result.size(), values.size());

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP TABLE ${dbName}.${tableName1} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}


