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

suite("test_unique_backup_restore", "backup_restore") {
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "unique_backup_restore_db"
    String tableName = "test_unique_backup_restore_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT NOT NULL DEFAULT "0")
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
        """

    // version1 (1,1)(2,2)
    sql """INSERT INTO ${dbName}.${tableName} VALUES (1,1),(2,2)"""
    def result = sql"SELECT * FROM ${dbName}.${tableName} ORDER BY id"
    assertEquals(result.size(), 2)

    // version2 (1,5)(2,2)
    sql "INSERT INTO ${dbName}.${tableName} VALUES (1,5)"
    
    String snapshotName = "test_unique_backup_restore_snapshot"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
        PROPERTIES ("type" = "full")
        """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "TRUNCATE TABLE ${dbName}.${tableName}"
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
        """

    syncer.waitAllRestoreFinish(dbName)

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), 2)
    
    sql "DROP REPOSITORY `${repoName}`"

    sql "DELETE FROM ${dbName}.${tableName} WHERE id = 1"
    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), 1)
    repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    syncer.createS3Repository(repoName)
    snapshotName = "test_unique_delete_backup_restore_snapshot"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
        PROPERTIES ("type" = "full")
        """
    
    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)
    sql "TRUNCATE TABLE ${dbName}.${tableName}"
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${tableName}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
        """
    syncer.waitAllRestoreFinish(dbName)
    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), 1)
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
