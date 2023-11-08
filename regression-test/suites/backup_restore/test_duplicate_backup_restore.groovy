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

suite("test_duplicate_backup_restore", "backup_restore") {
    String repoName = "test_duplicate_backup_restore_repo"
    String dbName = "duplicate_backup_restore_db"
    String tableName = "test_duplicate_backup_restore_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT NOT NULL DEFAULT "0")
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """

    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableName} VALUES (1,1),(2,2),(3,3),(1,1),(2,2)"
    def result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size() + 5);

    String snapshotName = "test_duplicate_backup_restore"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
        PROPERTIES ("type" = "full")
        """

    while (!syncer.checkSnapshotFinish(dbName)) {
        Thread.sleep(3000)
    }

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
            "replication_num" = "1"
        )
        """
    
    while (!syncer.checkAllRestoreFinish(dbName)) {
        Thread.sleep(3000)
    }

    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size() + 5);
    
    sql "DROP REPOSITORY `${repoName}`"

    sql "DELETE FROM ${dbName}.${tableName} WHERE id = 1"
    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size() + 5 - 3)
    repoName = "test_duplicate_delete_backup_restore_repo"
    syncer.createS3Repository(repoName)
    snapshotName = "test_duplicate_delete_backup_restore_snapshot"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableName})
        PROPERTIES ("type" = "full")
        """
    
    while (!syncer.checkSnapshotFinish(dbName)) {
        Thread.sleep(3000)
    }

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
            "replication_num" = "1"
        )
        """
    while (!syncer.checkAllRestoreFinish(dbName)) {
        Thread.sleep(3000)
    }
    result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size() + 5 - 3)
    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
