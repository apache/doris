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

suite("test_backup_restore_diff_repo_same_snapshot", "backup_restore") {
    String suiteName = "test_backup_restore_diff_repo_same_snapshot"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String tableName = "${suiteName}_table"
    String snapshotName = "${suiteName}_snapshot"

    def syncer = getSyncer()
    syncer.createS3Repository("${repoName}_1")
    syncer.createS3Repository("${repoName}_2")

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}_1"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}_2"
    sql "DROP TABLE IF EXISTS ${dbName}_1.${tableName}_1"
    sql "DROP TABLE IF EXISTS ${dbName}_2.${tableName}_2"
    sql """
        CREATE TABLE ${dbName}_1.${tableName}_1 (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ( "replication_num" = "1")
        """
    sql """
        CREATE TABLE ${dbName}_2.${tableName}_2 (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0")
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES ( "replication_num" = "1" )
        """

    List<String> values = []
    for (int i = 1; i <= 10; ++i) {
        values.add("(${i}, ${i})")
    }
    sql "INSERT INTO ${dbName}_1.${tableName}_1 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}_2.${tableName}_2 VALUES ${values.join(",")}"
    def result = sql "SELECT * FROM ${dbName}_1.${tableName}_1"
    assertEquals(result.size(), values.size());
    result = sql "SELECT * FROM ${dbName}_2.${tableName}_2"
    assertEquals(result.size(), values.size());

    // Backup to different repo, with same snapshot name.
    sql """
        BACKUP SNAPSHOT ${dbName}_1.${snapshotName}
        TO `${repoName}_1`
        ON (${tableName}_1)
    """
    sql """
        BACKUP SNAPSHOT ${dbName}_2.${snapshotName}
        TO `${repoName}_2`
        ON (${tableName}_2)
    """

    syncer.waitSnapshotFinish("${dbName}_1")
    syncer.waitSnapshotFinish("${dbName}_2")

    // Restore snapshot from repo_1 to db_1
    def snapshot = syncer.getSnapshotTimestamp("${repoName}_1", snapshotName)
    assertTrue(snapshot != null)

    sql "TRUNCATE TABLE ${dbName}_1.${tableName}_1"
    sql """
        RESTORE SNAPSHOT ${dbName}_1.${snapshotName}
        FROM `${repoName}_1`
        ON ( `${tableName}_1`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish("${dbName}_1")

    result = sql "SELECT * FROM ${dbName}_1.${tableName}_1"
    assertEquals(result.size(), values.size());

    // Restore snapshot from repo_2 to db_2
    snapshot = syncer.getSnapshotTimestamp("${repoName}_2", snapshotName)
    assertTrue(snapshot != null)

    sql "TRUNCATE TABLE ${dbName}_2.${tableName}_2"
    sql """
        RESTORE SNAPSHOT ${dbName}_2.${snapshotName}
        FROM `${repoName}_2`
        ON ( `${tableName}_2`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish("${dbName}_2")

    result = sql "SELECT * FROM ${dbName}_2.${tableName}_2"
    assertEquals(result.size(), values.size());

    sql "DROP TABLE ${dbName}_1.${tableName}_1 FORCE"
    sql "DROP TABLE ${dbName}_2.${tableName}_2 FORCE"
    sql "DROP DATABASE ${dbName}_1 FORCE"
    sql "DROP DATABASE ${dbName}_2 FORCE"
    sql "DROP REPOSITORY `${repoName}_1`"
    sql "DROP REPOSITORY `${repoName}_2`"
}

