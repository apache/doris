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

suite("test_backup_restore_colocate_with", "backup_restore") {
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "test_backup_restore_colocate_with_db"
    String tableName = "test_backup_restore_colocate_with_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `saler_id` int NOT NULL,
            `category_id` int NOT NULL,
            `sale_date` date NOT NULL
        )
        DUPLICATE KEY(`saler_id`)
        DISTRIBUTED BY HASH(`saler_id`) BUCKETS 10
        PROPERTIES 
        (
            "replication_num" = "1",
            "colocate_with" = "group1"
        )
        """

    sql """INSERT INTO ${dbName}.${tableName} VALUES (1, 1, '2023-11-01'),(2, 2, '2023-11-02')"""

    def result = sql"SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), 2)

    String snapshotName = "test_backup_restore_colocate_with_snapshot"

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
            "reserve_replica" = "true",
            "colocate_with" = "group1"
        )
        """

    syncer.waitAllRestoreFinish(dbName)

    def restore_index_comment = sql "SHOW CREATE TABLE ${dbName}.${tableName}"

    assertTrue(restore_index_comment[0][1].contains("\"colocate_with\" = \"group1\""))

    result = sql"SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), 2)

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"

}
