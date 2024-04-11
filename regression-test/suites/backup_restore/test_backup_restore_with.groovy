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

suite("test_backup_restore_with_view", "backup_restore") {
    String suiteName = "backup_restore_with_view"
    String dbName = "${suiteName}_db"
    String repoName = "${suiteName}_repo"
    String snapshotName = "${suiteName}_snapshot"
    String tableName = "${suiteName}_table"
    String viewName = "${suiteName}_view"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    int numRows = 10;
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
    sql "DROP VIEW IF EXISTS ${dbName}.${viewName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    List<String> values = []
    for (int j = 1; j <= numRows; ++j) {
        values.add("(${j}, ${j})")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"

    sql """CREATE VIEW ${dbName}.${viewName} (id, count)
        AS
        SELECT * FROM ${dbName}.${tableName} WHERE count > 5
        """

    qt_sql "SELECT * FROM ${dbName}.${tableName} ORDER BY id ASC"
    qt_sql "SELECT * FROM ${dbName}.${viewName} ORDER BY id ASC"

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP VIEW ${dbName}.${viewName}"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    qt_sql "SELECT * FROM ${dbName}.${tableName} ORDER BY id ASC"
    qt_sql "SELECT * FROM ${dbName}.${viewName} ORDER BY id ASC"

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP VIEW ${dbName}.${viewName}"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

