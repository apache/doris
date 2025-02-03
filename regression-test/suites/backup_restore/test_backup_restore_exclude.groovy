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

suite("test_backup_restore_exclude", "backup_restore") {
    String dbName = "backup_restore_exclude"
    String suiteName = "test_backup_restore_exclude"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String tableNamePrefix = "${suiteName}_tables"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    int numTables = 10;
    int numRows = 10;
    List<String> tables = []
    for (int i = 0; i < numTables; ++i) {
        String tableName = "${tableNamePrefix}_${i}"
        tables.add(tableName)
        sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
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
        def result = sql "SELECT * FROM ${dbName}.${tableName}"
        assertEquals(result.size(), numRows);
    }

    def backupExcludeTable = tables.removeLast();
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        EXCLUDE (${backupExcludeTable})
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    // Overwrite exists table.
    sql "INSERT INTO ${dbName}.${backupExcludeTable} VALUES (20, 20), (21, 21)"
    qt_select "SELECT * FROM ${dbName}.${backupExcludeTable} ORDER BY id"

    def restoreExcludeTable = tables.removeLast()
    sql "DROP TABLE ${dbName}.${restoreExcludeTable} FORCE"

    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        EXCLUDE (${restoreExcludeTable})
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    qt_select "SELECT * FROM ${dbName}.${backupExcludeTable} ORDER BY id"
    for (def tableName in tables) {
        def result = sql "SELECT * FROM ${dbName}.${tableName}"
        assertEquals(result.size(), numRows);
        sql "DROP TABLE ${dbName}.${tableName} FORCE"
    }

    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

