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

suite("test_backup_restore_atomic_reserve_replica", "backup_restore") {
    String suiteName = "test_backup_restore_atomic_reserve_replica"
    String dbName = "${suiteName}_db"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String tableNamePrefix = "${suiteName}_tables"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    // 1. atomic restore with different replication num, base 1, target 3
    // 2. atomic restore with reserve_replica = true, base 3, target 1

    int numTables = 4;
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
            PARTITION BY RANGE(`id`)
            (
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN ("30"),
                PARTITION p4 VALUES LESS THAN ("40"),
                PARTITION p5 VALUES LESS THAN ("50"),
                PARTITION p6 VALUES LESS THAN ("60"),
                PARTITION p7 VALUES LESS THAN ("120")
            )
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES
            (
                "replication_num" = "1"
            )
            """
    }

    int numRows = 10;
    List<String> values = []
    for (int j = 1; j <= numRows; ++j) {
        values.add("(${j}0, ${j}0)")
    }

    sql "INSERT INTO ${dbName}.${tableNamePrefix}_0 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_1 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_2 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_3 VALUES ${values.join(",")}"

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
    """

    syncer.waitSnapshotFinish(dbName)

    for (def tableName in tables) {
        sql "TRUNCATE TABLE ${dbName}.${tableName}"
    }

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    // restore with replication num = 3
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "replication_num" = "3",
            "atomic_restore" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)

    sql "sync"
    for (def tableName in tables) {
        qt_sql "SELECT * FROM ${dbName}.${tableName} ORDER BY id"
    }

    // restore with reserve_replica = true
    for (def tableName in tables) {
        sql "TRUNCATE TABLE ${dbName}.${tableName}"
    }

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

    sql "sync"
    for (def tableName in tables) {
        qt_sql "SELECT * FROM ${dbName}.${tableName} ORDER BY id"
    }

    for (def tableName in tables) {
        sql "DROP TABLE ${dbName}.${tableName} FORCE"
    }
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}



