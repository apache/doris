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

suite("test_backup_restore_clean_restore", "backup_restore") {
    String suiteName = "test_backup_restore_clean_restore"
    String dbName = "${suiteName}_db"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String tableNamePrefix = "${suiteName}_tables"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    String tableName1 = "${tableNamePrefix}_1"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName1}"
    sql """
        CREATE TABLE ${dbName}.${tableName1} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ("0"),
            PARTITION `p2` VALUES LESS THAN ("10"),
            PARTITION `p3` VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    def numRows = 20
    List<String> values = []
    for (int j = 0; j < numRows; ++j) {
        values.add("(${j}, ${j})")
    }
    sql "INSERT INTO ${dbName}.${tableName1} VALUES ${values.join(",")}"
    def result = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(result.size(), numRows);

    String tableName2 = "${tableNamePrefix}_2"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName2}"
    sql """
        CREATE TABLE ${dbName}.${tableName2} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ("0"),
            PARTITION `p2` VALUES LESS THAN ("10"),
            PARTITION `p3` VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    sql "INSERT INTO ${dbName}.${tableName2} VALUES ${values.join(",")}"
    result = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(result.size(), numRows);

    String tableName3 = "${tableNamePrefix}_3"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName3}"
    sql """
        CREATE TABLE ${dbName}.${tableName3} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION `p1` VALUES LESS THAN ("0"),
            PARTITION `p2` VALUES LESS THAN ("10"),
            PARTITION `p3` VALUES LESS THAN ("20")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
    """

    sql "INSERT INTO ${dbName}.${tableName3} VALUES ${values.join(",")}"
    result = sql "SELECT * FROM ${dbName}.${tableName3}"
    assertEquals(result.size(), numRows);


    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
    """

    while (!syncer.checkSnapshotFinish(dbName)) {
        Thread.sleep(3000)
    }

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    // restore table1, partition 3 of table2
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON (
            `${tableName1}`,
            `${tableName2}` PARTITION (`p3`)
        )
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "clean_tables" = "true",
            "clean_partitions" = "true"
        )
    """

    while (!syncer.checkAllRestoreFinish(dbName)) {
        Thread.sleep(3000)
    }

    // all data of table 1 must exists
    result = sql "SELECT * FROM ${dbName}.${tableName1}"
    assertEquals(result.size(), numRows);

    // only data in p3 of table 2 exists
    result = sql "SELECT * FROM ${dbName}.${tableName2}"
    assertEquals(result.size(), numRows-10)

    // table3 are dropped
    result = sql """
        SHOW TABLE STATUS FROM ${dbName} LIKE "${tableName3}"
    """
    assertEquals(result.size(), 0)

    sql "DROP TABLE ${dbName}.${tableName1} FORCE"
    sql "DROP TABLE ${dbName}.${tableName2} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

