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

suite("test_backup_restore_atomic", "backup_restore") {
    String suiteName = "test_backup_restore_atomic"
    String dbName = "${suiteName}_db_1"
    String dbName1 = "${suiteName}_db_2"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String tableNamePrefix = "${suiteName}_tables"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName1}"

    // 1. restore to not exists table_0
    // 2. restore partial data to table_1
    // 3. restore less data to table_2
    // 4. restore incremental data to table_3
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

    // 5. the len of table name equals to the config table_name_length_limit
    def maxLabelLen = getFeConfig("table_name_length_limit").toInteger()
    def maxTableName = "".padRight(maxLabelLen, "x")
    logger.info("config table_name_length_limit = ${maxLabelLen}, table name = ${maxTableName}")
    sql "DROP TABLE IF EXISTS ${dbName}.${maxTableName}"
    sql """
        CREATE TABLE ${dbName}.${maxTableName} (
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
    tables.add(maxTableName)

    int numRows = 10;
    List<String> values = []
    for (int j = 1; j <= numRows; ++j) {
        values.add("(${j}0, ${j}0)")
    }

    sql "INSERT INTO ${dbName}.${tableNamePrefix}_0 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_1 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_2 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_3 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${maxTableName} VALUES ${values.join(",")}"

    // the other partitions of table_1 will be drop
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (
            ${tableNamePrefix}_0,
            ${tableNamePrefix}_1 PARTITION (p1, p2, p3),
            ${tableNamePrefix}_2,
            ${tableNamePrefix}_3,
            ${maxTableName}
        )
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    // drop table_0
    sql "DROP TABLE ${dbName}.${tableNamePrefix}_0 FORCE"

    // insert external data to table_2
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_2 VALUES ${values.join(",")}"

    sql "TRUNCATE TABLE ${dbName}.${tableNamePrefix}_3"

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

    for (def tableName in tables) {
        qt_sql "SELECT * FROM ${dbName}.${tableName} ORDER BY id"
    }

    // restore table_3 to new db
    sql """
        RESTORE SNAPSHOT ${dbName1}.${snapshotName}
        FROM `${repoName}`
        ON (${tableNamePrefix}_3)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "atomic_restore" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName1)

    qt_sql "SELECT * FROM ${dbName1}.${tableNamePrefix}_3 ORDER BY id"

    // add partition and insert some data.
    sql "ALTER TABLE ${dbName}.${tableNamePrefix}_3 ADD PARTITION p8 VALUES LESS THAN MAXVALUE"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_3 VALUES ${values.join(",")}"
    sql "INSERT INTO ${dbName}.${tableNamePrefix}_3 VALUES (200, 200)"

    // backup again
    snapshotName = "${snapshotName}_1"
    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
        ON (${tableNamePrefix}_3)
    """

    syncer.waitSnapshotFinish(dbName)

    snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    // restore with incremental data
    sql """
        RESTORE SNAPSHOT ${dbName1}.${snapshotName}
        FROM `${repoName}`
        ON (${tableNamePrefix}_3)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true",
            "atomic_restore" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName1)

    qt_sql "SELECT * FROM ${dbName1}.${tableNamePrefix}_3 ORDER BY id"

    for (def tableName in tables) {
        sql "DROP TABLE ${dbName}.${tableName} FORCE"
    }
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP DATABASE ${dbName1} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}


