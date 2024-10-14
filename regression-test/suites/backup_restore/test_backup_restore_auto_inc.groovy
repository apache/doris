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

suite("test_backup_restore_auto_inc", "backup_restore") {
    String suiteName = "test_backup_restore_auto_inc"
    String dbName = "${suiteName}_db"
    String dbName2 = "${suiteName}_db2"
    String dbName3 = "${suiteName}_db3"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String table1 = "${suiteName}_tbl1"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${table1} force"
    sql """
        CREATE TABLE ${dbName}.${table1} (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `val` int)
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES("replication_num" = "1")
        """
    sql """INSERT INTO ${dbName}.${table1}(val) select number from numbers("number"="5"); """
    qt_sql_origin "select count(distinct id) from ${dbName}.${table1};"


    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`"""
    syncer.waitSnapshotFinish(dbName)
    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)


    // 1. restore to a new table
    sql "CREATE DATABASE IF NOT EXISTS ${dbName2}"
    sql "DROP TABLE IF EXISTS ${dbName2}.${table1} FORCE;"
    sql """
        RESTORE SNAPSHOT ${dbName2}.${snapshotName}
        FROM `${repoName}`
        ON (`${table1}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )"""
    syncer.waitAllRestoreFinish(dbName2)
    qt_restore_to_new_table "select count(distinct id) from ${dbName2}.${table1};"
    sql """INSERT INTO ${dbName2}.${table1}(val) select number from numbers("number"="5"); """
    qt_restore_to_new_table "select count(distinct id) from ${dbName2}.${table1};"


    // 2. restore to existing table
    // 2.1 existing table has higher auto_increment next_id
    sql """INSERT INTO ${dbName}.${table1}(val) select number from numbers("number"="5"); """
    qt_before "select count(distinct id) from ${dbName}.${table1};"
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${table1}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    qt_restore_to_existing_table1 "select count(distinct id) from ${dbName}.${table1};"
    sql """INSERT INTO ${dbName}.${table1}(val) select number from numbers("number"="5"); """
    qt_restore_to_existing_table1 "select count(distinct id) from ${dbName}.${table1};"


    // 2.2 existing table has lower auto_increment next_id
    sql "DROP TABLE IF EXISTS ${dbName}.${table1} force"
    sql """
        CREATE TABLE ${dbName}.${table1} (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `val` int)
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES("replication_num" = "1")
        """
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        ON ( `${table1}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName)
    qt_restore_to_existing_table2 "select count(distinct id) from ${dbName}.${table1};"
    sql """INSERT INTO ${dbName}.${table1}(val) select number from numbers("number"="5"); """
    qt_restore_to_existing_table2 "select count(distinct id) from ${dbName}.${table1};"


    // 3. illegal case: restore a table with auto-increment column to a existing table without auto-increment column
    sql "DROP DATABASE IF EXISTS ${dbName3} FORCE"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName3}"
    sql "DROP TABLE IF EXISTS ${dbName3}.${table1} force"
    sql """
        CREATE TABLE ${dbName3}.${table1} (
            `id` BIGINT NOT NULL,
            `val` int)
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES("replication_num" = "1")
        """
    sql """
        RESTORE SNAPSHOT ${dbName3}.${snapshotName}
        FROM `${repoName}`
        ON ( `${table1}`)
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """
    syncer.waitAllRestoreFinish(dbName3)
    def res = sql_return_maparray "show restore from ${dbName3}"
    assertEquals(res.size(), 1)
    assertTrue(res[0].State.equals("CANCELLED"))
    assertTrue(res[0].Status.contains("Table test_backup_restore_auto_inc_tbl1 already exists but with different schema, local table doesn't have auto-increment column but remote table has"))

    sql "DROP TABLE IF EXISTS ${dbName}.${table1} FORCE"
    sql "DROP TABLE IF EXISTS ${dbName2}.${table1} FORCE"
    sql "DROP TABLE IF EXISTS ${dbName3}.${table1} force"
    sql "DROP DATABASE IF EXISTS ${dbName} FORCE"
    sql "DROP DATABASE IF EXISTS ${dbName2} FORCE"
    sql "DROP DATABASE IF EXISTS ${dbName3} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

