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

suite("test_backup_restore_mv", "backup_restore") {
    String suiteName = "test_backup_restore_mv"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String dbName = "${suiteName}_db"
    String dbName1 = "${suiteName}_db_1"
    String tableName = "${suiteName}_table"
    String mvName = "${suiteName}_mv"
    String snapshotName = "${suiteName}_snapshot"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `item_id` LARGEINT NOT NULL)
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
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
    def result = sql "SELECT * FROM ${dbName}.${tableName}"
    assertEquals(result.size(), values.size());

    sql """DROP MATERIALIZED VIEW IF EXISTS ${dbName}.${mvName};"""
    sql """
        CREATE MATERIALIZED VIEW ${mvName}
        AS
        SELECT id, sum(item_id) FROM ${dbName}.${tableName} GROUP BY id;
    """

    def alter_finished = false
    for (int i = 0; i < 60 && !alter_finished; i++) {
        result = sql_return_maparray "SHOW ALTER TABLE MATERIALIZED VIEW FROM ${dbName}"
        logger.info("result: ${result}")
        for (int j = 0; j < result.size(); j++) {
            if (result[j]['TableName'] == "${tableName}" &&
                result[j]['RollupIndexName'] == "${mvName}" &&
                result[j]['State'] == 'FINISHED') {
                alter_finished = true
                break
            }
        }
        Thread.sleep(3000)
    }
    assertTrue(alter_finished);

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    sql "DROP DATABASE IF EXISTS ${dbName1}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName1}"

    sql """
        RESTORE SNAPSHOT ${dbName1}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName1)

    result = sql "SELECT * FROM ${dbName1}.${tableName}"
    assertEquals(result.size(), values.size());

    result = sql_return_maparray "DESC ${dbName1}.${tableName} ALL"
    logger.info("result: ${result}")
    def mv_existed = false
    for (int i = 0; i < result.size(); i++) {
        if (result[i]['IndexName'] == "${mvName}") {
            mv_existed = true
        }
    }
    assertTrue(mv_existed)

    sql "ANALYZE TABLE ${dbName1}.${tableName} WITH SYNC"

    def explain_result = sql """ EXPLAIN SELECT id, sum(item_id) FROM ${dbName1}.${tableName} GROUP BY id"""
    logger.info("explain result: ${explain_result}")
    // ATTN: RestoreJob will reset the src db name of OriginStatement of the MaterializedIndexMeta.
    assertTrue(explain_result.toString().contains("${dbName1}.${tableName}(${mvName})"))

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP TABLE ${dbName1}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP DATABASE ${dbName1} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

