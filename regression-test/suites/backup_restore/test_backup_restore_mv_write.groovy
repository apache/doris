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

suite("test_backup_restore_mv_write", "backup_restore") {
    String suiteName = "test_backup_restore_mv_write"
    String dbName = context.dbName
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "snapshot_${suiteName}"
    String tableNamePrefix = "tbl_${suiteName}"
    String viewName = "mv_${tableNamePrefix}"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)

    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    sql "DROP TABLE IF EXISTS ${tableNamePrefix}"
    sql "DROP materialized VIEW IF EXISTS ${viewName}"

    sql """
        CREATE TABLE `${tableNamePrefix}` (
        `k` int NOT NULL,
        `k1` datetime NOT NULL,
        `k2` datetime NOT NULL,
        `vin` varchar(128) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`k`, `k1`, `k2`, `vin`)
        DISTRIBUTED BY HASH(`k`) BUCKETS 5
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    createMV( """
            CREATE materialized VIEW ${viewName} AS SELECT
            DATE_FORMAT( date_add( k2, INTERVAL 1 HOUR ), '%Y-%m-%d %H:00:00' ) AS a1,
            vin as a2,
            count( 1 ) AS as a3 
            FROM
            ${tableNamePrefix} 
            WHERE
            k1 > '2025-06-12 00:00:00' 
            GROUP BY
            DATE_FORMAT( date_add( k2, INTERVAL 1 HOUR ), '%Y-%m-%d %H:00:00' ),
            vin
    """)

    def alter_finished = false
    for (int i = 0; i < 60 && !alter_finished; i++) {
        def result = sql_return_maparray "SHOW ALTER TABLE MATERIALIZED VIEW FROM ${dbName}"
        logger.info("result: ${result}")
        for (int j = 0; j < result.size(); j++) {
            if (result[j]['TableName'] == "${tableNamePrefix}" &&
                result[j]['RollupIndexName'] == "${viewName}" &&
                result[j]['State'] == 'FINISHED') {
                alter_finished = true
                break
            }
        }
        Thread.sleep(3000)
    }
    assertTrue(alter_finished);

    sql """
        BACKUP SNAPSHOT ${snapshotName}
        TO `${repoName}`
        ON (`${tableNamePrefix}`)
        PROPERTIES ("type" = "full")
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    def res0 = sql "desc ${tableNamePrefix} all"

    sql "DROP TABLE IF EXISTS ${tableNamePrefix}"

    sql """
        RESTORE SNAPSHOT ${snapshotName}
        FROM `${repoName}`
        ON ( `${tableNamePrefix}` )
        PROPERTIES
        (
            "backup_timestamp"="${snapshot}",
            "replication_num" = "1"
        );
    """

    syncer.waitAllRestoreFinish(dbName)

    def res = sql "desc ${tableNamePrefix} all"

    // DefineExpr and WhereClause should not be null after restore
    // desc table all:
    // IndexName: mv_t
    // IndexKeysType: AGG_KEYS
    // Field: mv_date_format(hours_add(k2, 1), '%Y-%m-%d %H:00:00')
    // Type: varchar(65533)
    // InternalType: varchar(65533)
    // Null: Yes
    // Key: true
    // Default: NULL
    // Extra: 
    // Visible: true
    // DefineExpr: date_format(hours_add(`k2`, 1), '%Y-%m-%d %H:00:00')
    // WhereClause: (`k1` > '2025-06-12 00:00:00')

    logger.info("res0: ${res0}")
    logger.info("res: ${res}")
    assertEquals(res0.toString(), res.toString())

    sql "insert into ${tableNamePrefix} values (1, '2025-06-12 01:00:00', '2025-06-12 02:00:00', 'test_vin_0')"
    sql "insert into ${tableNamePrefix} values (2, '2025-06-13 02:00:00', '2025-06-13 03:00:00', 'test_vin_1')"
    sql "insert into ${tableNamePrefix} values (3, '2025-06-14 03:00:00', '2025-06-14 04:00:00', 'test_vin_2')"

    def select_res = sql "select * from ${tableNamePrefix}"
    assertTrue(select_res.size() == 3)

    sql "DROP REPOSITORY `${repoName}`"
}