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

suite("test_backup_restore_inverted_idx", "backup_restore") {
    String suiteName = "test_backup_restore_inverted_idx"
    String dbName = "${suiteName}_db"
    String repoName = "repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `value` STRING DEFAULT "",
            `value1` STRING DEFAULT "",
            INDEX `idx_value` (`value`) USING INVERTED PROPERTIES ("parser" = "english")
        )
        UNIQUE KEY(`id`)
        PARTITION BY RANGE(`id`)
        (
            PARTITION p1 VALUES LESS THAN ("10"),
            PARTITION p2 VALUES LESS THAN ("20"),
            PARTITION p3 VALUES LESS THAN ("30"),
            PARTITION p4 VALUES LESS THAN ("40"),
            PARTITION p5 VALUES LESS THAN ("50"),
            PARTITION p6 VALUES LESS THAN ("60"),
            PARTITION p7 VALUES LESS THAN ("70")
        )
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    List<String> values = []
    int numRows = 6;
    for (int j = 0; j <= numRows; ++j) {
        values.add("(${j}1, \"${j} ${j*10} ${j*100}\", \"${j*11} ${j*12}\")")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"

    def indexes = sql_return_maparray "SHOW INDEX FROM ${dbName}.${tableName}"
    logger.info("current indexes: ${indexes}")
    assertTrue(indexes.any { it.Key_name == "idx_value" && it.Index_type == "INVERTED" })

    def query_index_id = { indexName ->
        def res = sql_return_maparray "SHOW TABLETS FROM ${dbName}.${tableName}"
        def tabletId = res[0].TabletId
        res = sql_return_maparray "SHOW TABLET ${tabletId}"
        def dbId = res[0].DbId
        def tableId = res[0].TableId
        res = sql_return_maparray """ SHOW PROC "/dbs/${dbId}/${tableId}/indexes" """
        for (def record in res) {
            if (record.KeyName == indexName) {
                return record.IndexId
            }
        }
        throw new Exception("index ${indexName} is not exists")
    }

    try {
        sql """ ADMIN SET FRONTEND CONFIG ("restore_reset_index_id" = "false") """
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName}
            TO `${repoName}`
            ON (`${tableName}`)
        """

        syncer.waitSnapshotFinish(dbName)

        def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
        assertTrue(snapshot != null)

        def indexId = query_index_id("idx_value")
        logger.info("the exists index id is ${indexId}")

        sql "DROP TABLE ${dbName}.${tableName}"

        sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName}
            FROM `${repoName}`
            ON (`${tableName}`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true"
            )
        """

        syncer.waitAllRestoreFinish(dbName)

        indexes = sql_return_maparray "SHOW INDEX FROM ${dbName}.${tableName}"
        logger.info("current indexes: ${indexes}")
        assertTrue(indexes.any { it.Key_name == "idx_value" && it.Index_type == "INVERTED" })

        def newIndexId = query_index_id("idx_value")
        assertTrue(newIndexId == indexId, "old index id ${indexId}, new index id ${newIndexId}")

        // 1. query with inverted index
        sql """ set enable_match_without_inverted_index = false """
        def res = sql """ SELECT /*+ SET_VAR(inverted_index_skip_threshold = 0, enable_common_expr_pushdown = true) */ * FROM ${dbName}.${tableName} WHERE value MATCH_ANY "10" """
        assertTrue(res.size() > 0)

        // 2. add partition and query
        sql """ ALTER TABLE ${dbName}.${tableName} ADD PARTITION p8 VALUES LESS THAN ("80") """
        sql """ INSERT INTO ${dbName}.${tableName} VALUES (75, "75 750", "76 77") """
        res = sql """ SELECT /*+ SET_VAR(inverted_index_skip_threshold = 0, enable_common_expr_pushdown = true) */ * FROM ${dbName}.${tableName} WHERE value MATCH_ANY "75" """
        assertTrue(res.size() > 0)

        // 3. add new index
        sql """ ALTER TABLE ${dbName}.${tableName}
            ADD INDEX idx_value1(value1) USING INVERTED PROPERTIES("parser" = "english") """

        indexes = sql_return_maparray """ SHOW INDEX FROM ${dbName}.${tableName} """
        logger.info("current indexes: ${indexes}")
        assertTrue(indexes.any { it.Key_name == "idx_value1" && it.Index_type == "INVERTED" })

        // 4. drop old index
        sql """ ALTER TABLE ${dbName}.${tableName} DROP INDEX idx_value"""
        indexes = sql_return_maparray """ SHOW INDEX FROM ${dbName}.${tableName} """
        logger.info("current indexes: ${indexes}")
        assertFalse(indexes.any { it.Key_name == "idx_value" && it.Index_type == "INVERTED" })

        // 5. query new index with inverted idx
        sql """ INSERT INTO ${dbName}.${tableName} VALUES(76, "76 760", "12321 121") """
        sql """ BUILD INDEX idx_value1 ON ${dbName}.${tableName} """
        def build_index_finished = false
        for (int i = 0; i < 100; i++) {
            def build_status = sql_return_maparray """
                SHOW BUILD INDEX FROM ${dbName} WHERE TableName = "${tableName}" """
            if (!(build_status.any { it.State != 'FINISHED' })) {
                build_index_finished = true
                break
            }
            sleep(1000)
        }
        if (!build_index_finished) {
            def build_status = sql_return_maparray """
                SHOW BUILD INDEX FROM ${dbName} WHERE TableName = "${tableName}" """
            logger.info("the build index status: ${build_status}")
            assertTrue(false)
        }
        res = sql """ SELECT /*+ SET_VAR(inverted_index_skip_threshold = 0, enable_common_expr_pushdown = true) */ * FROM ${dbName}.${tableName} WHERE value1 MATCH_ANY "12321" """
        assertTrue(res.size() > 0)

    } finally {
        sql """ set enable_match_without_inverted_index = true """
        sql """ ADMIN SET FRONTEND CONFIG ("restore_reset_index_id" = "true") """
    }

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}

