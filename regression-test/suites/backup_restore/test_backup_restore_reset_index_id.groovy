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

suite("test_backup_restore_reset_index_id", "backup_restore") {
    String suiteName = "test_backup_restore_reset_index_id"
    String dbName = "${suiteName}_db"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String tableName = "${suiteName}_table"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

    sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"
    sql """
        CREATE TABLE if NOT EXISTS ${dbName}.${tableName}
        (
            `test` INT,
            `id` INT,
            `username` varchar(32) NULL DEFAULT "",
            `extra` varchar(32) NULL DEFAULT "",
            INDEX idx_ngrambf (`username`) USING NGRAM_BF PROPERTIES("bf_size" = "256", "gram_size" = "3"),
            INDEX idx_extra_ngram (`extra`) USING NGRAM_BF PROPERTIES("bf_size" = "256", "gram_size" = "3"),
            INDEX idx_extra_inverted (`extra`) USING INVERTED
        )
        ENGINE=OLAP
        DUPLICATE KEY(`test`, `id`)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        )
    """
    List<String> values = []
    int numRows = 10;
    for (int j = 0; j <= numRows; ++j) {
        values.add("(${j}, ${j}1, \"${j} ${j*10}\", \"index_test\")")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"

    def indexes = sql_return_maparray "SHOW INDEX FROM ${dbName}.${tableName}"
    logger.info("current indexes: ${indexes}")

    def query_table_index_id = { indexName ->
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

    def query_tablet_schema_index_id = { indexName ->
        def metaUrl = sql_return_maparray("SHOW TABLETS FROM ${dbName}.${tableName}").get(0).MetaUrl
        def (code, out, err) = curl("GET", metaUrl)
        assertEquals(code, 0)
        def jsonMeta = parseJson(out.trim())

        for (def index : jsonMeta.schema.index) {
            if (index.index_name == indexName) {
                return index.index_id
            }
        }
        throw new Exception("index ${indexName} is not exists")
    }

    def query_rowset_schema_index_id = { indexName ->
        def metaUrl = sql_return_maparray("SHOW TABLETS FROM ${dbName}.${tableName}").get(0).MetaUrl
        def (code, out, err) = curl("GET", metaUrl)
        assertEquals(code, 0)
        def jsonMeta = parseJson(out.trim())
        assertTrue(jsonMeta.rs_metas.size() > 0)

        for (def meta : jsonMeta.rs_metas) {
            for (def index : meta.tablet_schema.index) {
                if (index.index_name == indexName) {
                    return index.index_id
                }
            }
        }
        throw new Exception("index ${indexName} is not exists")
    }

    try {
        sql """ ADMIN SET FRONTEND CONFIG ("restore_reset_index_id" = "true") """
        sql """
            BACKUP SNAPSHOT ${dbName}.${snapshotName}
            TO `${repoName}`
            ON (`${tableName}`)
        """

        syncer.waitSnapshotFinish(dbName)

        def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
        assertTrue(snapshot != null)

        def oldIndexNgramBfId = query_table_index_id("idx_ngrambf")
        def oldIndexExtraNgram = query_table_index_id("idx_extra_ngram")
        logger.info("the exists index id of idx_ngrambf is ${oldIndexNgramBfId}")
        logger.info("the exists index id of idx_ngrambf is ${oldIndexExtraNgram}")

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

        def newIndexNgramBfId = query_table_index_id("idx_ngrambf")
        def newIndexExtraNgram = query_table_index_id("idx_extra_ngram")
        assertTrue(oldIndexNgramBfId != newIndexNgramBfId, "old index id ${oldIndexNgramBfId}, new index id ${newIndexNgramBfId}")
        assertTrue(oldIndexExtraNgram != newIndexExtraNgram, "old index id ${oldIndexExtraNgram}, new index id ${newIndexExtraNgram}")

        def indexNgramBfId = query_tablet_schema_index_id("idx_ngrambf")
        def indexExtraNgram = query_tablet_schema_index_id("idx_extra_ngram")
        assertEquals(indexNgramBfId, newIndexNgramBfId.toLong(), "tablet index id ${indexNgramBfId}, new index id ${newIndexNgramBfId}")
        assertEquals(indexExtraNgram, newIndexExtraNgram.toLong(), "tablet index id ${indexExtraNgram}, new index id ${newIndexExtraNgram}")

        indexNgramBfId = query_rowset_schema_index_id("idx_ngrambf")
        indexExtraNgram = query_rowset_schema_index_id("idx_extra_ngram")
        assertEquals(indexNgramBfId, newIndexNgramBfId.toLong(), "tablet index id ${indexNgramBfId}, new index id ${newIndexNgramBfId}")
        assertEquals(indexExtraNgram, newIndexExtraNgram.toLong(), "tablet index id ${indexExtraNgram}, new index id ${newIndexExtraNgram}")
    } finally {
        sql """ ADMIN SET FRONTEND CONFIG ("restore_reset_index_id" = "false") """
    }

    sql "DROP TABLE ${dbName}.${tableName} FORCE"
    sql "DROP DATABASE ${dbName} FORCE"
    sql "DROP REPOSITORY `${repoName}`"
}
