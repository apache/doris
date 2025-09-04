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

suite("test_validate_restore_inverted_idx", "validate_restore") {
    def runValidateRestoreInvertedIdx = { String version ->
        String validateSuiteName = "test_backup_restore_inverted_idx"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String snapshotName = "${validateSuiteName}_snapshot"
        String tableName = "${validateSuiteName}_table"

        def syncer = getSyncer()
        String repoName = syncer.createS3ValidateRepository(validateSuiteName, version)

        sql "ADMIN SET FRONTEND CONFIG('experimental_enable_cloud_restore_job' = 'true');"
        try {
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"

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

                def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
                assertTrue(snapshot != null)

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

                def indexes = sql_return_maparray "SHOW INDEX FROM ${dbName}.${tableName}"
                logger.info("current indexes: ${indexes}")
                assertTrue(indexes.any { it.Key_name == "idx_value" && it.Index_type == "INVERTED" })

                def newIndexId = query_index_id("idx_value")
                logger.info("new index id ${newIndexId}")

                // 1. query with inverted index
                sql """ set enable_match_without_inverted_index = false """
                sql """ set enable_fallback_on_missing_inverted_index = false """
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

                def add_index_finished = false
                for (int i = 0; i < 30; i++) {
                    indexes = sql_return_maparray """ SHOW INDEX FROM ${dbName}.${tableName} """
                    logger.info("current indexes: ${indexes}")
                    if (indexes.any { it.Key_name == "idx_value1" && it.Index_type == "INVERTED" }) {
                        add_index_finished = true
                        break
                    }
                    sleep(1000)
                }
                if (!add_index_finished) {
                    indexes = sql_return_maparray """ SHOW INDEX FROM ${dbName}.${tableName} """
                    logger.error("add index failed, current indexes: ${indexes}")
                    assertTrue(false)
                }

                // 4. drop old index
                sql """ ALTER TABLE ${dbName}.${tableName} DROP INDEX idx_value"""
                def drop_index_finished = false
                for (int i = 0; i < 30; i++) {
                    indexes = sql_return_maparray """ SHOW INDEX FROM ${dbName}.${tableName} """
                    logger.info("current indexes: ${indexes}")
                    if (!(indexes.any { it.Key_name == "idx_value" && it.Index_type == "INVERTED" })) {
                        drop_index_finished = true
                        break
                    }
                    sleep(1000)
                }
                if (!drop_index_finished) {
                    indexes = sql_return_maparray """ SHOW INDEX FROM ${dbName}.${tableName} """
                    logger.error("drop index failed, current indexes: ${indexes}")
                    assertTrue(false)
                }

                // 5. query new index with inverted idx
                sql """ INSERT INTO ${dbName}.${tableName} VALUES(76, "76 760", "12321 121") """
                if (!isCloudMode()) {
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
                }
                res = sql """ SELECT /*+ SET_VAR(inverted_index_skip_threshold = 0, enable_common_expr_pushdown = true) */ * FROM ${dbName}.${tableName} WHERE value1 MATCH_ANY "12321" """
                assertTrue(res.size() > 0)

            } finally {
                sql """ set enable_match_without_inverted_index = true """
                sql """ set enable_fallback_on_missing_inverted_index = true """
                sql """ ADMIN SET FRONTEND CONFIG ("restore_reset_index_id" = "true") """
            }
        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreInvertedIdx("3.0")
    runValidateRestoreInvertedIdx("2.1")
}


