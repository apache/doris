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

suite("test_validate_restore_mv", "validate_restore") {
    def runValidateRestoreMv = { String version ->
        String validateSuiteName = "test_backup_restore_mv"
        String dbName1 = "${validateSuiteName}_db_1_${version.replace('.', '_')}"
        String tableName = "${validateSuiteName}_table"
        String mvName = "${validateSuiteName}_mv"
        String snapshotName = "${validateSuiteName}_snapshot"

        def syncer = getSyncer()
        String repoName = syncer.createS3ValidateRepository(validateSuiteName, version)

        def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
        assertTrue(snapshot != null)

        sql "ADMIN SET FRONTEND CONFIG('experimental_enable_cloud_restore_job' = 'true');"
        try {

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

            def result = sql "SELECT * FROM ${dbName1}.${tableName}"
            assertEquals(result.size(), 10);

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

        } finally {
            sql "DROP TABLE IF EXISTS ${dbName1}.${tableName} FORCE"
            sql "DROP DATABASE ${dbName1} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreMv("3.0")
    runValidateRestoreMv("2.1")
}


