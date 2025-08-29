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

suite("test_validate_restore_diff_repo_same_snapshot", "validate_restore") {
    def runValidateRestoreDiffRepoSameSnapshot = { String version ->
        String validateSuiteName = "test_backup_restore_diff_repo_same_snapshot"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String tableName = "${validateSuiteName}_table"
        String snapshotName = "${validateSuiteName}_snapshot"

        def syncer = getSyncer()
        String repoName_1 = syncer.createS3ValidateRepository("${validateSuiteName}_1", version)
        String repoName_2 = syncer.createS3ValidateRepository("${validateSuiteName}_2", version)

        sql "ADMIN SET FRONTEND CONFIG('experimental_enable_cloud_restore_job' = 'true');"
        try {
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}_1"
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}_2"
            sql "DROP TABLE IF EXISTS ${dbName}_1.${tableName}_1"
            sql "DROP TABLE IF EXISTS ${dbName}_2.${tableName}_2"

            // Restore snapshot from repo_1 to db_1
            def snapshot = syncer.getSnapshotTimestamp("${repoName_1}", snapshotName)
            assertTrue(snapshot != null)

            sql """
                RESTORE SNAPSHOT ${dbName}_1.${snapshotName}
                FROM `${repoName_1}`
                ON ( `${tableName}_1`)
                PROPERTIES
                (
                    "backup_timestamp" = "${snapshot}",
                    "reserve_replica" = "true"
                )
            """

            syncer.waitAllRestoreFinish("${dbName}_1")

            def result = sql "SELECT * FROM ${dbName}_1.${tableName}_1"
            assertEquals(result.size(), 10);

            // Restore snapshot from repo_2 to db_2
            snapshot = syncer.getSnapshotTimestamp("${repoName_2}", snapshotName)
            assertTrue(snapshot != null)

            sql """
                RESTORE SNAPSHOT ${dbName}_2.${snapshotName}
                FROM `${repoName_2}`
                ON ( `${tableName}_2`)
                PROPERTIES
                (
                    "backup_timestamp" = "${snapshot}",
                    "reserve_replica" = "true"
                )
            """

            syncer.waitAllRestoreFinish("${dbName}_2")

            result = sql "SELECT * FROM ${dbName}_2.${tableName}_2"
            assertEquals(result.size(), 10);
        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}_1.${tableName}_1 FORCE"
            sql "DROP TABLE IF EXISTS ${dbName}_2.${tableName}_2 FORCE"
            sql "DROP DATABASE ${dbName}_1 FORCE"
            sql "DROP DATABASE ${dbName}_2 FORCE"
            sql "DROP REPOSITORY `${repoName_1}`"
            sql "DROP REPOSITORY `${repoName_2}`"
        }
    }

    runValidateRestoreDiffRepoSameSnapshot("3.0")
    runValidateRestoreDiffRepoSameSnapshot("2.1")
}
