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

suite("test_validate_restore_with_view", "validate_restore") {
    def runValidateRestoreWithView = { String version ->
        String validateSuiteName = "backup_restore_with_view"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String dbName1 = "${validateSuiteName}_db_1_${version.replace('.', '_')}"
        String snapshotName = "${validateSuiteName}_snapshot"
        String tableName = "${validateSuiteName}_table"
        String viewName = "${validateSuiteName}_view"

        def syncer = getSyncer()
        String repoName = syncer.createS3ValidateRepository(validateSuiteName, version)

        sql "ADMIN SET FRONTEND CONFIG('experimental_enable_cloud_restore_job' = 'true');"
        try {
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
            sql "CREATE DATABASE IF NOT EXISTS ${dbName1}"

            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP VIEW IF EXISTS ${dbName}.${viewName}"

            def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
            assertTrue(snapshot != null)

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

            qt_sql "SELECT * FROM ${dbName1}.${tableName} ORDER BY id ASC"
            qt_sql "SELECT * FROM ${dbName1}.${viewName} ORDER BY id ASC"

            def show_view_result = sql_return_maparray "SHOW VIEW FROM ${tableName} FROM ${dbName1}"
            logger.info("show view result: ${show_view_result}")
            assertTrue(show_view_result.size() == 1);
            def show_view = show_view_result[0]['Create View']
            assertTrue(show_view.contains("${dbName1}"))
            assertTrue(show_view.contains("${tableName}"))

            // restore to db, test the view signature.
            sql """
                RESTORE SNAPSHOT ${dbName}.${snapshotName}
                FROM `${repoName}`
                PROPERTIES
                (
                    "backup_timestamp" = "${snapshot}",
                    "reserve_replica" = "true"
                )
            """
            syncer.waitAllRestoreFinish(dbName)

            def restore_result = sql_return_maparray """ SHOW RESTORE FROM ${dbName} WHERE Label ="${snapshotName}" """
            restore_result.last()
            logger.info("show restore result: ${restore_result}")
            assertTrue(restore_result.last().State == "FINISHED")

            sql "DROP TABLE ${dbName1}.${tableName} FORCE"
            sql "DROP VIEW ${dbName1}.${viewName}"

            // restore to db1, test the view signature.
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

            restore_result = sql_return_maparray """ SHOW RESTORE FROM ${dbName1} WHERE Label ="${snapshotName}" """
            restore_result.last()
            logger.info("show restore result: ${restore_result}")
            assertTrue(restore_result.last().State == "FINISHED")

        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP VIEW IF EXISTS ${dbName}.${viewName}"
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP TABLE IF EXISTS ${dbName1}.${tableName} FORCE"
            sql "DROP VIEW IF EXISTS ${dbName1}.${viewName}"
            sql "DROP DATABASE ${dbName1} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreWithView("3.0")
    //runValidateRestoreWithView("2.1")
}
