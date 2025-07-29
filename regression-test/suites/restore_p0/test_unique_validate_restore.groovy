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

suite("test_unique_validate_restore", "validate_restore") {
    def runUniqueValidateRestore = { String version ->
        String validateSuiteName = "test_unique_backup_restore"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String tableName = "test_unique_backup_restore_table"
        String snapshotName = "test_unique_backup_restore_snapshot"

        def syncer = getSyncer()
        String repoName = syncer.createS3ValidateRepository(validateSuiteName, version)

        sql "ADMIN SET FRONTEND CONFIG('experimental_enable_cloud_restore_job' = 'true');"
        try {
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"

            def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
            assertTrue(snapshot != null)

            sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName}
            FROM `${repoName}`
            ON ( `${tableName}`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true"
            )
            """
            syncer.waitAllRestoreFinish(dbName)

            def result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 2)

            sql "DROP REPOSITORY `${repoName}`"

            sql "DELETE FROM ${dbName}.${tableName} WHERE id = 1"
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 1)

            sql "DROP TABLE ${dbName}.${tableName} FORCE"

            repoName = syncer.createS3ValidateRepository(validateSuiteName + "_delete", version)
            snapshotName = "test_unique_delete_backup_restore_snapshot"

            snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
            assertTrue(snapshot != null)

            sql """
            RESTORE SNAPSHOT ${dbName}.${snapshotName}
            FROM `${repoName}`
            ON ( `${tableName}`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true"
            )
            """
            syncer.waitAllRestoreFinish(dbName)
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 1)

            sql """INSERT INTO ${dbName}.${tableName} VALUES (3, 3),(4, 4)"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 3)

            sql """INSERT INTO ${dbName}.${tableName} VALUES (5, 5),(6, 6)"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 5)

            sql """DELETE FROM ${dbName}.${tableName} WHERE id = 2"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 4)

            sql """DELETE FROM ${dbName}.${tableName} WHERE id = 5"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 3)

            qt_select "SELECT * FROM ${dbName}.${tableName} ORDER BY id"

        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runUniqueValidateRestore("3.0")
    //runUniqueValidateRestore("2.1")
}

