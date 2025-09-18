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

suite("test_validate_restore_dup_without_default_keys", "validate_restore") {
    def runValidateRestoreDupWithoutDefaultKeys = { String version ->
        String validateSuiteName = "test_backup_restore_dup_without_default_keys"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String tableName = "dup_without_keys_table"
        String snapshotName = "${validateSuiteName}_snapshot"

        // backup & restore for duplicate without keys by default
        def syncer = getSyncer()
        String repoName = syncer.createS3ValidateRepository(validateSuiteName, version)

        sql "ADMIN SET FRONTEND CONFIG('experimental_enable_cloud_restore_job' = 'true');"
        try {
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"

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
            def result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 10);

            sql """INSERT INTO ${dbName}.${tableName} VALUES (11, 11),(12, 12)"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 12);

            sql """DELETE FROM ${dbName}.${tableName} WHERE id = 11"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 11);

            qt_select "SELECT * FROM ${dbName}.${tableName} ORDER BY id"
        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreDupWithoutDefaultKeys("3.0")
    //runValidateRestoreDupWithoutDefaultKeys("2.1")
}

