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

suite("test_validate_restore_inverted_index", "validate_restore") {
    def runValidateRestoreInvertedIndex = { String version ->
        String validateSuiteName = "test_backup_restore_inverted_index"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String tableName = "${validateSuiteName}_table"
        String snapshotName = "${validateSuiteName}_snapshot"

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
            ON ( `${tableName}`)
            PROPERTIES
            (
                "backup_timestamp" = "${snapshot}",
                "reserve_replica" = "true"
            )
            """

            syncer.waitAllRestoreFinish(dbName)

            def restore_index_comment = sql "SHOW CREATE TABLE ${dbName}.${tableName}"

            assertTrue(restore_index_comment[0][1].contains("USING INVERTED PROPERTIES(\"lower_case\" = \"true\", \"parser\" = \"english\", \"support_phrase\" = \"true\")"))

            def result = sql "SELECT id, TOKENIZE(comment,'\"parser\"=\"english\"') as token FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 2)

            result = sql"SELECT * FROM ${dbName}.${tableName} ORDER BY id"
            assertEquals(result.size(), 2)

            sql "INSERT INTO ${dbName}.${tableName} VALUES (3, 'hello world')"
            result = sql"SELECT * FROM ${dbName}.${tableName} ORDER BY id"
            assertEquals(result.size(), 3)

            sql "DELETE FROM ${dbName}.${tableName} WHERE id = 3"
            result = sql"SELECT * FROM ${dbName}.${tableName} ORDER BY id"
            assertEquals(result.size(), 2)
        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreInvertedIndex("3.0")
    //runValidateRestoreInvertedIndex("2.1")
}

