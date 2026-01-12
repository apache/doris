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

suite("test_validate_restore_multi_tables", "validate_restore") {
    def runValidateRestoreMultiTables = { String version ->
        String validateSuiteName = "test_backup_restore_multi_tables"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String snapshotName = "${validateSuiteName}_snapshot"
        String tableNamePrefix = "${validateSuiteName}_tables"

        def syncer = getSyncer()
        String repoName = syncer.createS3ValidateRepository(validateSuiteName, version)

        sql "ADMIN SET FRONTEND CONFIG('experimental_enable_cloud_restore_job' = 'true');"
        try {
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}"

            int numTables = 10;
            int numRows = 10;
            List<String> tables = []
            for (int i = 0; i < numTables; ++i) {
                String tableName = "${tableNamePrefix}_${i}"
                tables.add(tableName)
            }

            def restoreTables = tables[0..5]

            syncer.waitSnapshotFinish(dbName)

            def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
            assertTrue(snapshot != null)

            sql """
                RESTORE SNAPSHOT ${dbName}.${snapshotName}
                FROM `${repoName}`
                ON (${restoreTables.join(",")})
                PROPERTIES
                (
                    "backup_timestamp" = "${snapshot}",
                    "reserve_replica" = "true"
                )
            """

            syncer.waitAllRestoreFinish(dbName)

            for (def tableName in restoreTables) {
                def result = sql "SELECT * FROM ${dbName}.${tableName}"
                assertEquals(result.size(), numRows);
                sql "DROP TABLE ${dbName}.${tableName} FORCE"
            }

        } finally {
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreMultiTables("3.0")
}


