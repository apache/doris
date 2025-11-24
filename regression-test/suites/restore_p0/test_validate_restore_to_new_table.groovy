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

suite("test_validate_restore_to_new_table", "validate_restore") {
    def runValidateRestoreToNewTable = { String version ->
        String validateSuiteName = "test_restore_to_new_table"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String tableName = "${validateSuiteName}_table"
        String snapshotName = "test_backup_restore_snapshot"

        def syncer = getSyncer()
        String repoName = syncer.createS3ValidateRepository(validateSuiteName, version)

        sql "ADMIN SET FRONTEND CONFIG('experimental_enable_cloud_restore_job' = 'true');"
        try {
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"

            sql """
            CREATE TABLE ${dbName}.${tableName} (
                `id` LARGEINT NOT NULL,
                `count` LARGEINT SUM DEFAULT "0")
            AGGREGATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 2
            PROPERTIES
            (
                "replication_num" = "1"
            )
            """

            List<String> values = []
            for (int i = 1; i <= 10; ++i) {
                values.add("(${i}, ${i})")
            }
            sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"
            def result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), values.size());

            def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
            assertTrue(snapshot != null)

            sql "DROP TABLE ${dbName}.${tableName} FORCE"

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
            assertEquals(result.size(), values.size())

        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreToNewTable("3.0")
    //runValidateRestoreToNewTable("2.1")
}
