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

suite("test_validate_restore_reserve_dynamic_partition_false", "validate_restore") {
    def runValidateRestoreReserveDynamicPartitionFalse = { String version ->
        String validateSuiteName = "test_backup_restore_reserve_dynamic_partition_false"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String tableName = "dynamic_partition_reserve_false_table"
        String snapshotName = "test_backup_restore_dynamic_partition_reserve_true_snapshot"

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
                ON (`${tableName}`)
                PROPERTIES
                (
                    "backup_timestamp" = "${snapshot}",
                    "reserve_dynamic_partition_enable" = "false",
                    "reserve_replica" = "true"
                )
            """
            syncer.waitAllRestoreFinish(dbName)

            def restore_properties = sql "SHOW CREATE TABLE ${dbName}.${tableName}"

            assertTrue(restore_properties[0][1].contains("\"dynamic_partition.enable\" = \"false\""))

            def result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 20)

        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreReserveDynamicPartitionFalse("3.0")
    //runValidateRestoreReserveDynamicPartitionFalse("2.1")
}

