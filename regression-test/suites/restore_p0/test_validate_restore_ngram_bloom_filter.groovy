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

suite("test_validate_restore_ngram_bloom_filter", "validate_restore") {
    def runValidateRestoreNgramBloomFilter = { String version ->
        String validateSuiteName = "test_backup_restore_ngram_bloom_filter"
        String dbName = "${validateSuiteName}_db_${version.replace('.', '_')}"
        String tableName = "${validateSuiteName}_table"
        String snapshotName = "${validateSuiteName}_snapshot"

        def syncer = getSyncer()
        String repoName = syncer.createS3ValidateRepository(validateSuiteName, version)

        sql "ADMIN SET FRONTEND CONFIG('experimental_enable_cloud_restore_job' = 'true');"
        try {
            sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName}"

            def checkNgramBf = { inputRes ->
                Boolean
                for (List<Object> row : inputRes) {
                    if (row[2] == "idx_ngrambf" && row[10] == "NGRAM_BF") {
                        return true
                    }
                }
                return false
            }
            def checkBloomFilter = { inputRes ->
                Boolean
                for (List<Object> row : inputRes) {
                    if ((row[1] as String).contains("\"bloom_filter_columns\" = \"id\"")) {
                        return true
                    }
                }
                return false
            }

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

            def res = sql "SHOW INDEXES FROM ${dbName}.${tableName}"
            assertTrue(checkNgramBf(res));
            res = sql "SHOW CREATE TABLE ${dbName}.${tableName}"
            assertTrue(checkBloomFilter(res));

            sql """
            ALTER TABLE ${dbName}.${tableName}
            ADD INDEX idx_only4test(`only4test`) USING NGRAM_BF PROPERTIES("gram_size"="3", "bf_size"="256")
            """
            def checkNgramBf1 = { inputRes ->
                Boolean
                for (List<Object> row : inputRes) {
                    if (row[2] == "idx_only4test" && row[10] == "NGRAM_BF") {
                        return true
                    }
                }
                return false
            }

            int count = 0;
            while (true) {
                res = sql "SHOW INDEXES FROM ${dbName}.${tableName}"
                if (checkNgramBf1(res)) {
                    break
                }
                count += 1;
                if (count >= 30) {
                    throw new IllegalStateException("alter table add index timeouted")
                }
                Thread.sleep(1000);
            }

            def result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 10)

            sql """INSERT INTO ${dbName}.${tableName} VALUES (100, 100, "test_100", "100_test")"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 11)

            sql """DELETE FROM ${dbName}.${tableName} WHERE id = 100"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 10)

            snapshotName = "${snapshotName}_1"

            snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
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

            res = sql "SHOW INDEXES FROM ${dbName}.${tableName}"
            assertTrue(checkNgramBf1(res));

            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 10)

            sql """INSERT INTO ${dbName}.${tableName} VALUES (100, 100, "test_100", "100_test")"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 11)

            sql """DELETE FROM ${dbName}.${tableName} WHERE id = 100"""
            result = sql "SELECT * FROM ${dbName}.${tableName}"
            assertEquals(result.size(), 10)

        } finally {
            sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
            sql "DROP DATABASE ${dbName} FORCE"
            sql "DROP REPOSITORY `${repoName}`"
        }
    }

    runValidateRestoreNgramBloomFilter("3.0")
    //runValidateRestoreNgramBloomFilter("2.1")
}

