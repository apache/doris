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

import org.apache.doris.regression.util.ObjectStorageIamTestUtils

suite("test_external_catalog_iam") {
    def config = ObjectStorageIamTestUtils.getConfig(context.config.otherConfigs)
    if (config == null) {
        logger.info("skip ${name} because objectStorageIamProvider is not configured")
        return
    }

    def cases = config.authCases.collect { authCase ->
        return [
                name: authCase.name,
                warehouse: "${config.scheme}://${config.bucket}/${config.prefix}" +
                        "/test_external_catalog_iam/${authCase.name}",
                properties: """
                    ${authCase.storageSqlProperties}
                """
        ]
    }

    cases.eachWithIndex { testCase, index ->
        def randomStr = UUID.randomUUID().toString().replace("-", "")
        def catalogName = "iam_iceberg_${index}_${randomStr}"
        def databaseName = "iam_db"
        def tableName = "iam_table"

        logger.info("run ${name} with ${testCase.name}")
        try {
            sql """
                CREATE CATALOG ${catalogName} PROPERTIES (
                    "type" = "iceberg",
                    "iceberg.catalog.type" = "hadoop",
                    "warehouse" = "${testCase.warehouse}/${randomStr}",
                    ${testCase.properties}
                )
            """
            sql "CREATE DATABASE ${catalogName}.${databaseName}"
            def databases = sql "SHOW DATABASES FROM ${catalogName}"
            assertTrue(databases.any { it[0] == databaseName }, testCase.name)
            sql """
                CREATE TABLE ${catalogName}.${databaseName}.${tableName} (
                    id INT,
                    value STRING
                )
                PROPERTIES("file_format" = "parquet")
            """
            sql "INSERT INTO ${catalogName}.${databaseName}.${tableName} VALUES (1, 'one'), (2, 'two')"
            def result = sql "SELECT COUNT(*) FROM ${catalogName}.${databaseName}.${tableName}"
            assertEquals(2, result[0][0], testCase.name)
            def rows = sql "SELECT id, value FROM ${catalogName}.${databaseName}.${tableName} ORDER BY id"
            assertEquals([[1, 'one'], [2, 'two']], rows, testCase.name)
            sql "DROP TABLE ${catalogName}.${databaseName}.${tableName}"
            sql "DROP DATABASE ${catalogName}.${databaseName}"
            // Exercise namespace deletion and recreation through Hadoop FileSystem as well.
            sql "CREATE DATABASE ${catalogName}.${databaseName}"
            sql "DROP DATABASE ${catalogName}.${databaseName}"
        } finally {
            sql "DROP CATALOG IF EXISTS ${catalogName}"
        }
    }
}
