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

suite("test_upgrade_lower_case_catalog", "p0,external,doris,external_docker,external_docker_doris") {

    test {
        sql """show databases from test_upgrade_lower_case_catalog"""

        // Verification results include external_test_lower and external_test_UPPER
        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["upgrade_lower_case_catalog_lower", "upgrade_lower_case_catalog_upper"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    test {
        sql """show tables from test_upgrade_lower_case_catalog.upgrade_lower_case_catalog_lower"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower", "upper"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    qt_sql_test_upgrade_lower_case_catalog_1 "select * from test_upgrade_lower_case_catalog.upgrade_lower_case_catalog_lower.lower"
    qt_sql_test_upgrade_lower_case_catalog_2 "select * from test_upgrade_lower_case_catalog.upgrade_lower_case_catalog_lower.upper"

}