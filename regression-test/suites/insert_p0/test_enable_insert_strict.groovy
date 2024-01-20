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

import org.junit.jupiter.api.Assertions
import com.mysql.cj.jdbc.StatementImpl

suite("test_enable_insert_strict") {

    // utils
    def expect_insert_success = { sql ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("insert result: {}, server info: {}", result, serverInfo)
        return result
    }

    def expect_insert_fail = { sql ->
        try {
            expect_insert_success sql
            Assertions.fail(() -> "insert sql should fail. sql:\n${sql}".toString())
        } catch (java.sql.SQLException e) {
            logger.warn("insert error: {}", e.getMessage())
            return e
        }
    }
    // utils

    // runner
    def db_name = "regression_test_insert_p0"

    def cases_enable_insert_strict = []
    def cases_disable_insert_strict = []

    def run_all_cases = {
        sql """ set enable_insert_strict = true; """
        for (c in cases_enable_insert_strict) {
            c()
        }
        sql """ set enable_insert_strict = false; """
        for (c in cases_disable_insert_strict) {
            c()
        }
    }

    def test_in_single_column_table = { column_type, correct_values, incorrect_values ->
        def column_name = column_type.replace("(", "_").replace(",", "_").replace(")", "")
        def table_name = "${db_name}.insert_strict_single_column_${column_name}"

        sql """ DROP TABLE IF EXISTS ${table_name}; """

        sql """
            CREATE TABLE ${table_name} (
                `c_${column_name}` ${column_type} NOT NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`c_${column_name}`)
            DISTRIBUTED BY HASH(`c_${column_name}`) BUCKETS 1
            PROPERTIES (
            "replication_num" = "1"
            );
        """
        
        for (value in correct_values) {
            def insert_sql = """ INSERT INTO ${table_name} VALUES (${value}); """
            cases_enable_insert_strict.add({
                def result = expect_insert_success insert_sql
                assertEquals(1, result , () -> "insert row count error. sql:\n${insert_sql}".toString())
            })
            cases_disable_insert_strict.add({
                def result = expect_insert_success insert_sql
                assertEquals(1, result , () -> "insert row count error. sql:\n${insert_sql}".toString())
            })
        }
        
        for (value in incorrect_values) {
            def insert_sql = """ INSERT INTO ${table_name} VALUES (${value}); """
            cases_enable_insert_strict.add({
                def e = expect_insert_fail insert_sql
                assertTrue(e.getMessage().contains("Insert has filtered data in strict mode"), () -> "insert error message incorrect: ${e}".toString())
            })
            cases_disable_insert_strict.add({
                def result = expect_insert_success insert_sql
                assertEquals(0, result , () -> "insert row count error. sql:\n${insert_sql}".toString())
            })
        }
    }
    // runner

    // cases
    test_in_single_column_table(
        column_type="int",
        correct_values=["0", "1", ],
        incorrect_values=["'a'", "''", ],
    )
    // cases

    run_all_cases()
}
