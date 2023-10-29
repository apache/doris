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

import com.mysql.cj.jdbc.StatementImpl

suite("insert_group_commit_with_prepare_stmt_nereids") {
    def db = "regression_test_insert_p0"
    def table = "insert_group_commit_with_prepare_stmt_nereids"

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${table}"
            logger.info("rowCount: " + rowCount + ", retry: " + retry)
            if (rowCount[0][0] >= expectedRowCount) {
                break
            }
            retry++
        }
    }

    def group_commit_insert = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        if (result != expected_row_count) {
            logger.warn("insert result: " + result + ", expected_row_count: " + expected_row_count + ", sql: " + sql)
        }
        assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'PREPARE'"))
        assertTrue(serverInfo.contains("'label':'group_commit_"))
        assertTrue(serverInfo.contains("'optimizer':'nereids"))
    }

    try {
        // create table
        sql """ drop table if exists ${table}; """

        sql """
        CREATE TABLE `${table}` (
            `id` varchar(11),
            `name` varchar(1100) NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`, `name`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        );
        """

        connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
            sql """ set enable_nereids_dml = true; """
            sql """ set experimental_enable_nereids_planner=true; """
            sql """ set enable_insert_group_commit = true; """
            sql """ use ${db}; """

            // insert into 5 rows
            def insert_sql = """ insert into ${table} values('1', 'a')  """
            for (def i in 2..5) {
                insert_sql += """, ('${i}', 'a') """
            }
            group_commit_insert insert_sql, 5
            getRowCount(5)
        }
    } finally {
        // try_sql("DROP TABLE ${table}")
    }
}
