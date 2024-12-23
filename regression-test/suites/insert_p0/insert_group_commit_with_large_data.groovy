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

suite("insert_group_commit_with_large_data") {
    def db = "regression_test_insert_p0"
    def table = "insert_group_commit_with_large_data"

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
    }

    for (item in ["legacy", "nereids"]) { 
        try {
            // create table
            sql """ drop table if exists ${table}; """

            sql """
            CREATE TABLE `${table}` (
                `id` int(11) NOT NULL,
                `name` varchar(1100) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            );
            """

            connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
                sql """ set group_commit = async_mode; """
                if (item == "nereids") {
                    sql """ set enable_nereids_dml = true; """
                    sql """ set enable_nereids_planner=true; """
                    //sql """ set enable_fallback_to_original_planner=false; """
                } else {
                    sql """ set enable_nereids_dml = false; """
                }
                sql """ use ${db}; """

                // insert into 5000 rows
                def insert_sql = """ insert into ${table} values(1, 'a', 10)  """
                for (def i in 2..5000) {
                    insert_sql += """, (${i}, 'a', 10) """
                }
                group_commit_insert insert_sql, 5000
                getRowCount(5000)

                // data size is large than 4MB, need " set global max_allowed_packet = 5508950 "
                /*def name_value = ""
                for (def i in 0..1024) {
                    name_value += 'a'
                }
                insert_sql = """ insert into ${table} values(1, '${name_value}', 10)  """
                for (def i in 2..5000) {
                    insert_sql += """, (${i}, '${name_value}', 10) """
                }
                result = sql """ ${insert_sql} """
                group_commit_insert insert_sql, 5000
                getRowCount(10000)
                */
            }
        } finally {
            // try_sql("DROP TABLE ${table}")
        }
    }
}
