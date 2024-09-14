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

suite("insert_group_commit_into_unique") {
    def dbName = "regression_test_insert_p0"
    def tableName = "insert_group_commit_into_unique"
    def dbTableName = dbName + "." + tableName

    def getRowCount = { expectedRowCount ->
        def retry = 0
        while (retry < 30) {
            sleep(2000)
            def rowCount = sql "select count(*) from ${dbTableName}"
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
        // assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'PREPARE'"))
        assertTrue(serverInfo.contains("'label':'group_commit_"))
    }

    def checkStreamLoadResult = { exception, result, total_rows, loaded_rows, filtered_rows, unselected_rows ->
        if (exception != null) {
            throw exception
        }
        log.info("Stream load result: ${result}".toString())
        def json = parseJson(result)
        assertEquals("success", json.Status.toLowerCase())
        assertTrue(json.GroupCommit)
        assertTrue(json.Label.startsWith("group_commit_"))
        assertEquals(total_rows, json.NumberTotalRows)
        assertEquals(loaded_rows, json.NumberLoadedRows)
        assertEquals(filtered_rows, json.NumberFilteredRows)
        assertEquals(unselected_rows, json.NumberUnselectedRows)
        if (filtered_rows > 0) {
            assertFalse(json.ErrorURL.isEmpty())
        } else {
            assertTrue(json.ErrorURL == null || json.ErrorURL.isEmpty())
        }
    }

    for (item in ["legacy", "nereids"]) {
        // 1. table without sequence column
        try {
            tableName = "insert_group_commit_into_unique" + "1_" + item
            dbTableName = dbName + "." + tableName
            // create table
            sql """ drop table if exists ${dbTableName}; """

            sql """
            CREATE TABLE ${dbTableName} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            UNIQUE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "group_commit_interval_ms" = "500"
            );
            """

            // 1. insert into
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
                sql """ set group_commit = async_mode; """
                if (item == "nereids") {
                    sql """ set enable_nereids_planner=true; """
                    sql """ set enable_fallback_to_original_planner=false; """
                } else {
                    sql """ set enable_nereids_planner = false; """
                }

                group_commit_insert """ insert into ${dbTableName} values (1, 'a', 10),(5, 'q', 50); """, 2
                group_commit_insert """ insert into ${dbTableName}(id) values(6); """, 1
                group_commit_insert """ insert into ${dbTableName}(id) values(4);  """, 1
                group_commit_insert """ insert into ${dbTableName}(name, id) values('c', 3);  """, 1
                group_commit_insert """ insert into ${dbTableName}(id, name) values(2, 'b'); """, 1
                group_commit_insert """ insert into ${dbTableName}(id, name, score, __DORIS_DELETE_SIGN__) values(1, 'a', 10, 1) """, 1

                /*getRowCount(5)
                qt_sql """ select * from ${dbTableName} order by id, name, score asc; """*/
            }

            // 2. stream load
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'group_commit', 'async_mode'
                set 'columns', 'id, name, score'
                file "test_group_commit_1.csv"
                unset 'label'

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    checkStreamLoadResult(exception, result, 4, 4, 0, 0)
                }
            }
            /*getRowCount(9)
            qt_sql """ select * from ${dbTableName} order by id, name, score asc; """*/

            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'group_commit', 'async_mode'
                set 'columns', 'id, name, score, __DORIS_DELETE_SIGN__'
                file "test_group_commit_2.csv"
                unset 'label'

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    checkStreamLoadResult(exception, result, 5, 5, 0, 0)
                }
            }
            getRowCount(12)
            sql """ set show_hidden_columns = true """
            qt_sql """ select id, name, score, __DORIS_DELETE_SIGN__ from ${dbTableName} order by id, name, score asc; """
            sql """ set show_hidden_columns = false """
            qt_sql """ select id, name, score, __DORIS_DELETE_SIGN__ from ${dbTableName} order by id, name, score asc; """
        } finally {
            // try_sql("DROP TABLE ${dbTableName}")
        }

        // 2. table with "function_column.sequence_col"
        try {
            tableName = "insert_group_commit_into_unique" + "2_" + item
            dbTableName = dbName + "." + tableName
            // create table
            sql """ drop table if exists ${dbTableName}; """

            sql """
            CREATE TABLE ${dbTableName} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            UNIQUE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.sequence_col" = "score",
                "group_commit_interval_ms" = "500"
            );
            """

            // 1. insert into
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
                sql """ set group_commit = async_mode; """
                if (item == "nereids") {
                    sql """ set enable_nereids_planner=true; """
                    sql """ set enable_fallback_to_original_planner=false; """
                } else {
                    sql """ set enable_nereids_planner = false; """
                }

                group_commit_insert """ insert into ${dbTableName} values (1, 'a', 10),(5, 'q', 50); """, 2
                group_commit_insert """ insert into ${dbTableName}(id, score) values(6, 60); """, 1
                group_commit_insert """ insert into ${dbTableName}(id, score) values(4, 70);  """, 1
                group_commit_insert """ insert into ${dbTableName}(name, id, score) values('c', 3, 30);  """, 1
                group_commit_insert """ insert into ${dbTableName}(score, id, name) values(30, 2, 'b'); """, 1
                group_commit_insert """ insert into ${dbTableName}(id, name, score, __DORIS_DELETE_SIGN__) values(1, 'a', 10, 1) """, 1

                /*getRowCount(5)
                qt_sql """ select * from ${dbTableName} order by id, name, score asc; """*/
            };

            // 2. stream load
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'group_commit', 'async_mode'
                set 'columns', 'id, name, score'
                file "test_group_commit_1.csv"
                unset 'label'

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    checkStreamLoadResult(exception, result, 4, 4, 0, 0)
                }
            }
            /*getRowCount(9)
            qt_sql """ select * from ${dbTableName} order by id, name, score asc; """*/

            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'group_commit', 'async_mode'
                set 'columns', 'id, name, score, __DORIS_DELETE_SIGN__'
                file "test_group_commit_2.csv"
                unset 'label'

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    checkStreamLoadResult(exception, result, 5, 5, 0, 0)
                }
            }
            getRowCount(12)
            sql """ set show_hidden_columns = true """
            qt_sql """ select id, name, score, __DORIS_SEQUENCE_COL__, __DORIS_DELETE_SIGN__ from ${dbTableName} order by id, name, score asc; """
            sql """ set show_hidden_columns = false """
            qt_sql """ select id, name, score, __DORIS_SEQUENCE_COL__, __DORIS_DELETE_SIGN__ from ${dbTableName} order by id, name, score asc; """
        } finally {
            // try_sql("DROP TABLE ${dbTableName}")
            sql """ set show_hidden_columns = false """
        }

        // 3. table with "function_column.sequence_type"
        try {
            tableName = "insert_group_commit_into_unique" + "3_" + item
            dbTableName = dbName + "." + tableName
            // create table
            sql """ drop table if exists ${dbTableName}; """

            sql """
            CREATE TABLE ${dbTableName} (
                `id` int(11) NOT NULL,
                `name` varchar(50) NULL,
                `score` int(11) NULL default "-1"
            ) ENGINE=OLAP
            UNIQUE KEY(`id`, `name`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "function_column.sequence_type" = "int",
                "group_commit_interval_ms" = "500"
            );
            """

            // 1. insert into
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = context.config.jdbcUrl) {
                sql """ set group_commit = async_mode; """
                if (item == "nereids") {
                    sql """ set enable_nereids_planner=true; """
                    sql """ set enable_fallback_to_original_planner=false; """
                } else {
                    sql """ set enable_nereids_planner = false; """
                }

                group_commit_insert """ insert into ${dbTableName}(id, name, score, __DORIS_SEQUENCE_COL__) values (1, 'a', 10, 100),(5, 'q', 50, 500); """, 2
                group_commit_insert """ insert into ${dbTableName}(id, score, __DORIS_SEQUENCE_COL__) values(6, 60, 600); """, 1
                group_commit_insert """ insert into ${dbTableName}(id, score, __DORIS_SEQUENCE_COL__) values(6, 50, 500);  """, 1
                group_commit_insert """ insert into ${dbTableName}(name, id, score, __DORIS_SEQUENCE_COL__) values('c', 3, 30, 300);  """, 1
                group_commit_insert """ insert into ${dbTableName}(score, id, name, __DORIS_SEQUENCE_COL__) values(30, 2, 'b', 200); """, 1
                group_commit_insert """ insert into ${dbTableName}(id, name, score, __DORIS_DELETE_SIGN__, __DORIS_SEQUENCE_COL__) values(1, 'a', 200, 1, 200) """, 1
                group_commit_insert """ insert into ${dbTableName}(score, id, name, __DORIS_SEQUENCE_COL__, __DORIS_DELETE_SIGN__) values(30, 2, 'b', 100, 1); """, 1

                /*getRowCount(4)
                qt_sql """ select * from ${dbTableName} order by id, name, score asc; """*/
            };

            // 2. stream load
            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'group_commit', 'async_mode'
                set 'columns', 'id, name, score, __DORIS_SEQUENCE_COL__'
                set 'function_column.sequence_col', '__DORIS_SEQUENCE_COL__'
                file "test_group_commit_3.csv"
                unset 'label'

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    checkStreamLoadResult(exception, result, 4, 4, 0, 0)
                }
            }
            /*getRowCount(9)
            qt_sql """ select * from ${dbTableName} order by id, name, score asc; """*/

            streamLoad {
                table "${tableName}"

                set 'column_separator', ','
                set 'group_commit', 'async_mode'
                set 'columns', 'id, name, score, __DORIS_SEQUENCE_COL__, __DORIS_DELETE_SIGN__'
                set 'function_column.sequence_col', '__DORIS_SEQUENCE_COL__'
                file "test_group_commit_4.csv"
                unset 'label'

                time 10000 // limit inflight 10s

                check { result, exception, startTime, endTime ->
                    checkStreamLoadResult(exception, result, 7, 7, 0, 0)
                }
            }
            getRowCount(10)
            sql """ set show_hidden_columns = true """
            qt_sql """ select id, name, score, __DORIS_SEQUENCE_COL__, __DORIS_DELETE_SIGN__ from ${dbTableName} order by id, name, score asc; """
            sql """ set show_hidden_columns = false """
            qt_sql """ select id, name, score, __DORIS_SEQUENCE_COL__, __DORIS_DELETE_SIGN__ from ${dbTableName} order by id, name, score asc; """
        } finally {
            // try_sql("DROP TABLE ${dbTableName}")
            sql """ set show_hidden_columns = false """
        }
    }
}
