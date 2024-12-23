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

suite("insert_group_commit_into_max_filter_ratio") {
    def dbName = "regression_test_insert_p0"
    def tableName = "insert_group_commit_into_max_filter_ratio"
    def dbTableName = dbName + "." + tableName

    def get_row_count = { expectedRowCount ->
        def rowCount = sql "select count(*) from ${dbTableName}"
        logger.info("rowCount: " + rowCount + ", expecedRowCount: " + expectedRowCount)
        assertEquals(expectedRowCount, rowCount[0][0])
    }

    def get_row_count_with_retry = { expectedRowCount ->
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

    def off_mode_group_commit_insert = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        def result = stmt.executeUpdate()
        logger.info("insert result: " + result)
        def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
        logger.info("result server info: " + serverInfo)
        if (result != expected_row_count) {
            logger.warn("insert result: " + result + ", expected_row_count: " + expected_row_count + ", sql: " + sql)
        }
        // assertEquals(result, expected_row_count)
        assertTrue(serverInfo.contains("'status':'VISIBLE'"))
        assertFalse(serverInfo.contains("'label':'group_commit_"))
    }

    def fail_group_commit_insert = { sql, expected_row_count ->
        def stmt = prepareStatement """ ${sql}  """
        try {
            def result = stmt.executeUpdate()
            logger.info("insert result: " + result)
            def serverInfo = (((StatementImpl) stmt).results).getServerInfo()
            logger.info("result server info: " + serverInfo)
            if (result != expected_row_count) {
                logger.warn("insert result: " + result + ", expected_row_count: " + expected_row_count + ", sql: " + sql)
            }
            // assertEquals(result, expected_row_count)
            assertTrue(serverInfo.contains("'status':'ABORTED'"))
            // assertFalse(serverInfo.contains("'label':'group_commit_"))
        } catch (Exception e) {
            logger.info("exception: " + e)
        }
    }

    def check_stream_load_result = { exception, result, total_rows, loaded_rows, filtered_rows, unselected_rows ->
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

    def check_stream_load_result_with_exception = { exception, result, total_rows, loaded_rows, filtered_rows, unselected_rows ->
        if (exception != null) {
            throw exception
        }
        log.info("Stream load result: ${result}".toString())
        def json = parseJson(result)
        assertEquals("fail", json.Status.toLowerCase())
        assertTrue(json.GroupCommit)
        // assertTrue(json.Label.startsWith("group_commit_"))
        assertEquals(total_rows, json.NumberTotalRows)
        assertEquals(0, json.NumberLoadedRows)
        assertEquals(filtered_rows, json.NumberFilteredRows)
        assertEquals(unselected_rows, json.NumberUnselectedRows)
        if (filtered_rows > 0) {
            assertFalse(json.ErrorURL.isEmpty())
        } else {
            assertTrue(json.ErrorURL == null || json.ErrorURL.isEmpty())
        }
        assertTrue(json.Message.contains("too many filtered rows"))
    }

    def check_off_mode_stream_load_result = { exception, result, total_rows, loaded_rows, filtered_rows, unselected_rows ->
        if (exception != null) {
            throw exception
        }
        log.info("Stream load result: ${result}".toString())
        def json = parseJson(result)
        assertEquals("success", json.Status.toLowerCase())
        assertFalse(json.Label.startsWith("group_commit_"))
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

    // create table
    sql """ drop table if exists ${tableName}; """
    sql """
        CREATE TABLE ${tableName} (
            `id` int(11) NOT NULL,
            `type` varchar(1) NULL,
            `score` int(11) NULL default "-1"
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "group_commit_interval_ms" = "1000"
        );
    """

    // insert
    // legacy, nereids
    // if enable strict mode
    // 100 rows(success, fail), 10000 rows(success, fail), 15000 rows(success, fail)
    // async mode, sync mode, off mode
    for (item in ["legacy", "nereids"]) {
        sql """ truncate table ${tableName} """
        connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
            if (item == "nereids") {
                sql """ set enable_nereids_dml = true; """
                sql """ set enable_nereids_planner=true; """
                // sql """ set enable_fallback_to_original_planner=false; """
            } else {
                sql """ set enable_nereids_dml = false; """
            }

            sql """ set group_commit = sync_mode; """
            group_commit_insert """ insert into ${dbTableName} values (1, 'a', 10); """, 1
            sql """ set group_commit = async_mode; """
            group_commit_insert """ insert into ${dbTableName}(id) select 2; """, 1
            sql """ set group_commit = off_mode; """
            off_mode_group_commit_insert """ insert into ${dbTableName} values (3, 'a', 10); """, 1
            sql """ set group_commit = async_mode; """
            fail_group_commit_insert """ insert into ${dbTableName} values (4, 'abc', 10); """, 0
            sql """ set enable_insert_strict = false; """
            group_commit_insert """ insert into ${dbTableName} values (5, 'abc', 10); """, 0

            // The row 6 and 7 is different between legacy and nereids
            try {
                sql """ set group_commit = off_mode; """
                sql """ set enable_insert_strict = true; """
                sql """ insert into ${dbTableName} values (6, 'a', 'a'); """
            } catch (Exception e) {
                logger.info("exception: " + e)
                assertTrue(e.toString().contains("Invalid number format"))
            }

            try {
                sql """ set group_commit = off_mode; """
                sql """ set enable_insert_strict = false; """
                sql """ insert into ${dbTableName} values (7, 'a', 'a'); """
            } catch (Exception e) {
                logger.info("exception: " + e)
                assertTrue(e.toString().contains("Invalid number format"))
            }

            // TODO should throw exception?
            sql """ set group_commit = async_mode; """
            sql """ set enable_insert_strict = true; """
            fail_group_commit_insert """ insert into ${dbTableName} values (8, 'a', 'a'); """, 0

            sql """ set group_commit = async_mode; """
            sql """ set enable_insert_strict = false; """
            group_commit_insert """ insert into ${dbTableName} values (9, 'a', 'a'); """, 1
        }
        if (item == "nereids") {
            get_row_count_with_retry(6)
        } else {
            get_row_count_with_retry(4)
        }
        order_qt_sql """ select * from ${dbTableName} """
    }
    sql """ truncate table ${tableName} """

    // 2. stream load(async or sync mode, strict mode, max_filter_ratio, 10000 rows)
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        file "test_group_commit_10.csv"
        unset 'label'
        set 'group_commit', 'async_mode'

        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            check_stream_load_result(exception, result, 4, 4, 0, 0)
        }
    }
    get_row_count_with_retry(4)

    // sync_mode, strict_mode = true, max_filter_ratio = 0
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        file "test_group_commit_10.csv"
        unset 'label'
        set 'group_commit', 'sync_mode'
        set 'strict_mode', 'true'

        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            check_stream_load_result_with_exception(exception, result, 4, 3, 1, 0)
        }
    }
    get_row_count(4)

    // sync_mode, strict_mode = true, max_filter_ratio = 0.3
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        file "test_group_commit_10.csv"
        unset 'label'
        set 'group_commit', 'sync_mode'
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '0.3'

        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            check_stream_load_result(exception, result, 4, 3, 1, 0)
        }
    }
    get_row_count(7)

    order_qt_sql """ select * from ${tableName} """

    // 10001 rows
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        file "test_group_commit_11.csv.gz"
        unset 'label'
        set 'compress_type', 'gz'
        set 'group_commit', 'sync_mode'
        set 'strict_mode', 'true'

        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            check_stream_load_result(exception, result, 10001, 10000, 1, 0)
        }
    }
    get_row_count(10007)
    sql """ truncate table ${tableName} """

    // 3. http stream(async or sync mode, strict mode, max_filter_ratio, 10000 rows)
    streamLoad {
        set 'version', '1'
        set 'sql', """
                    insert into ${dbTableName} select * from http_stream
                    ("format"="csv", "column_separator"=",")
                """
        set 'group_commit', 'sync_mode'
        file "test_group_commit_10.csv"
        unset 'label'

        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            check_stream_load_result(exception, result, 4, 4, 0, 0)
        }
    }
    get_row_count_with_retry(4)

    // not use group commit
    streamLoad {
        set 'version', '1'
        set 'sql', """
                    insert into ${dbTableName} select * from http_stream
                    ("format"="csv", "column_separator"=",")
                """
        file "test_group_commit_10.csv"

        time 10000 // limit inflight 10s

        check { result, exception, startTime, endTime ->
            check_off_mode_stream_load_result(exception, result, 4, 4, 0, 0)
        }
    }
    get_row_count(8)

    order_qt_sql """ select * from ${tableName} """
}
