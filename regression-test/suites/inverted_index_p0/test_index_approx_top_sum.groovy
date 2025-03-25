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


suite("test_index_approx_top_sum", "p0"){
    def tableName = "test_index_approx_top_sum"

    sql "DROP TABLE IF EXISTS ${tableName}"

    def create_table = {table_name ->
      sql """
        CREATE TABLE ${table_name} (
          `@timestamp` int(11) NULL COMMENT "",
          `clientip` text NULL COMMENT "",
          `request` text NULL COMMENT "",
          `status` int NULL COMMENT "",
          `size` int NULL COMMENT ""
        ) ENGINE=OLAP
        DUPLICATE KEY(`@timestamp`)
        COMMENT "OLAP"
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES (
          "replication_allocation" = "tag.location.default: 1",
          "disable_auto_compaction" = "true"
        );
      """
    }

    def load_httplogs_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1, load_to_single_tablet = 'true' ->
        
        // load the json data
        streamLoad {
            table "${table_name}"
            
            // set http request header params
            set 'label', label + "_" + UUID.randomUUID().toString()
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            file file_name // import json file
            time 10000 // limit inflight 10s
            if (expected_succ_rows >= 0) {
                set 'max_filter_ratio', '1'
            }

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
		        if (ignore_failure && expected_succ_rows < 0) { return }
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    if (expected_succ_rows >= 0) {
                        assertEquals(json.NumberLoadedRows, expected_succ_rows)
                    } else {
                        assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                        assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }

    try {
        create_table(tableName)

        load_httplogs_data.call(tableName, 'test_index_approx_top_sum', 'true', 'json', 'documents-1000.json')

        sql "sync"

        sql """ set enable_common_expr_pushdown = true """

        sql """ set debug_skip_fold_constant = true; """
        qt_sql """ select size, sum(size) as sum from ${tableName} group by size order by sum desc, size asc limit 10; """
        qt_sql """ select approx_top_sum(size, 10, 300) from ${tableName}; """
        qt_sql """ select approx_top_sum(size, 5 + 5, 300) from ${tableName}; """
        qt_sql """ select approx_top_sum(size, abs(-10), 300) from ${tableName}; """

        qt_sql """ select clientip, status, size, sum(size) as sum from ${tableName} group by clientip, status, size order by sum desc, clientip asc limit 10; """
        qt_sql """ select approx_top_sum(clientip, status, size, 10, 300) from ${tableName}; """
        qt_sql """ select approx_top_sum(clientip, status, size, 5 + 5, 300) from ${tableName}; """
        qt_sql """ select approx_top_sum(clientip, status, size, abs(-10), 300) from ${tableName}; """

        def result1 = "fail"
        try {
            drop_result = sql " select approx_top_sum(size, -10, 300) from ${tableName}; "
            result1 = 'success'
        } catch(Exception ex) {
            logger.info("error msg: " + ex)
        }
        assertEquals(result1, 'fail')

        qt_sql """
            WITH tmp AS (
                SELECT approx_top_sum(clientip, status, size, 10, 300) AS json_output FROM ${tableName}
            )
            SELECT 
                e1
            FROM 
                tmp 
            LATERAL VIEW explode_json_array_json(json_output) tmp1 AS e1;
        """

        sql """ set debug_skip_fold_constant = true; """
        qt_sql """ select size, sum(size) as sum from ${tableName} group by size order by sum desc, size asc limit 10; """
        qt_sql """ select approx_top_sum(size, 10, 300) from ${tableName}; """
        qt_sql """ select approx_top_sum(size, 5 + 5, 300) from ${tableName}; """
        qt_sql """ select approx_top_sum(size, abs(-10), 300) from ${tableName}; """

        qt_sql """ select clientip, status, size, sum(size) as sum from ${tableName} group by clientip, status, size order by sum desc, clientip asc limit 10; """
        qt_sql """ select approx_top_sum(clientip, status, size, 10, 300) from ${tableName}; """
        qt_sql """ select approx_top_sum(clientip, status, size, 5 + 5, 300) from ${tableName}; """
        qt_sql """ select approx_top_sum(clientip, status, size, abs(-10), 300) from ${tableName}; """

        def result2 = "fail"
        try {
            drop_result = sql " select approx_top_sum(size, -10, 300) from ${tableName}; "
            result2 = 'success'
        } catch(Exception ex) {
            logger.info("error msg: " + ex)
        }
        assertEquals(result2, 'fail')

        qt_sql """
            WITH tmp AS (
                SELECT approx_top_sum(clientip, status, size, 10, 300) AS json_output FROM ${tableName}
            )
            SELECT 
                e1
            FROM 
                tmp 
            LATERAL VIEW explode_json_array_json(json_output) tmp1 AS e1;
        """
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}
