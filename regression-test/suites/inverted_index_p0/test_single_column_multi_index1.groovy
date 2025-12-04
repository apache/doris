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

suite("test_single_column_multi_index1", "p0") {
    def tableName = "test_single_column_multi_index1"

    // Function to create the test table
    def createTestTable = { ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
          CREATE TABLE ${tableName} (
            `@timestamp` int(11) NULL COMMENT "",
            `clientip` varchar(20) NULL COMMENT "",
            `request` text NULL COMMENT "",
            `status` int(11) NULL COMMENT "",
            `size` int(11) NULL COMMENT ""
          ) ENGINE=OLAP
          DUPLICATE KEY(`@timestamp`)
          COMMENT "OLAP"
          DISTRIBUTED BY RANDOM BUCKETS 1
          PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
          )
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

    // Function to load test data
    def loadTestData = { times = 3 ->
        for (int i = 0; i < times; i++) {
            load_httplogs_data.call(tableName, 'test_single_column_multi_index', 'true', 'json', 'documents-1000.json')
        }
        sql "sync"
    }

    // Function to run match queries with debug points
    def runMatchQueries = { ->
        sql """ set enable_common_expr_pushdown = true; """
        sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
        try {
            qt_sql """ select /*+ SET_VAR(enable_match_without_inverted_index = true) */ count() from ${tableName} where (request = 'GET /images/hm_bg.jpg HTTP/1.0'); """
            qt_sql """ select /*+ SET_VAR(enable_match_without_inverted_index = true) */ count() from ${tableName} where (request match 'images'); """
        } finally {
        }
    }

    def timeout = 60000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0
    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
    }

    try {
      createTestTable()
      loadTestData()
      runMatchQueries()

      sql """ alter table ${tableName} add index request_text_idx(`request`) USING INVERTED PROPERTIES("support_phrase" = "true", "parser" = "unicode", "lower_case" = "true"); """
      wait_for_latest_op_on_table_finish(tableName, timeout)
      sql """ alter table ${tableName} add index request_keyword_idx(`request`) USING INVERTED;; """
      wait_for_latest_op_on_table_finish(tableName, timeout)
      
      loadTestData()
      runMatchQueries()

      
      if (!isCloudMode()) {
        sql """ BUILD INDEX request_text_idx ON ${tableName}; """
        wait_for_build_index_on_partition_finish(tableName, timeout)
      }

      if (!isCloudMode()) {
        sql """ BUILD INDEX request_keyword_idx ON ${tableName}; """
        wait_for_build_index_on_partition_finish(tableName, timeout)
      }

      runMatchQueries()

      sql """ DROP INDEX request_text_idx ON ${tableName}; """
      wait_for_latest_op_on_table_finish(tableName, timeout)
      sql """ DROP INDEX request_keyword_idx ON ${tableName}; """
      wait_for_latest_op_on_table_finish(tableName, timeout)

      runMatchQueries()
    } finally {
    }
}