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


suite("test_compound_inlist", "nonConcurrent"){
    def indexTbName1 = "test_compound_inlist_1"
    def indexTbName2 = "test_compound_inlist_2"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    sql """
      CREATE TABLE ${indexTbName1} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX clientip_idx (`clientip`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
        INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
      );
    """

    sql "DROP TABLE IF EXISTS ${indexTbName2}"
    sql """
      CREATE TABLE ${indexTbName2} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX clientip_idx (`clientip`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
        INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
        INDEX status_idx (`status`) USING INVERTED COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "disable_auto_compaction" = "true"
      );
    """

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
        load_httplogs_data.call(indexTbName1, 'test_compound_list_1', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName2, 'test_compound_list_2', 'true', 'json', 'documents-1000.json')

        sql "sync"
        sql """ set enable_common_expr_pushdown = true """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304)); """
        
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 200)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 200)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 304)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 200)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 200)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) or status in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) or status in (1, 2, 304)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) or status in (1, 2, 200)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) or status in (1, 2, 200)); """
      
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 200)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 200)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 304)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304, 200) and status not in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304, 200) and status not in (1, 2, 304)); """

        sql """ set enable_common_expr_pushdown = false; """
        
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304)); """
        
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 200)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 200)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 304)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 200)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 200)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) or status in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) or status in (1, 2, 304)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) or status in (1, 2, 200)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) or status in (1, 2, 200)); """
      
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 200)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 200)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status not in (1, 2, 304)); """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName1} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304, 200) and status not in (1, 2, 304)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName2} where ((request match_phrase 'hm' and clientip match_phrase '1') or (request match_phrase 'jpg' and clientip match_phrase '2')) or (((request match_phrase 'images' and clientip match_phrase '3') or (request match_phrase 'english' and clientip match_phrase '4')) and status in (1, 2, 304, 200) and status not in (1, 2, 304)); """

        sql """ set enable_common_expr_pushdown = true; """

    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}