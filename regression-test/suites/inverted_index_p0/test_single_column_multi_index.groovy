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


suite("test_single_column_multi_index", "nonConcurrent"){
    def indexTbName = "test_single_column_multi_index"

    sql "DROP TABLE IF EXISTS ${indexTbName}"

    sql """
      CREATE TABLE ${indexTbName} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX request_keyword_idx (`request`) USING INVERTED COMMENT '',
        INDEX request_text_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
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
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(indexTbName, 'test_index_match_all', 'true', 'json', 'documents-1000.json')

        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """
        GetDebugPoint().enableDebugPointForAllBEs("VMatchPredicate.execute")

        GetDebugPoint().enableDebugPointForAllBEs("inverted_index_reader._select_best_reader", [type: 0])
        try {
          qt_sql """ select count() from ${indexTbName} where (request match 'images'); """
        } finally {
          GetDebugPoint().disableDebugPointForAllBEs("inverted_index_reader._select_best_reader")
        }

        GetDebugPoint().enableDebugPointForAllBEs("inverted_index_reader._select_best_reader", [type: 1])
        try {
          qt_sql """ select count() from ${indexTbName} where (request = 'GET /images/hm_bg.jpg HTTP/1.0'); """
        } finally {
          GetDebugPoint().disableDebugPointForAllBEs("inverted_index_reader._select_best_reader")
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("VMatchPredicate.execute")
    }
}