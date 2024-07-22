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

suite("test_index_inlist_fault_injection", "nonConcurrent") {
    // define a sql table
    def indexTbName = "test_index_inlist_fault_injection"

    sql "DROP TABLE IF EXISTS ${indexTbName}"
    sql """
      CREATE TABLE ${indexTbName} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
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
            }
        }
    }

    try {
      load_httplogs_data.call(indexTbName, 'test_index_inlist_fault_injection', 'true', 'json', 'documents-1000.json')

      sql "sync"

      try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator._rowid_result_for_index")

        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName} where clientip in ('40.135.0.0', '232.0.0.0', '26.1.0.0'); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName} where status in (1, 304, 200); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName} where (request match 'hm' or clientip in ('40.135.0.0', '232.0.0.0', '26.1.0.0')); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName} where (request match 'hm' and clientip in ('40.135.0.0', '232.0.0.0', '26.1.0.0')); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName} where (request match 'hm' or status in (1, 304, 200)); """
        qt_sql """ select /*+ SET_VAR(inverted_index_skip_threshold = 0) */ count() from ${indexTbName} where (request match 'hm' and status in (1, 304, 200)); """

      } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._rowid_result_for_index")
      }

      try {
        sql """ set enable_common_expr_pushdown = true; """

        qt_sql """ select count() from ${indexTbName} where (clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0')); """
        qt_sql """ select count() from ${indexTbName} where (clientip not in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0')); """
        qt_sql """ select count() from ${indexTbName} where (clientip match '2.1.0.0' and clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0') and clientip match '120.1.0.0'); """
        qt_sql """ select count() from ${indexTbName} where (clientip match '2.1.0.0' or clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0') or clientip match '120.1.0.0'); """
        qt_sql """ select count() from ${indexTbName} where (clientip match '2.1.0.0' and clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0') or clientip match '120.1.0.0'); """
        qt_sql """ select count() from ${indexTbName} where (clientip match '2.1.0.0' or clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0') and clientip match '120.1.0.0'); """

        qt_sql """ select count() from ${indexTbName} where (clientip = '2.1.0.0' and clientip = NULL or clientip = '40.135.0.0'); """
        qt_sql """ select count() from ${indexTbName} where (clientip = '2.1.0.0' or clientip = NULL and clientip = '40.135.0.0'); """

        sql """ set enable_common_expr_pushdown = false; """

        qt_sql """ select count() from ${indexTbName} where (clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0')); """
        qt_sql """ select count() from ${indexTbName} where (clientip not in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0')); """
        qt_sql """ select count() from ${indexTbName} where (clientip match '2.1.0.0' and clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0') and clientip match '120.1.0.0'); """
        qt_sql """ select count() from ${indexTbName} where (clientip match '2.1.0.0' or clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0') or clientip match '120.1.0.0'); """
        qt_sql """ select count() from ${indexTbName} where (clientip match '2.1.0.0' and clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0') or clientip match '120.1.0.0'); """
        qt_sql """ select count() from ${indexTbName} where (clientip match '2.1.0.0' or clientip in ('40.135.0.0', '232.0.0.0', NULL, '26.1.0.0', '247.37.0.0') and clientip match '120.1.0.0'); """

        qt_sql """ select count() from ${indexTbName} where (clientip = '2.1.0.0' and clientip = NULL or clientip = '40.135.0.0'); """
        qt_sql """ select count() from ${indexTbName} where (clientip = '2.1.0.0' or clientip = NULL and clientip = '40.135.0.0'); """

        sql """ set enable_common_expr_pushdown = true; """
      } finally {
      }
    } finally {
    }
}