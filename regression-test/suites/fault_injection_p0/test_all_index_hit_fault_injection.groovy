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

suite("test_all_index_hit_fault_injection", "nonConcurrent") {
    // define a sql table
    def indexTbName1 = "test_all_index_hit_fault_injection_1"
    def indexTbName2 = "test_all_index_hit_fault_injection_2"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    sql """
      CREATE TABLE ${indexTbName1} (
        `@timestamp` int(11) NULL COMMENT "",
        `clientip` varchar(20) NULL COMMENT "",
        `request` text NULL COMMENT "",
        `status` int(11) NULL COMMENT "",
        `size` int(11) NULL COMMENT "",
        INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
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
        INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
        INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
      ) ENGINE=OLAP
      UNIQUE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
      PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true",
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
      load_httplogs_data.call(indexTbName1, 'test_all_index_hit_fault_injection_1', 'true', 'json', 'documents-1000.json')
      load_httplogs_data.call(indexTbName2, 'test_all_index_hit_fault_injection_2', 'true', 'json', 'documents-1000.json')

      sql "sync"

      try {
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator._read_columns_by_index", [column_name: "clientip,request"])
        GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.fast_execute", [column_name: "status,size"])

        
        qt_sql """ select count() from ${indexTbName1} where (request match_phrase 'hm'); """
        qt_sql """ select count() from ${indexTbName1} where (request match_phrase 'hm' and clientip = '126.1.0.0'); """
        qt_sql """ select count() from ${indexTbName1} where (request match_phrase 'hm' and clientip = '126.1.0.0') or (request match_phrase 'bg' and clientip = '201.0.0.0'); """
        qt_sql """ select count() from ${indexTbName1} where (request match_phrase 'hm' and clientip = '126.1.0.0' or clientip = '247.37.0.0') or (request match_phrase 'bg' and clientip = '201.0.0.0' or clientip = '232.0.0.0'); """
        qt_sql """ select count() from ${indexTbName1} where (request match_phrase 'hm' and clientip in ('126.1.0.0', '247.37.0.0')) or (request match_phrase 'bg' and clientip in ('201.0.0.0', '232.0.0.0')); """
        qt_sql """ select count() from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455); """
        qt_sql """ select count() from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm'); """
        qt_sql """ select count() from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm' or request match_phrase 'ag'); """
        qt_sql """ select count() from ${indexTbName1} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm' or request match_phrase 'ag' or status = 304); """

        qt_sql """ select count() from ${indexTbName2} where (request match_phrase 'hm'); """
        qt_sql """ select count() from ${indexTbName2} where (request match_phrase 'hm' and clientip = '126.1.0.0'); """
        qt_sql """ select count() from ${indexTbName2} where (request match_phrase 'hm' and clientip = '126.1.0.0') or (request match_phrase 'bg' and clientip = '201.0.0.0'); """
        qt_sql """ select count() from ${indexTbName2} where (request match_phrase 'hm' and clientip = '126.1.0.0' or clientip = '247.37.0.0') or (request match_phrase 'bg' and clientip = '201.0.0.0' or clientip = '232.0.0.0'); """
        qt_sql """ select count() from ${indexTbName2} where (request match_phrase 'hm' and clientip in ('126.1.0.0', '247.37.0.0')) or (request match_phrase 'bg' and clientip in ('201.0.0.0', '232.0.0.0')); """
        qt_sql """ select count() from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455); """
        qt_sql """ select count() from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm'); """
        qt_sql """ select count() from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm' or request match_phrase 'ag'); """
        qt_sql """ select count() from ${indexTbName2} where (`@timestamp` >= 893964617 and `@timestamp` < 893966455) and (request match_phrase 'hm' or request match_phrase 'ag' or status = 304); """

      } finally {
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator._read_columns_by_index")
        GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.fast_execute")
      }
    } finally {
    }
}