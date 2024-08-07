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


suite("test_no_index_match", "p0") {
    // define a sql table
    def testTable_unique = "httplogs_unique"

    def create_httplogs_unique_table = {testTablex ->
      // multi-line sql
      def result = sql """
        CREATE TABLE ${testTablex} (
          `@timestamp` int(11) NULL COMMENT "",
          `clientip` string NULL COMMENT "",
          `request` string NULL COMMENT "",
          `status` string NULL COMMENT "",
          `size` string NULL COMMENT ""
          ) ENGINE=OLAP
          DUPLICATE KEY(`@timestamp`)
          COMMENT "OLAP"
          DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
          PROPERTIES (
          "replication_allocation" = "tag.location.default: 1"
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
      sql "DROP TABLE IF EXISTS ${testTable_unique}"
      create_httplogs_unique_table.call(testTable_unique)
      load_httplogs_data.call(testTable_unique, 'httplogs_unique', 'true', 'json', 'documents-1000.json')

      sql """ INSERT INTO ${testTable_unique} VALUES (1, '1', '', 1, 1); """

      sql 'sync'

      try {
          qt_sql """ select count() from ${testTable_unique} where (request match_any 'hm bg');  """
          qt_sql """ select count() from ${testTable_unique} where (request match_all 'hm bg');  """
          qt_sql """ select count() from ${testTable_unique} where (request match_phrase 'hm bg');  """
          qt_sql """ select count() from ${testTable_unique} where (request match_phrase_prefix 'hm b');  """
          qt_sql """ select count() from ${testTable_unique} where (request match_regexp 'la');  """

          qt_sql """ select count() from ${testTable_unique} where (request match_phrase '欧冶工业品');  """
          qt_sql """ select count() from ${testTable_unique} where (request match_phrase_prefix '欧冶工业品');  """
      } finally {
      }

      try {
          sql """ select /*+ SET_VAR(enable_match_without_inverted_index = 0) */ count() from ${testTable_unique} where (request match_phrase 'hm bg');  """
      } catch (Exception e) {
        log.info(e.getMessage());
        assertTrue(e.getMessage().contains("match_phrase not support execute_match"))
      }

      try {
          sql """ select /*+ SET_VAR(enable_match_without_inverted_index = 0) */ count() from ${testTable_unique} where (request match_phrase_prefix 'hm b');  """
      } catch (Exception e) {
        log.info(e.getMessage());
        assertTrue(e.getMessage().contains("match_phrase_prefix not support execute_match"))
      }
    } finally {
    }
}