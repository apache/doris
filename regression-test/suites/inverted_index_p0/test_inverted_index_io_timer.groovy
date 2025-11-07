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

import java.util.regex.Pattern

suite('test_inverted_index_io_timer', 'p0') {
    if (!isCloudMode()) {
        return;
    }
    
    def indexTbName1 = "test_inverted_index_io_timer_tbl"
    
    sql "DROP TABLE IF EXISTS ${indexTbName1}"
    
    // Create table with inverted index using httplogs schema
    sql """
      CREATE TABLE ${indexTbName1} (
      `@timestamp` int(11) NULL COMMENT "",
      `clientip` varchar(20) NULL COMMENT "",
      `request` text NULL COMMENT "",
      `status` int(11) NULL COMMENT "",
      `size` int(11) NULL COMMENT "",
      INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
      INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
      INDEX status_idx (`status`) USING INVERTED COMMENT '',
      INDEX size_idx (`size`) USING INVERTED COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """
    
    // Define data loading function
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
        // Load 1000 documents
        load_httplogs_data.call(indexTbName1, indexTbName1, 'true', 'json', 'documents-1000.json')
        
        sql "sync"
        
        // Enable profile
        sql """ set enable_profile = true; """
        sql """ set profile_level = 2; """
        sql """ set enable_sql_cache = false; """
        sql """ set enable_inverted_index_searcher_cache = false; """
        sql """ set enable_inverted_index_query_cache = false; """
        sql """ set enable_common_expr_pushdown = true; """
        sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
        sql """ set enable_match_without_inverted_index = false; """
        
        // Execute query with inverted index using profile
        def queryId = "test_inverted_index_io_timer_${System.currentTimeMillis()}"
        try {
            profile("${queryId}") {
                run {
                    sql "/* ${queryId} */ select * from ${indexTbName1} where request match 'images' order by `@timestamp` limit 10"
                }
                
                check { profileString, exception ->
                    def local = 0
                    def remote = 0

                    def localMatcher = Pattern.compile("InvertedIndexNumLocalIOTotal:\\s*(\\d+)").matcher(profileString)
                    if (localMatcher.find()) {
                        local = Integer.parseInt(localMatcher.group(1))
                        log.info("InvertedIndexNumLocalIOTotal: {}", local)
                    }

                    def remoteMatcher = Pattern.compile("InvertedIndexNumRemoteIOTotal:\\s*(\\d+)").matcher(profileString)
                    if (remoteMatcher.find()) {
                        remote = Integer.parseInt(remoteMatcher.group(1))
                        log.info("InvertedIndexNumRemoteIOTotal: {}", remote)
                    }

                    def total = local + remote
                    assertTrue(total > 0, "InvertedIndexNumLocalIOTotal + InvertedIndexNumRemoteIOTotal should be > 0, got: ${total} (local=${local}, remote=${remote})")
                }
            }
        } catch (IllegalStateException e) {
            if (e.message?.contains("HttpCliAction failed")) {
                log.warn("Profile HTTP request failed, skipping profile check: {}", e.message)
            } else {
                throw e
            }
        }
        
        // Also verify the query returns correct result
        def result = sql "select count(*) from ${indexTbName1} where request match 'images'"
        assertTrue(result[0][0] > 0, "Should have at least one row matching 'images'")
        
        log.info("Test completed successfully: InvertedIndexIOTimer is greater than 0")
    } finally {
        // Clean up
        // sql "DROP TABLE IF EXISTS ${indexTbName1}"
    }
}