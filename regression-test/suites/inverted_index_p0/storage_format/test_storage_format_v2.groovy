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

suite("test_storage_format_v2", "p0, nonConcurrent") {
    // define a sql table
    def testTable = "httplogs_dup_v1"

    def create_httplogs_dup_table = {test_table ->
        // multi-line sql
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${test_table} (
                          `@timestamp` int(11) NULL,
                          `clientip` varchar(20) NULL,
                          `request` string NULL,
                          `status` int(11) NULL,
                          `size` int(11) NULL,
                          INDEX size_idx (`size`) USING INVERTED COMMENT '',
                          INDEX status_idx (`status`) USING INVERTED COMMENT '',
                          INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
                          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser"="english") COMMENT ''
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`@timestamp`)
                        COMMENT 'OLAP'
                        DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 2
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "inverted_index_storage_format" = "V2",
                        "disable_auto_compaction" = "true"
                        );
                        """
    }
    
    def load_httplogs_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1 ->
        
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
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_httplogs_dup_table.call(testTable)

        GetDebugPoint().enableDebugPointForAllBEs("inverted_index_storage_format_must_be_v2")
        GetDebugPoint().enableDebugPointForAllBEs("match.invert_index_not_support_execute_match")
        load_httplogs_data.call(testTable, 'label1', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(testTable, 'label2', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(testTable, 'label3', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(testTable, 'label4', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(testTable, 'label5', 'true', 'json', 'documents-1000.json')
        sql "sync"

        qt_sql(" select COUNT(*) from ${testTable} where request match 'images' ")
        
        def getJobState = { indexName ->
            def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${indexName}' ORDER BY createtime DESC LIMIT 1 """
            return jobStateResult[0][9]
        }

        def wait_for_schema_change = { ->
            int max_try_time = 3000
            while (max_try_time--){
                String result = getJobState(testTable)
                if (result == "FINISHED") {
                    sleep(3000)
                    break
                } else {
                    if (result == "RUNNING") {
                        sleep(3000)
                    }
                    if (max_try_time < 1){
                        assertEquals(1,2)
                    }
                }
            }
        }

        sql """ ALTER TABLE ${testTable} modify COLUMN status text"""
        wait_for_schema_change.call()

        qt_sql(" select COUNT(*) from ${testTable} where request match 'images' ")
        
    } finally {
        sql("DROP TABLE IF EXISTS ${testTable}")
        GetDebugPoint().disableDebugPointForAllBEs("inverted_index_storage_format_must_be_v2")
        GetDebugPoint().disableDebugPointForAllBEs("match.invert_index_not_support_execute_match")
    }
}
