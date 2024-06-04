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

suite("test_index_compound_directory_failure_injection", "nonConcurrent") {
    // define a sql table
    def testTable_dup = "httplogs_dup_compound"

    def create_httplogs_dup_table = {testTablex ->
        // multi-line sql
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${testTablex} (
                          `@timestamp` int(11) NULL,
                          `clientip` varchar(20) NULL,
                          `request` text NULL,
                          `status` int(11) NULL,
                          `size` int(11) NULL,
                          INDEX size_idx (`size`) USING INVERTED COMMENT '',
                          INDEX status_idx (`status`) USING INVERTED COMMENT '',
                          INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
                          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser"="english") COMMENT ''
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`@timestamp`)
                        COMMENT 'OLAP'
                        PARTITION BY RANGE(`@timestamp`)
                        (PARTITION p181998 VALUES [("-2147483648"), ("894225602")),
                        PARTITION p191998 VALUES [("894225602"), ("894830402")),
                        PARTITION p201998 VALUES [("894830402"), ("895435201")),
                        PARTITION p211998 VALUES [("895435201"), ("896040001")),
                        PARTITION p221998 VALUES [("896040001"), ("896644801")),
                        PARTITION p231998 VALUES [("896644801"), ("897249601")),
                        PARTITION p241998 VALUES [("897249601"), ("897854300")),
                        PARTITION p251998 VALUES [("897854300"), ("2147483647")))
                        DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 12
                        PROPERTIES (
                        "replication_allocation" = "tag.location.default: 1",
                        "storage_format" = "V2",
                        "compression" = "ZSTD",
                        "light_schema_change" = "true",
                        "disable_auto_compaction" = "false"
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
            }
        }
    }

    try {
        sql "DROP TABLE IF EXISTS ${testTable_dup}"
        create_httplogs_dup_table.call(testTable_dup)

        try {
            GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_destructor")
            load_httplogs_data.call(testTable_dup, 'test_index_compound_directory', 'true', 'json', 'documents-1000.json')
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_destructor")
        }
        qt_sql "select COUNT() from ${testTable_dup} where request match 'images'"
        try {
            GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_bufferedindexoutput_close")
            load_httplogs_data.call(testTable_dup, 'test_index_compound_directory', 'true', 'json', 'documents-1000.json')
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_bufferedindexoutput_close")
        }
        qt_sql "select COUNT() from ${testTable_dup} where request match 'images'"
        try {
            GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._set_writer_close_status_error")
            load_httplogs_data.call(testTable_dup, 'test_index_compound_directory', 'true', 'json', 'documents-1000.json')
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._set_writer_close_status_error")
        }
        qt_sql "select COUNT() from ${testTable_dup} where request match 'images'"
        try {
            def test_index_compound_directory = "test_index_compound_directory1"
            sql "DROP TABLE IF EXISTS ${test_index_compound_directory}"
            create_httplogs_dup_table.call(test_index_compound_directory)
            GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._mock_append_data_error_in_fsindexoutput_flushBuffer")
            load_httplogs_data.call(test_index_compound_directory, test_index_compound_directory, 'true', 'json', 'documents-1000.json')
            qt_sql "select COUNT() from ${test_index_compound_directory} where request match 'gif'"
            try_sql("DROP TABLE IF EXISTS ${test_index_compound_directory}")
        } catch(Exception ex) {
            logger.info("_mock_append_data_error_in_fsindexoutput_flushBuffer,  result: " + ex)
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._mock_append_data_error_in_fsindexoutput_flushBuffer")
        }
        try {
            def test_index_compound_directory = "test_index_compound_directory2"
            sql "DROP TABLE IF EXISTS ${test_index_compound_directory}"
            create_httplogs_dup_table.call(test_index_compound_directory)
            GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer")
            load_httplogs_data.call(test_index_compound_directory, test_index_compound_directory, 'true', 'json', 'documents-1000.json')
            qt_sql "select COUNT() from ${test_index_compound_directory} where request match 'images'"
            try_sql("DROP TABLE IF EXISTS ${test_index_compound_directory}")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer")
        }
        try {
            def test_index_compound_directory = "test_index_compound_directory3"
            sql "DROP TABLE IF EXISTS ${test_index_compound_directory}"
            create_httplogs_dup_table.call(test_index_compound_directory)
            GetDebugPoint().enableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init")
            load_httplogs_data.call(test_index_compound_directory, test_index_compound_directory, 'true', 'json', 'documents-1000.json')
            qt_sql "select COUNT() from ${test_index_compound_directory} where request match 'png'"
            try_sql("DROP TABLE IF EXISTS ${test_index_compound_directory}")
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init")
        }
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}
