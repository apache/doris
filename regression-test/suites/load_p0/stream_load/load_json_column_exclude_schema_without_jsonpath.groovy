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

suite("test_load_json_column_exclude_schema_without_jsonpath", "p0") {
    // define a sql table
    def testTable = "tbl_test_load_json_column_exclude_schema_without_jsonpath"
    
    def create_test_table = {
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              k1 TINYINT NULL,
              k2 SMALLINT NULL,
              k3 INT NULL,
              k4 BIGINT NULL,
              k5 DATETIME NULL,
              v1 DATE REPLACE NULL,
              v2 CHAR REPLACE NULL,
              v3 VARCHAR(4096) REPLACE NULL,
              v4 FLOAT SUM NULL,
              v5 DOUBLE SUM NULL,
              v6 DECIMAL(20,7) SUM NULL
            ) ENGINE=OLAP
            AGGREGATE KEY(k1,k2,k3,k4,k5)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 5
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
            """
    }

    def load_array_data = {new_json_reader_flag, table_name, strip_flag, read_flag, format_flag, exprs, json_paths, 
                            json_root, where_expr, fuzzy_flag, column_sep, file_name ->

        // load the json data
        streamLoad {
            table table_name
            
            // set http request header params
            set 'strip_outer_array', strip_flag
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            set 'columns', exprs
            set 'jsonpaths', json_paths
            set 'json_root', json_root
            set 'where', where_expr
            set 'fuzzy_parse', fuzzy_flag
            set 'column_separator', column_sep
            set 'max_filter_ratio', '0.4'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.NumberTotalRows == 60)
                assertTrue(json.NumberLoadedRows == 0)
                assertTrue(json.NumberFilteredRows == 30);
            }
        }
    }

    // case1: import array data in json format and enable vectorized engine
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table.call()

        load_array_data.call('false', testTable, 'true', '', 'json', '', '', '', '', '', '', 'json_column_match.json')

        // test new json load, should be deleted after new_load_scan ready
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_test_table.call()
        load_array_data.call('true', testTable, 'true', '', 'json', '', '', '', '', '', '', 'json_column_match.json')

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}
