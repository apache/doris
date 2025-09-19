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

suite("test_chinese_col_load", "p0") {
    def tableName = "test_chinese_col_load"
    
    sql "SET enable_unicode_name_support = true"
    sql "SET sql_mode = ''"

    def stream_load_success = {table_name, _format, _columns, _jsonpaths, col_sep, 
                               array_json, line_json, file_name ->
        streamLoad {
            table table_name
            // http request header
            set 'format', _format
            set 'columns', _columns
            set 'jsonpaths', _jsonpaths
            set 'column_separator', col_sep
            set 'strip_outer_array', array_json
            set 'read_json_by_line', line_json
            set 'enable_unicode_name_support', 'true'
            file file_name
            time 10000 // limit inflight 10s
            
            enableUtf8Encoding true
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    log.error("Unexpected exception with UTF-8 encoding: ${exception}")
                    throw exception
                }
                log.info("Stream load result (UTF-8 enabled): ${result}".toString())
                def json = parseJson(result)
                
                assertEquals("Success", json.Status)
                assertEquals(3, json.NumberLoadedRows.toLong())
                log.info("✅ Chinese column names handled correctly with UTF-8 encoding")
            }
        }
    }

    def stream_load_failure = {table_name, _format, _columns, _jsonpaths, col_sep,  
                               array_json, line_json, file_name ->
        streamLoad {
            table table_name
            // http request header
            set 'format', _format
            set 'columns', _columns
            set 'jsonpaths', _jsonpaths
            set 'column_separator', col_sep
            set 'strip_outer_array', array_json
            set 'read_json_by_line', line_json
            set 'enable_unicode_name_support', 'true'
            file file_name
            time 10000 // limit inflight 10s
            
            enableUtf8Encoding false
            
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    log.info("Expected exception without UTF-8 encoding: ${exception}")
                    return
                }
                log.info("Stream load result (UTF-8 disabled): ${result}".toString())
                def json = parseJson(result)
                
                if ("Success".equals(json.Status)) {
                    log.warn("⚠️ Unexpected success without UTF-8 encoding - server may have fallback handling")
                    log.info("Loaded rows: ${json.NumberLoadedRows}")
                } else {
                    log.info("❌ Expected failure without UTF-8 encoding - Status: ${json.Status}")
                    log.info("Error message: ${json.Message}")
                    if (json.ErrorURL) {
                        def (code, out, err) = curl("GET", json.ErrorURL)
                        log.info("Error details: " + out)
                        assertTrue("Should contain encoding-related error", 
                                  out.contains("Duplicate column") || out.contains("??") || 
                                  json.Message.contains("ParseException") ||
                                  json.Message.contains("Unknown column"))
                    }
                }
            }
        }
    }

    def create_test_table = {table_name ->
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                `OBJECTID` varchar(36) NULL,
                `姓名` varchar(255) NULL,
                `CREATEDTIME` datetime NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`OBJECTID`)
            DISTRIBUTED BY HASH(`OBJECTID`) BUCKETS 3
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1"
            )
        """
    }

    // case1: test UTF-8
    sql "DROP TABLE IF EXISTS ${tableName}_success"
    create_test_table.call("${tableName}_success")
    log.info("🧪 Testing Chinese column names WITH UTF-8 encoding (should succeed)")
    stream_load_success.call("${tableName}_success", "csv", "OBJECTID,姓名,CREATEDTIME", "", ",", "", "", "chinese_col_complex.csv")
    try_sql("DROP TABLE IF EXISTS ${tableName}_success")

    // case2: test default
    sql "DROP TABLE IF EXISTS ${tableName}_failure"  
    create_test_table.call("${tableName}_failure")
    log.info("🧪 Testing Chinese column names WITHOUT UTF-8 encoding (may fail)")
    stream_load_failure.call("${tableName}_failure", "csv", "OBJECTID,姓名,CREATEDTIME", "", ",", "", "", "chinese_col_complex.csv")
    try_sql("DROP TABLE IF EXISTS ${tableName}_failure")
}
