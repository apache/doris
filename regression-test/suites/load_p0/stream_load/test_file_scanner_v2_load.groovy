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

suite("test_file_scanner_v2_load", "p0") {
    def tableName = "test_file_scanner_v2_load"
    def checkRows = { result, exception, loaded, filtered, unselected ->
        if (exception != null) {
            throw exception
        }
        def response = parseJson(result)
        assertEquals("Success", response.Status)
        assertEquals(loaded, response.NumberLoadedRows as int)
        assertEquals(filtered, response.NumberFilteredRows as int)
        assertEquals(unselected, response.NumberUnselectedRows as int)
    }

    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """CREATE TABLE ${tableName} (k INT NOT NULL, v INT NULL DEFAULT '7')
               DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
               PROPERTIES("replication_num" = "1")"""
        streamLoad {
            table tableName
            set 'column_separator', ','
            set 'columns', 'raw_k,v,k=cast(raw_k as int)+10'
            set 'where', 'k = 13'
            set 'strict_mode', 'true'
            set 'max_filter_ratio', '1'
            file 'file_scanner_v2_load.csv'
            check { result, exception, startTime, endTime ->
                // Conversion errors must be counted even if the target WHERE rejects that row.
                checkRows(result, exception, 1, 2, 2)
            }
        }
        sql "sync"
        assertEquals([[13, 30]], sql("SELECT k, v FROM ${tableName} ORDER BY k"))

        sql "TRUNCATE TABLE ${tableName}"
        streamLoad {
            table tableName
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'true'
            set 'max_filter_ratio', '1'
            file 'file_scanner_v2_load.json'
            check { result, exception, startTime, endTime ->
                checkRows(result, exception, 3, 2, 0)
            }
        }
        sql "sync"
        assertEquals([[1, 7], [2, null], [4, 40]], sql("SELECT k, v FROM ${tableName} ORDER BY k"))

        ["0", "1"].each { ratio ->
            sql "TRUNCATE TABLE ${tableName}"
            streamLoad {
                table tableName
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'strip_outer_array', 'true'
                set 'max_filter_ratio', ratio
                file 'file_scanner_v2_load_empty_array.json'
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def response = parseJson(result)
                    assertEquals(2, response.NumberTotalRows as int)
                    assertEquals(1, response.NumberFilteredRows as int)
                    assertEquals(0, response.NumberUnselectedRows as int)
                    if (ratio == "0") {
                        // Empty arrays must reach the quality gate even when the next row is valid.
                        assertEquals("Fail", response.Status)
                        assertEquals(0, response.NumberLoadedRows as int)
                        assertTrue(response.Message.contains("too many filtered rows"))
                    } else {
                        checkRows(result, exception, 1, 1, 0)
                    }
                }
            }
            sql "sync"
            assertEquals(ratio == "0" ? [] : [[1, 7]],
                         sql("SELECT k, v FROM ${tableName} ORDER BY k"))
        }

        sql "DROP TABLE ${tableName}"
        sql """CREATE TABLE ${tableName} (k INT NOT NULL, v INT NULL)
               UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
               PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true",
                          "enable_unique_key_skip_bitmap_column" = "true")"""
        sql "INSERT INTO ${tableName} VALUES (1, 10), (2, 20)"
        streamLoad {
            table tableName
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'max_filter_ratio', '1'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file 'file_scanner_v2_load_partial.json'
            check { result, exception, startTime, endTime ->
                checkRows(result, exception, 2, 1, 0)
            }
        }
        sql "sync"
        assertEquals([[1, 10], [2, null]], sql("SELECT k, v FROM ${tableName} ORDER BY k"))
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName}"
    }
}
