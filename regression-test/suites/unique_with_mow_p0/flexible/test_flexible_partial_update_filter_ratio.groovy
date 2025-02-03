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

suite('test_flexible_partial_update_filter_ratio') {

    for (def row_store : [false, true]) {
        // rows that miss key columns will be filtered
        def tableName = "test_flexible_partial_update_filter_missing_key"
        sql """ DROP TABLE IF EXISTS ${tableName} force"""
        sql """ CREATE TABLE ${tableName} (
            `k1` int NULL, 
            `k2` int null,
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
            ) UNIQUE KEY(k1,k2) DISTRIBUTED BY HASH(k1,k2) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "${row_store}"); """

        def show_res = sql "show create table ${tableName}"
        assertTrue(show_res.toString().contains('"enable_unique_key_skip_bitmap_column" = "true"'))
        sql """insert into ${tableName} select number, number, number, number, number, number, number from numbers("number" = "6"); """
        qt_sql "select k1,k2,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k1,k2;"

        // the default value of max_filter_ratio is 0, will fail
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "key_missing.json"
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[DATA_QUALITY_ERROR]too many filtered rows"))
                assertEquals(5, json.NumberTotalRows)
                assertEquals(3, json.NumberFilteredRows)
                assertEquals(0, json.NumberLoadedRows)
            }
        }
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            set 'max_filter_ratio', '0.5'
            file "key_missing.json"
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[DATA_QUALITY_ERROR]too many filtered rows"))
                assertEquals(5, json.NumberTotalRows)
                assertEquals(3, json.NumberFilteredRows)
                assertEquals(0, json.NumberLoadedRows)
            }
        }
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "key_missing.json"
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("[DATA_QUALITY_ERROR]too many filtered rows"))
                assertEquals(5, json.NumberTotalRows)
                // newly inserted rows will be counted into filtered rows
                assertEquals(4, json.NumberFilteredRows)
                assertEquals(0, json.NumberLoadedRows)
            }
        }
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "key_missing2.json"
            set 'max_filter_ratio', '0.6'
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(6, json.NumberTotalRows)
                assertEquals(3, json.NumberFilteredRows)
                assertEquals(3, json.NumberLoadedRows)
            }
        }
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'false'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            set 'max_filter_ratio', '0.8'
            file "key_missing3.json"
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(6, json.NumberTotalRows)
                assertEquals(3, json.NumberFilteredRows)
                assertEquals(3, json.NumberLoadedRows)
            }
        }
        qt_key_missing "select k1,k2,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k1,k2;"

        // in strict mode, rows with invalid input data will be filtered
        // and the load will fail if exceeds max_filter_ratio
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            set 'max_filter_ratio', '0.8'
            file "quality1.json"
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(3, json.NumberTotalRows)
                assertEquals(1, json.NumberFilteredRows)
                assertEquals(2, json.NumberLoadedRows)
            }
        }
        qt_quality1 "select k1,k2,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k1,k2;"
        

        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'strict_mode', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            set 'max_filter_ratio', '0.2'
            file "quality2.json"
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertEquals(3, json.NumberTotalRows)
                assertEquals(1, json.NumberFilteredRows)
                assertEquals(0, json.NumberLoadedRows)
            }
        }
        qt_quality2 "select k1,k2,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k1,k2;"

        sql """ DROP TABLE IF EXISTS ${tableName} force;"""
    }
}