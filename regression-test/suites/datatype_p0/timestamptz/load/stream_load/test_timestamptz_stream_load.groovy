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

suite("test_timestamptz_stream_load") {
    def csvFile = """test_timestamptz_stream_load.csv"""
    def prepare_table_dup_key = {
        sql """ DROP TABLE IF EXISTS test_timestamptz_stream_load_dup_key"""
        sql """
            CREATE TABLE `test_timestamptz_stream_load_dup_key` (
              `ts_tz` TIMESTAMPTZ,
              `VALUE` INT
            ) DUPLICATE KEY(`ts_tz`)
            partition by RANGE(`ts_tz`) (
                PARTITION p2023_01 VALUES LESS THAN ('2023-02-01 00:00:00 +00:00'),
                PARTITION p2023_02 VALUES LESS THAN ('2023-03-01 00:00:00 +00:00'),
                PARTITION p2023_03 VALUES LESS THAN ('2023-04-01 00:00:00 +00:00'),
                PARTITION p2023_04 VALUES LESS THAN ('2023-05-01 00:00:00 +00:00'),
                PARTITION p2023_05 VALUES LESS THAN ('2023-06-01 00:00:00 +00:00'),
                PARTITION p2023_06 VALUES LESS THAN ('2023-07-01 00:00:00 +00:00'),
                PARTITION p2023_07 VALUES LESS THAN ('2023-08-01 00:00:00 +00:00'),
                PARTITION p2023_08 VALUES LESS THAN ('2023-09-01 00:00:00 +00:00'),
                PARTITION p2023_09 VALUES LESS THAN ('2023-10-01 00:00:00 +00:00'),
                PARTITION p2023_10 VALUES LESS THAN ('2023-11-01 00:00:00 +00:00'),
                PARTITION p2023_11 VALUES LESS THAN ('2023-12-01 00:00:00 +00:00'),
                PARTITION p2023_12 VALUES LESS THAN ('2024-01-01 00:00:00 +00:00')
            )
            DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }

    prepare_table_dup_key()

    // 1.1 strict_mode=false, load success
    streamLoad {
        table "test_timestamptz_stream_load_dup_key"
        file """${csvFile}"""
        set 'column_separator', '|'
        set 'strict_mode', 'false'
        set 'max_filter_ratio', '0'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(10, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            // assertTrue(json.Message.contains("Encountered unqualified data, stop processing. Please"))
        }
    }
    qt_dup_key_non_strict "select * from test_timestamptz_stream_load_dup_key order by 1,2"

    // 1.2 strict_mode=true, max_filter_ratio=0.3, load success
    prepare_table_dup_key()
    streamLoad {
        table "test_timestamptz_stream_load_dup_key"
        file """${csvFile}"""
        set 'column_separator', '|'
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '0.2'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(8, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
            assertTrue(result.contains("ErrorURL"))
        }
    }
    qt_dup_key_strict0 "select * from test_timestamptz_stream_load_dup_key order by 1,2"

    // 1.3 strict_mode=true, max_filter_ratio=0.2, load fail
    prepare_table_dup_key()
    streamLoad {
        table "test_timestamptz_stream_load_dup_key"
        file """${csvFile}"""
        set 'column_separator', '|'
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '0.1'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
            assertEquals(2, json.NumberFilteredRows)
            assertTrue(json.Message.contains("too many filtered rows"))
            assertTrue(result.contains("ErrorURL"))
        }
    }
    qt_dup_key_strict1 "select * from test_timestamptz_stream_load_dup_key order by 1,2"

    def prepare_table_dup_key_not_null = {
        sql """ DROP TABLE IF EXISTS test_timestamptz_stream_load_dup_key"""
        sql """
            CREATE TABLE `test_timestamptz_stream_load_dup_key` (
              `ts_tz` TIMESTAMPTZ not null,
              `VALUE` INT
            ) DUPLICATE KEY(`ts_tz`)
            partition by RANGE(`ts_tz`) (
                PARTITION p2023_01 VALUES LESS THAN ('2023-02-01 00:00:00 +00:00'),
                PARTITION p2023_02 VALUES LESS THAN ('2023-03-01 00:00:00 +00:00'),
                PARTITION p2023_03 VALUES LESS THAN ('2023-04-01 00:00:00 +00:00'),
                PARTITION p2023_04 VALUES LESS THAN ('2023-05-01 00:00:00 +00:00'),
                PARTITION p2023_05 VALUES LESS THAN ('2023-06-01 00:00:00 +00:00'),
                PARTITION p2023_06 VALUES LESS THAN ('2023-07-01 00:00:00 +00:00'),
                PARTITION p2023_07 VALUES LESS THAN ('2023-08-01 00:00:00 +00:00'),
                PARTITION p2023_08 VALUES LESS THAN ('2023-09-01 00:00:00 +00:00'),
                PARTITION p2023_09 VALUES LESS THAN ('2023-10-01 00:00:00 +00:00'),
                PARTITION p2023_10 VALUES LESS THAN ('2023-11-01 00:00:00 +00:00'),
                PARTITION p2023_11 VALUES LESS THAN ('2023-12-01 00:00:00 +00:00'),
                PARTITION p2023_12 VALUES LESS THAN ('2024-01-01 00:00:00 +00:00')
            )
            DISTRIBUTED BY HASH(`ts_tz`) BUCKETS 16
            PROPERTIES (
            "replication_num" = "1"
            );
        """
    }

    // 2. null value to not null column
    // 2.1 strict_mode=false, max_filter_ratio=0.2, load fail
    prepare_table_dup_key_not_null()
    streamLoad {
        table "test_timestamptz_stream_load_dup_key"
        file """${csvFile}"""
        set 'column_separator', '|'
        set 'strict_mode', 'false'
        set 'max_filter_ratio', '0.2'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
            assertEquals(3, json.NumberFilteredRows)
            assertTrue(json.Message.contains("too many filtered rows"))
            assertTrue(result.contains("ErrorURL"))
        }
    }
    qt_dup_key_null_to_not_null_non_strict0 "select * from test_timestamptz_stream_load_dup_key order by 1,2"

    // 2.2 strict_mode=false, max_filter_ratio=0.3, load success
    prepare_table_dup_key_not_null()
    streamLoad {
        table "test_timestamptz_stream_load_dup_key"
        file """${csvFile}"""
        set 'column_separator', '|'
        set 'strict_mode', 'false'
        set 'max_filter_ratio', '0.3'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(7, json.NumberLoadedRows)
            assertEquals(3, json.NumberFilteredRows)
            assertTrue(result.contains("ErrorURL"))
        }
    }
    qt_dup_key_null_to_not_null_non_strict1 "select * from test_timestamptz_stream_load_dup_key order by 1,2"

    // 2.3 strict_mode=true, max_filter_ratio=0.3, load success
    prepare_table_dup_key_not_null()
    streamLoad {
        table "test_timestamptz_stream_load_dup_key"
        file """${csvFile}"""
        set 'column_separator', '|'
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '0.3'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(7, json.NumberLoadedRows)
            assertEquals(3, json.NumberFilteredRows)
            assertTrue(result.contains("ErrorURL"))
        }
    }
    qt_dup_key_null_to_not_null_strict0 "select * from test_timestamptz_stream_load_dup_key order by 1,2"

    // 2.4 strict_mode=true, max_filter_ratio=0.2, load fail
    prepare_table_dup_key_not_null()
    streamLoad {
        table "test_timestamptz_stream_load_dup_key"
        file """${csvFile}"""
        set 'column_separator', '|'
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '0.2'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(0, json.NumberLoadedRows)
            assertEquals(3, json.NumberFilteredRows)
            assertTrue(json.Message.contains("too many filtered rows"))
            assertTrue(result.contains("ErrorURL"))
        }
    }
    qt_dup_key_null_to_not_null_strict1 "select * from test_timestamptz_stream_load_dup_key order by 1,2"
}
