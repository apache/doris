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

suite('test_flexible_partial_update_restricts') {

    def tableName = "test_flexible_partial_update_restricts"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "store_row_column" = "false"); """

    sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "6"); """
    order_qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

    streamLoad {
        table "${tableName}"
        set 'format', 'csv'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.csv"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("flexible partial update only support json format as input file currently"));
        }
    }

    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'fuzzy_parse', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Don't support flexible partial update when 'fuzzy_parse' is enabled"));
        }
    }

    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'columns', 'k,v1,v3,v5'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Don't support flexible partial update when 'columns' is specified"));
        }
    }

    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'jsonpaths', '["$.k","$.v1","$.v3"]'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Don't support flexible partial update when 'jsonpaths' is specified"));
        }
    }

    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'hidden_columns', '__DORIS_DELETE_SIGN__'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Don't support flexible partial update when 'hidden_columns' is specified"));
        }
    }

    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'update'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("[INVALID_ARGUMENT]Invalid unique_key_partial_mode update, must be one of 'UPSERT', 'UPDATE_FIXED_COLUMNS' or 'UPDATE_FLEXIBLE_COLUMNS"));
        }
    }

    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        set 'merge_type', 'APPEND'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Don't support flexible partial update when 'merge_type' is specified"));
        }
    }

    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        set 'where', 'v2 > 5'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Don't support flexible partial update when 'where' is specified"));
        }
    }

    if (!isCloudMode()) {
        // in cloud mode, all tables has light schema change on
        tableName = "test_flexible_partial_update_restricts2"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "false",
            "enable_unique_key_skip_bitmap_column" = "true",
            "store_row_column" = "false"); """
        
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            file "test1.json"
            time 20000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("fail", json.Status.toLowerCase())
                assertTrue(json.Message.contains("Flexible partial update can only support table with light_schema_change enabled."));
            }
        }
    }

    tableName = "test_flexible_partial_update_restricts3"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "light_schema_change" = "true",
        "enable_unique_key_skip_bitmap_column" = "false",
        "store_row_column" = "false"); """
    def show_res = sql "show create table ${tableName}"
    assertTrue(!show_res.toString().contains('enable_unique_key_skip_bitmap_column'))
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Flexible partial update can only support table with skip bitmap hidden column."));
        }
    }

    tableName = "test_flexible_partial_update_restricts4"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL,
        `v6` variant
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "true",
        "enable_unique_key_skip_bitmap_column" = "true",
        "store_row_column" = "false"); """
    
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Flexible partial update can only support table without variant columns."));
        }
    }

    def doSchemaChange = { cmd ->
        sql cmd
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1"""
            time 2000
        }
    }

    tableName = "test_flexible_partial_update_restricts5"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL,
        ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1",
        "enable_unique_key_merge_on_write" = "false",
        "store_row_column" = "false"); """
    
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Only unique key merge on write support partial update"));
        }
    }

    tableName = "test_flexible_partial_update_restricts6"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT NULL,
        `v2` BIGINT NULL DEFAULT "9876",
        `v3` BIGINT NOT NULL,
        `v4` BIGINT NOT NULL DEFAULT "1234",
        `v5` BIGINT NULL,
        ) DUPLICATE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1"); """
    
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Only unique key merge on write support partial update"));
        }
    }

    tableName = "test_flexible_partial_update_restricts7"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
        `k` int(11) NULL, 
        `v1` BIGINT  min,
        `v2` BIGINT  max,
        `v3` BIGINT  min,
        `v4` BIGINT  sum,
        `v5` BIGINT  sum
        ) AGGREGATE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
        "replication_num" = "1"); """
    
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test1.json"
        time 20000
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("Only unique key merge on write support partial update"));
        }
    }

    tableName = "test_flexible_partial_update_restricts8"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    test {
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL,
            `__DORIS_SKIP_BITMAP_COL__` bigint null
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "light_schema_change" = "false",
            "store_row_column" = "false"); """
        exception "Disable to create table column with name start with __DORIS_: __DORIS_SKIP_BITMAP_COL__"
    }
}