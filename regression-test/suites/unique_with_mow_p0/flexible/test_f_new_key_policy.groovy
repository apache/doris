
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

suite("test_f_new_key_policy", "p0") {

    def tableName = "test_f_new_key_policy"
    sql """ DROP TABLE IF EXISTS ${tableName} force"""
    sql """ CREATE TABLE ${tableName} (
            `k` BIGINT NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int)
            UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "enable_unique_key_skip_bitmap_column" = "true"); """
    sql """insert into ${tableName} select number,number,number,number from numbers("number"="5");"""
    qt_sql """select * from ${tableName} order by k;"""

    def checkVariable = { expected -> 
        def res = sql_return_maparray """show variables where Variable_name="partial_update_new_key_behavior";""";
        logger.info("res: ${res}")
        assertTrue(res[0].VARIABLE_VALUE.equalsIgnoreCase(expected));
    }

    // 2. test stream load
    // 2.1
    String load1 = """{"k":1,"c1":999}
                      {"k":2,"c2":888}
                      {"k":10,"c1":888,"c3":7777}
                      {"k":12,"c2":888,"c3":7777}"""
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        set 'partial_update_new_key_behavior', 'append'
        inputStream new ByteArrayInputStream(load1.getBytes())
        time 10000
    }
    qt_stream_load_append """select * from ${tableName} order by k;"""

    String load2 = """{"k":3,"c1":999}
                      {"k":4,"c2":888}
                      {"k":20,"c1":888,"c3":7777}
                      {"k":22,"c2":888,"c3":7777}"""
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        set 'partial_update_new_key_behavior', 'error'
        inputStream new ByteArrayInputStream(load2.getBytes())
        time 10000
        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
            assertTrue(json.Message.toString().contains("[E-7003]Can't append new rows in partial update when partial_update_new_key_behavior is ERROR. Row with key=[20] is not in table."))
        }
    }
    qt_stream_load_error """select * from ${tableName} order by k;"""


    String load3 = """{"k":3,"c1":999}
                      {"k":4,"c2":888}
                      {"k":20,"c1":888,"c3":7777}
                      {"k":22,"c2":888,"c3":7777}"""
    // 2.2 partial_update_new_key_behavior will not take effect when it's not a partial update
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'partial_update_new_key_behavior', 'error'
        inputStream new ByteArrayInputStream(load3.getBytes())
        time 10000
    }
    qt_stream_ignore_property """select * from ${tableName} order by k;"""
}
