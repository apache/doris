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

suite("test_jsonb_unique_load_and_function", "p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // TODO: remove it after we add implicit cast check in Nereids
    sql "set enable_nereids_dml=false"

    // define a sql table
    def testTable = "tbl_test_jsonb_unique"
    def dataFile = "test_jsonb_unique_key.csv"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            j JSONB
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // load the jsonb data from csv file
    // fail by default for invalid data rows
    streamLoad {
        table testTable
        
        file dataFile // import csv file
        time 10000 // limit inflight 10s
        set 'strict_mode', 'true'

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("too many filtered rows"))
            assertEquals(75, json.NumberTotalRows)
            assertEquals(54, json.NumberLoadedRows)
            assertEquals(21, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    // load the jsonb data from csv file
    // success with header 'max_filter_ratio: 0.3'
    streamLoad {
        table testTable
        
        // set http request header params
        set 'max_filter_ratio', '0.3'
        file dataFile // import csv file
        time 10000 // limit inflight 10s
        set 'strict_mode', 'true'

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(75, json.NumberTotalRows)
            assertEquals(54, json.NumberLoadedRows)
            assertEquals(21, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    sql "sync"
    // check result
    qt_select "SELECT * FROM ${testTable} ORDER BY id"

    // insert into valid json rows
    sql """INSERT INTO ${testTable} VALUES(26, NULL)"""
    sql """INSERT INTO ${testTable} VALUES(27, '{"k1":"v1", "k2": 200}')"""
    sql """INSERT INTO ${testTable} VALUES(28, '{"a.b.c":{"k1.a1":"v31", "k2": 300},"a":"niu"}')"""

    // insert into invalid json rows with enable_insert_strict=true
    // expect excepiton and no rows not changed
    sql """ set enable_insert_strict = true """
    def success = true
    try {
        sql """INSERT INTO ${testTable} VALUES(26, '')"""
    } catch(Exception ex) {
       logger.info("""INSERT INTO ${testTable} invalid json failed: """ + ex)
       success = false
    }
    assertEquals(false, success)
    success = true
    try {
        sql """INSERT INTO ${testTable} VALUES(26, 'abc')"""
    } catch(Exception ex) {
       logger.info("""INSERT INTO ${testTable} invalid json failed: """ + ex)
       success = false
    }
    assertEquals(false, success)

    // insert into invalid json rows with enable_insert_strict=false
    // expect no excepiton but no rows not changed
    sql """ set enable_insert_strict = false """
    success = true
    try {
        sql """INSERT INTO ${testTable} VALUES(26, '')"""
    } catch(Exception ex) {
       logger.info("""INSERT INTO ${testTable} invalid json failed: """ + ex)
       success = false
    }
    assertEquals(true, success)
    success = true
    try {
        sql """INSERT INTO ${testTable} VALUES(26, 'abc')"""
    } catch(Exception ex) {
       logger.info("""INSERT INTO ${testTable} invalid json failed: """ + ex)
       success = false
    }
    assertEquals(true, success)

    qt_select "SELECT * FROM ${testTable} ORDER BY id"

    // jsonb_extract
    qt_jsonb_extract_select "SELECT id, j, jsonb_extract(j, '\$') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.*') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.\"a.b.c\"') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.\"a.b.c\".\"k1.a1\"') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"


    // jsonb_extract_multipath
    qt_jsonb_extract_multipath "SELECT id, j, jsonb_extract(j, '\$', '\$.*', '\$.k1', '\$[0]') FROM ${testTable} ORDER BY id"

    // jsonb_extract_string
    qt_jsonb_extract_string_select "SELECT id, j, jsonb_extract_string(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_string(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"

    // jsonb_extract_int
    qt_jsonb_extract_int_select "SELECT id, j, jsonb_extract_int(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_int(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"

    // jsonb_extract_bigint
    qt_jsonb_extract_bigint_select "SELECT id, j, jsonb_extract_bigint(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"


    // jsonb_extract_double
    qt_jsonb_extract_double_select "SELECT id, j, jsonb_extract_double(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_double(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"

    // jsonb_extract_bool
    qt_jsonb_extract_bool_select "SELECT id, j, jsonb_extract_bool(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"

    // jsonb_extract_isnull
    qt_jsonb_extract_isnull_select "SELECT id, j, jsonb_extract_isnull(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"

    // jsonb_exists_path
    qt_jsonb_exists_path_select "SELECT id, j, jsonb_exists_path(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_exists_path(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"

    // jsonb_type
    qt_jsonb_type_select "SELECT id, j, jsonb_type(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_type(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_type(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_type(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_type(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"


    // CAST from JSONB
    qt_cast_from_select "SELECT id, j, CAST(j AS BOOLEAN) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS SMALLINT) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS INT) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS BIGINT) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS DOUBLE) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS STRING) FROM ${testTable} ORDER BY id"

    // CAST to JSONB
    qt_cast_to_select "SELECT id, j, CAST(CAST(j AS BOOLEAN) AS JSONB) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS SMALLINT) AS JSONB) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS INT) AS JSONB) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS BIGINT) AS JSONB) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS DOUBLE) AS JSONB) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS STRING) AS JSONB) FROM ${testTable} ORDER BY id"

    qt_select """SELECT CAST(NULL AS JSONB)"""
    qt_select """SELECT CAST('null' AS JSONB)"""
    qt_select """SELECT CAST('true' AS JSONB)"""
    qt_select """SELECT CAST('false' AS JSONB)"""
    qt_select """SELECT CAST('100' AS JSONB)"""
    qt_select """SELECT CAST('10000' AS JSONB)"""
    qt_select """SELECT CAST('1000000000' AS JSONB)"""
    qt_select """SELECT CAST('1152921504606846976' AS JSONB)"""
    qt_select """SELECT CAST('6.18' AS JSONB)"""
    qt_select """SELECT CAST('"abcd"' AS JSONB)"""
    qt_select """SELECT CAST('{}' AS JSONB)"""
    qt_select """SELECT CAST('{"k1":"v31", "k2": 300}' AS JSONB)"""
    qt_select """SELECT CAST('[]' AS JSONB)"""
    qt_select """SELECT CAST('[123, 456]' AS JSONB)"""
    qt_select """SELECT CAST('["abc", "def"]' AS JSONB)"""
    qt_select """SELECT CAST('[null, true, false, 100, 6.18, "abc"]' AS JSONB)"""
    qt_select """SELECT CAST('[{"k1":"v41", "k2": 400}, 1, "a", 3.14]' AS JSONB)"""
    qt_select """SELECT CAST('{"k1":"v31", "k2": 300, "a1": [{"k1":"v41", "k2": 400}, 1, "a", 3.14]}' AS JSONB)"""
    qt_select """SELECT CAST("''" AS JSONB)"""
    qt_select """SELECT CAST("'abc'" AS JSONB)"""
    qt_select """SELECT CAST('abc' AS JSONB)"""
    qt_select """SELECT CAST('100x' AS JSONB)"""
    qt_select """SELECT CAST('6.a8' AS JSONB)"""
    qt_select """SELECT CAST('{x' AS JSONB)"""
    qt_select """SELECT CAST('[123, abc]' AS JSONB)"""

}
