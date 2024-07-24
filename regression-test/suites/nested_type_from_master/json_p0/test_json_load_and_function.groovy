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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_json_load_and_function", "p0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    // TODO: remove it after we add implicit cast check in Nereids
    sql "set enable_nereids_dml=false"

    // define a sql table
    def testTable = "tbl_test_json"
    def dataFile = "test_json.csv"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            j JSON
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // load the json data from csv file
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

            def (code, out, err) = curl("GET", json.ErrorURL)
            log.info("error result: " + out)

            assertEquals("fail", json.Status.toLowerCase())
            assertTrue(json.Message.contains("too many filtered rows"))
            assertEquals(25, json.NumberTotalRows)
            assertEquals(18, json.NumberLoadedRows)
            assertEquals(7, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
            log.info("url: " + json.ErrorURL)
        }
    }

    // load the json data from csv file
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

            def (code, out, err) = curl("GET", json.ErrorURL)
            log.info("error result: " + out)

            assertEquals("success", json.Status.toLowerCase())
            assertEquals(25, json.NumberTotalRows)
            assertEquals(18, json.NumberLoadedRows)
            assertEquals(7, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    // check result
    qt_select "SELECT * FROM ${testTable} ORDER BY id"

    // insert into valid json rows
    sql """INSERT INTO ${testTable} VALUES(26, NULL)"""
    sql """INSERT INTO ${testTable} VALUES(27, '{"k1":"v1", "k2": 200}')"""
    sql """INSERT INTO ${testTable} VALUES(28, '{"a.b.c":{"k1.a1":"v31", "k2": 300},"a":"niu"}')"""
    // int64 value
    sql """INSERT INTO ${testTable} VALUES(29, '12524337771678448270')"""
    // int64 min value
    sql """INSERT INTO ${testTable} VALUES(30, '-9223372036854775808')"""
    // int64 max value
    sql """INSERT INTO ${testTable} VALUES(31, '18446744073709551615')"""
    // insert into json with empty key
    sql """INSERT INTO ${testTable} VALUES(32, '{"":"v1"}')"""
    sql """INSERT INTO ${testTable} VALUES(33, '{"":1, "":"v1"}')"""
    sql """INSERT INTO ${testTable} VALUES(34, '{"":1, "ab":"v1", "":"v1", "": 2}')"""

    qt_select "SELECT * FROM ${testTable} where id in (32, 33, 34) ORDER BY id"

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

    // json_extract
    qt_select "SELECT id, j, jsonb_extract(j, '\$') FROM ${testTable} ORDER BY id"
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
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"

    // json_extract_string
    qt_select "SELECT id, j, json_extract_string(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_string(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_string(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_string(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_string(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_string(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"

    // json_extract_int
    qt_select "SELECT id, j, json_extract_int(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_int(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_int(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_int(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_int(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_int(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"

    // json_extract_bigint
    qt_select "SELECT id, j, json_extract_bigint(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_bigint(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_bigint(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_bigint(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_bigint(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bigint(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"


    // json_extract_largeint
    qt_json_extract_largeint_select "SELECT id, j, json_extract_largeint(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_largeint(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_largeint(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_largeint(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"


    // json_extract_double
    qt_select "SELECT id, j, json_extract_double(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_double(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_double(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_double(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_double(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_double(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"

    // json_extract_bool
    qt_select "SELECT id, j, json_extract_bool(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_bool(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_bool(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_bool(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_bool(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_bool(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"

    // json_extract_isnull
    qt_select "SELECT id, j, json_extract_isnull(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_isnull(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_isnull(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_isnull(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_extract_isnull(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_extract_isnull(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"

    // json_exists_path
    qt_select "SELECT id, j, json_exists_path(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_exists_path(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_exists_path(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_exists_path(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_exists_path(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_exists_path(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"

    // json_type
    qt_select "SELECT id, j, json_type(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_type(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_type(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_type(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, json_type(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, json_type(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"


    // CAST from JSON
    qt_select "SELECT id, j, CAST(j AS BOOLEAN) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS SMALLINT) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS INT) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS BIGINT) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS DOUBLE) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(j AS STRING) FROM ${testTable} ORDER BY id"

    // CAST to JSON
    qt_select "SELECT id, j, CAST(CAST(j AS BOOLEAN) AS JSON) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS SMALLINT) AS JSON) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS INT) AS JSON) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS BIGINT) AS JSON) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS DOUBLE) AS JSON) FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, CAST(CAST(j AS STRING) AS JSON) FROM ${testTable} ORDER BY id"

    qt_select """SELECT CAST(NULL AS JSON)"""
    qt_select """SELECT CAST('null' AS JSON)"""
    qt_select """SELECT CAST('true' AS JSON)"""
    qt_select """SELECT CAST('false' AS JSON)"""
    qt_select """SELECT CAST('100' AS JSON)"""
    qt_select """SELECT CAST('10000' AS JSON)"""
    qt_select """SELECT CAST('1000000000' AS JSON)"""
    qt_select """SELECT CAST('1152921504606846976' AS JSON)"""
    qt_select """SELECT CAST('6.18' AS JSON)"""
    qt_select """SELECT CAST('"abcd"' AS JSON)"""
    qt_select """SELECT CAST('{}' AS JSON)"""
    qt_select """SELECT CAST('{"k1":"v31", "k2": 300}' AS JSON)"""
    qt_select """SELECT CAST('[]' AS JSON)"""
    qt_select """SELECT CAST('[123, 456]' AS JSON)"""
    qt_select """SELECT CAST('["abc", "def"]' AS JSON)"""
    qt_select """SELECT CAST('[null, true, false, 100, 6.18, "abc"]' AS JSON)"""
    qt_select """SELECT CAST('[{"k1":"v41", "k2": 400}, 1, "a", 3.14]' AS JSON)"""
    qt_select """SELECT CAST('{"k1":"v31", "k2": 300, "a1": [{"k1":"v41", "k2": 400}, 1, "a", 3.14]}' AS JSON)"""
    qt_select """SELECT CAST("''" AS JSON)"""
    qt_select """SELECT CAST("'abc'" AS JSON)"""
    qt_select """SELECT CAST('abc' AS JSON)"""
    qt_select """SELECT CAST('100x' AS JSON)"""
    qt_select """SELECT CAST('6.a8' AS JSON)"""
    qt_select """SELECT CAST('{x' AS JSON)"""
    qt_select """SELECT CAST('[123, abc]' AS JSON)"""

    qt_select """SELECT id, JSON_VALID(j) FROM ${testTable} ORDER BY id"""
    qt_select """SELECT JSON_VALID('{"k1":"v31","k2":300}')"""
    qt_select """SELECT JSON_VALID('invalid json')"""
    qt_select """SELECT JSON_VALID(NULL)"""

    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.k1') FROM ${testTable} ORDER BY id"""
    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.k2', '\$.[1]') FROM ${testTable} ORDER BY id"""
    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.k2', '\$.x.y') FROM ${testTable} ORDER BY id"""
    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.k2', null) FROM ${testTable} ORDER BY id"""
    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.a1[0].k1', '\$.a1[0].k2', '\$.a1[2]') FROM ${testTable} ORDER BY id"""

    // json_parse
    qt_sql_json_parse """SELECT/*+SET_VAR(enable_fold_constant_by_be=false)*/ json_parse('{"":"v1"}')"""
    qt_sql_json_parse """SELECT/*+SET_VAR(enable_fold_constant_by_be=false)*/ json_parse('{"":1, "":"v1"}')"""
    qt_sql_json_parse """SELECT/*+SET_VAR(enable_fold_constant_by_be=false)*/ json_parse('{"":1, "ab":"v1", "":"v1", "": 2}')"""
}
