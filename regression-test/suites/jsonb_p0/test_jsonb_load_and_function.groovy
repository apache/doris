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

suite("test_jsonb_load_and_function", "p0") {

    // define a sql table
    def testTable = "tbl_test_jsonb"
    def dataFile = "test_jsonb.csv"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            j JSONB
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // load the jsonb data from csv file
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
            def (code, out, err) = curl("GET", json.ErrorURL)
            log.info("error result: " + out)

            assertEquals("success", json.Status.toLowerCase())
            assertEquals(25, json.NumberTotalRows)
            assertEquals(18, json.NumberLoadedRows)
            assertEquals(7, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    sql """ sync; """

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
    sql """INSERT INTO ${testTable} VALUES(33, '{"":1, " ":"v1"}')"""
    sql """INSERT INTO ${testTable} VALUES(34, '{"":1, "ab":"v1", " ":"v1", "  ": 2}')"""

    qt_select "SELECT * FROM ${testTable} where id in (32, 33, 34) ORDER BY id"

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


    // jsonb_extract_largeint
    qt_jsonb_extract_largeint_select "SELECT id, j, jsonb_extract_largeint(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.k1') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.k2') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$[5]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$[6]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$[10]') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[3]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[4]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[last]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[last-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[last-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[last-2]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[last-10]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[-0]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[-1]') FROM ${testTable} ORDER BY id"
    qt_select "SELECT id, j, jsonb_extract_largeint(j, '\$.a1[-10]') FROM ${testTable} ORDER BY id"


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

    qt_select """SELECT id, JSON_VALID(j) FROM ${testTable} ORDER BY id"""
    qt_select """SELECT JSON_VALID('{"k1":"v31","k2":300}')"""
    qt_select """SELECT JSON_VALID('invalid json')"""
    qt_select """SELECT JSON_VALID(NULL)"""

    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.k1') FROM ${testTable} ORDER BY id"""
    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.k2', '\$.[1]') FROM ${testTable} ORDER BY id"""
    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.k2', '\$.x.y') FROM ${testTable} ORDER BY id"""
    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.k2', null) FROM ${testTable} ORDER BY id"""
    qt_select """SELECT id, j, JSON_EXTRACT(j, '\$.a1[0].k1', '\$.a1[0].k2', '\$.a1[2]') FROM ${testTable} ORDER BY id"""

    //json_length
    qt_sql_json_length """SELECT json_length('1')"""
    qt_sql_json_length """SELECT json_length('true')"""
    qt_sql_json_length """SELECT json_length('null')"""
    qt_sql_json_length """SELECT json_length('"abc"')"""
    qt_sql_json_length """SELECT json_length('[]')"""
    qt_sql_json_length """SELECT json_length('[1, 2]')"""
    qt_sql_json_length """SELECT json_length('[1, {"x": 2}]')"""
    qt_sql_json_length """SELECT json_length('{"x": 1, "y": [1, 2]}', '\$.y')"""
    qt_sql_json_length """SELECT json_length('{"k1":"v31","k2":300}')"""
    qt_sql_json_length """SELECT json_length('{"a.b.c":{"k1.a1":"v31", "k2": 300},"a":"niu"}')"""
    qt_sql_json_length """SELECT json_length('{"a":{"k1.a1":"v31", "k2": 300},"b":"niu"}','\$.a')"""
    qt_sql_json_length """SELECT json_length('abc','\$.k1')"""

    qt_select_length """SELECT id, j, json_length(j) FROM ${testTable} ORDER BY id"""
    qt_select_length """SELECT id, j, json_length(j, '\$[1]') FROM ${testTable} ORDER BY id"""
    qt_select_length """SELECT id, j, json_length(j, '\$.k2') FROM ${testTable} ORDER BY id"""
    qt_select_length """SELECT id, j, json_length(null) FROM ${testTable} ORDER BY id"""

    //json_contains
    qt_sql_json_contains """SELECT json_contains('[1, 2, {"x": 3}]', '1')"""
    qt_sql_json_contains """SELECT json_contains('[1, 2, {"x": 3}]', '{"x": 3}')"""
    qt_sql_json_contains """SELECT json_contains('[1, 2, {"x": 3}]', '3')"""
    qt_sql_json_contains """SELECT json_contains('[1, 2, [3, 4]]', '2')"""
    qt_sql_json_contains """SELECT json_contains('[1, 2, [3, 4]]', '2', '\$[2]')"""
    qt_sql_json_contains """SELECT json_contains('{"k1":"v31","k2":300}', '{"k2":300}')"""
    qt_sql_json_contains """SELECT json_contains('{"k1":"v31","k2":300}', '{"k2":300,"k1":"v31"}')"""

    qt_select_json_contains """SELECT id, j, json_contains(j, cast('true' as json)) FROM ${testTable} ORDER BY id"""
    qt_select_json_contains """SELECT id, j, json_contains(j, cast('{"k2":300}' as json)) FROM ${testTable} ORDER BY id"""
    qt_select_json_contains """SELECT id, j, json_contains(j, cast('{"k1":"v41","k2":400}' as json), '\$.a1') FROM ${testTable} ORDER BY id"""
    qt_select_json_contains """SELECT id, j, json_contains(j, cast('[123,456]' as json)) FROM ${testTable} ORDER BY id"""
    // old planner do not support explode_json_object
    test {
        sql """ select /*+SET_VAR(experimental_enable_nereids_planner=false)*/ id, j, k,v from ${testTable} lateral view explode_json_object_outer(j) tmp as k,v order by id; """
        exception "errCode = 2"
    }
    test {
        sql """ select /*+SET_VAR(experimental_enable_nereids_planner=false)*/ id, j, k,v from ${testTable} lateral view explode_json_object_outer(j) tmp as k,v order by id; """
        exception "errCode = 2"
    }

    // json_parse
    qt_sql_json_parse """SELECT/*+SET_VAR(enable_fold_constant_by_be=false)*/ json_parse('{"":"v1"}')"""
    qt_sql_json_parse """SELECT/*+SET_VAR(enable_fold_constant_by_be=false)*/ json_parse('{"":1, "":"v1"}')"""
    qt_sql_json_parse """SELECT/*+SET_VAR(enable_fold_constant_by_be=false)*/ json_parse('{"":1, "ab":"v1", "":"v1", "": 2}')"""
    
    // json_keys
    qt_sql_json_keys """SELECT json_keys('{"k1":"v31","k2":300}')"""
    qt_sql_json_keys """SELECT json_keys('{"a.b.c":{"k1.a1":"v31", "k2": 300},"a":"niu"}')"""
    qt_sql_json_keys """SELECT json_keys('{"a":{"k1.a1":"v31", "k2": 300},"b":"niu"}','\$.a')"""
    qt_sql_json_keys """SELECT json_keys('abc','\$.k1')"""
    qt_sql_json_keys """SELECT json_keys('["a", "b", "c"]', '\$.k2')"""
    qt_sql_json_keys """SELECT json_keys('["a", "b", "c"]')"""
    qt_sql_json_keys """SELECT json_keys('["a", "b", "c"]', '\$[1]')"""

    // error keys
    test {
        sql """ SELECT JSON_KEYS('{"a": {}, "a.b.c": {"c": 30}}', ''); """
        exception("Invalid Json Path for value")
    }

    test {
        sql """ SELECT JSON_KEYS('{"a": {}, "a.b.c": {"c": 30}}', 'a.b.c'); """
        exception("Invalid Json Path for value")
    }

    // from table
    qt_select_json_keys """SELECT id, j, json_keys(j) FROM ${testTable} ORDER BY id"""
    qt_select_json_keys """SELECT id, j, json_keys(j, '\$.k2') FROM ${testTable} ORDER BY id"""
    qt_select_json_keys """SELECT id, j, json_keys(j, '\$.a1') FROM ${testTable} ORDER BY id"""
    
    // make table with path
    sql """ DROP TABLE IF EXISTS json_keys_table;"""
    sql """
        CREATE TABLE IF NOT EXISTS json_keys_table (
            id INT,
            j JSONB,
            p STRING
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    sql """ insert into json_keys_table values (1, '{"a.b.c":{"k1.a1":"v31", "k2": 300}, "a": {}}', '\$.a'), (2, '{"a.b.c":{"k1.a1":"v31", "k2": 300}}', '\$.a.b.c'), (3, '{"a.b.c":{"k1.a1":"v31", "k2": 300}, "a": {"k1.a1": 1}}', '\$.a'), (4, '["a", "b"]', '\$.a'); """
    qt_select_json_keys """SELECT j, p, json_keys(j, p) FROM json_keys_table ORDER BY id"""
}
