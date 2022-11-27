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

suite("test_jsonb_load_and_function", "p0") {
    // define a sql table
    def testTable = "tbl_test_jsonb"
    def dataFile = "test_jsonb.csv"

    sql """ set enable_vectorized_engine = true """

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
    // fail by default for invalid data rows
    streamLoad {
        table testTable
        
        file dataFile // import csv file
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
            assertEquals("too many filtered rows", json.Message)
            assertEquals(25, json.NumberTotalRows)
            assertEquals(18, json.NumberLoadedRows)
            assertEquals(7, json.NumberFilteredRows)
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

        // if declared a check callback, the default check condition will ignore.
        // So you must check all condition
        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(25, json.NumberTotalRows)
            assertEquals(18, json.NumberLoadedRows)
            assertEquals(7, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    // check result
    qt_select "SELECT * FROM ${testTable} ORDER BY id"

    // insert into 1 row and then check result
    sql """INSERT INTO ${testTable} VALUES(26, '{"k1":"v1", "k2": 200}')"""
    qt_select "SELECT * FROM ${testTable} ORDER BY id"

    // jsonb_extract
    qt_select "SELECT id, j, jsonb_extract(j, '\$') FROM ${testTable} ORDER BY id"

    qt_select "SELECT id, j, jsonb_extract(j, '\$.k1') FROM ${testTable} ORDER BY id"
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

    // jsonb_extract_string
    qt_select "SELECT id, j, jsonb_extract_string(j, '\$') FROM ${testTable} ORDER BY id"

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

    // jsonb_extract_int
    qt_select "SELECT id, j, jsonb_extract_int(j, '\$') FROM ${testTable} ORDER BY id"

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

    // jsonb_extract_bigint
    qt_select "SELECT id, j, jsonb_extract_bigint(j, '\$') FROM ${testTable} ORDER BY id"

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


    // jsonb_extract_double
    qt_select "SELECT id, j, jsonb_extract_double(j, '\$') FROM ${testTable} ORDER BY id"

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

    // jsonb_extract_bool
    qt_select "SELECT id, j, jsonb_extract_bool(j, '\$') FROM ${testTable} ORDER BY id"

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

    // jsonb_extract_isnull
    qt_select "SELECT id, j, jsonb_extract_isnull(j, '\$') FROM ${testTable} ORDER BY id"

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

    // jsonb_exists_path
    qt_select "SELECT id, j, jsonb_exists_path(j, '\$') FROM ${testTable} ORDER BY id"

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

    // jsonb_type
    qt_select "SELECT id, j, jsonb_type(j, '\$') FROM ${testTable} ORDER BY id"

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
}
