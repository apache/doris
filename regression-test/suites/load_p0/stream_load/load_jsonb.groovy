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

suite("test_load_jsonb", "p0") {
    // define a sql table
    def testTable = "tbl_test_load_jsonb"
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
}
