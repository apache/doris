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

suite("test_complex_type_unique_key", "p0") {
    // define a sql table
    def testTable = "tbl_test_complex_unique"
    def dataFile = "complex_unique_1.csv"
    def dataFile1 = "complex_unique_2.csv"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE IF NOT EXISTS tbl_test_complex_unique (
            id INT,
            a ARRAY<SMALLINT> NOT NULL COMMENT "",
            m MAP<STRING, INT(11)> NOT NULL COMMENT "",
            s STRUCT<f1:SMALLINT COMMENT 'sf1', f2:INT(11) COMMENT 'sf2'> NOT NULL COMMENT "",
            an ARRAY<DATE>,
            mn MAP<STRING, INT(11)>,
            sn STRUCT<f1:SMALLINT, f2:INT(11)>
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // insert into valid json rows
    sql """INSERT INTO ${testTable} VALUES (2, [], {"doris": 1}, {128, 32768}, ['2023-12-24'], {"amory": 7}, {12, 3276});"""
    sql """INSERT INTO ${testTable} VALUES (1, [1, 2, 3], {"k1": 10, "k2": 12}, {128, 32768}, ['2022-07-13'], NULL, NULL)"""

    // check result
    qt_select "SELECT * FROM ${testTable} ORDER BY id"

    // stream_load for same key
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
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(2, json.NumberTotalRows)
            assertEquals(2, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    sql """sync"""
    // check result : now only 2 rows
    qt_select "SELECT * FROM ${testTable} ORDER BY id"

    // stream load different key
    streamLoad {
        table testTable

        file dataFile1 // import csv file
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
            assertEquals(2, json.NumberTotalRows)
            assertEquals(2, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    sql """sync"""
    // check result : now 4 rows
    qt_select "SELECT * FROM ${testTable} ORDER BY id"
}
