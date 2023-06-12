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

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Paths

suite("test_array_nested_array_load", "p0") {
    // define a sql table
    def testTable = "test_array_nested_array_load"
    def dataFile = "test_array_nested_array_load.csv"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            a_int ARRAY<INT(11)>,
            a_a_int ARRAY<ARRAY<INT(11)>>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // load the map data from csv file
    streamLoad {
        table testTable

        file dataFile // import csv file
        set 'max_filter_ratio', '0.6'

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
            assertEquals("OK", json.Message)
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows + json.NumberFilteredRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)

        }
    }

    sql "sync"
    // check result
    qt_select_all "SELECT * FROM ${testTable} ORDER BY id"

    // define a sql table
    def testTable_1 = "test_array_nested_array_more_load"
    def dataFile_1 = "test_array_nested_array_more_load.csv"

    sql "DROP TABLE IF EXISTS ${testTable_1}"

    sql """
            CREATE TABLE IF NOT EXISTS ${testTable_1} (
                id INT,
                a_a_int ARRAY<ARRAY<ARRAY<ARRAY<STRING>>>>
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 10
            PROPERTIES("replication_num" = "1");
            """

    // load the map data from csv file
    streamLoad {
        table testTable_1

        file dataFile_1 // import csv file
        set 'max_filter_ratio', '0.6'
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
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows + json.NumberFilteredRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)

        }
    }

    sql "sync"
    // check result
    qt_select_all "SELECT * FROM ${testTable_1} ORDER BY id"
}
