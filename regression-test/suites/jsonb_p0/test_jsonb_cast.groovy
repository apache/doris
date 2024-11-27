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

suite("test_jsonb_cast", "p0") {

    // define a sql table with array<text> which has some Escape Character and should also to cast to json
    def testTable = "tbl_test_array_text_cast_jsonb"
    def dataFile = "test_jsonb_cast.csv"

    sql "DROP TABLE IF EXISTS ${testTable}"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            a ARRAY<TEXT>,
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 3
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
            assertEquals(4, json.NumberTotalRows)
            assertEquals(4, json.NumberLoadedRows)
            assertTrue(json.LoadBytes > 0)
        }
    }

    sql """ sync; """

    // check result
    qt_select_1 "SELECT * FROM ${testTable} ORDER BY id"


    // insert into valid json rows
    sql """INSERT INTO ${testTable} VALUES(27, ['{"k1":"v1", "k2":200}'])"""
    sql """INSERT INTO ${testTable} VALUES(28, ['{"a.b.c":{"k1.a1":"v31", "k2":300},"a":"niu"}'])"""
    sql """INSERT INTO ${testTable} VALUES(29, ['\f\n\r', "\f\n\r"])"""
    sql """INSERT INTO ${testTable} VALUES(30, ["\\f\\b\\r\\n", '\\f\\b\\r\\n"'])"""

    // check result
    qt_select_2 "SELECT * FROM ${testTable} ORDER BY id"
    // check cast as json
    qt_select_json "SELECT id, cast(a as JSON) FROM ${testTable} ORDER BY id"
}
