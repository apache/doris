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

suite("test_map_load_and_function", "p0") {
    // define a sql table
    def testTable = "tbl_test_map"
    def dataFile = "test_map.csv"

    sql "DROP TABLE IF EXISTS ${testTable}"
    sql "ADMIN SET FRONTEND CONFIG ('enable_map_type' = 'true')"

    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            m Map<STRING, INT>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """

    // load the map data from csv file
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
            assertEquals("success", json.Status.toLowerCase())
            assertEquals("OK", json.Message)
            assertEquals(15, json.NumberTotalRows)
            assertTrue(json.LoadBytes > 0)

        }
    }

    // check result
    qt_select_all "SELECT * FROM ${testTable} ORDER BY id"

    // insert into valid json rows
    sql """INSERT INTO ${testTable} VALUES(17, NULL)"""
    sql """INSERT INTO ${testTable} VALUES(18, {"k1":100, "k2": 130})"""

    // map element_at
    qt_select_m "SELECT id, m['k2'] FROM ${testTable} ORDER BY id"


    testTable = "tbl_test_map2"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            `m1` MAP<STRING, INT> NULL,
            `m2` MAP<INT, STRING> NULL,
            `m3` MAP<STRING, STRING> NULL,
            `m4` MAP<INT, BIGINT> NULL,
            `m5` MAP<BIGINT, INT> NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """
    sql """INSERT INTO ${testTable} VALUES(1, {'k1':100, 'k2':200}, {100:'k1', 200:'k2'}, {'k1':'v1', 'k2':'v2'}, {100:10000, 200:20000}, {10000:100, 20000:200})"""

    // map element_at
    qt_select_m1 "SELECT id, m1['k1'], m1['k2'], m1['nokey'] FROM ${testTable} ORDER BY id"
    qt_select_m2 "SELECT id, m2[100], m2[200], m1[300] FROM ${testTable} ORDER BY id"
    qt_select_m3 "SELECT id, m3['k1'], m3['k2'], m3['nokey'] FROM ${testTable} ORDER BY id"
    qt_select_m4 "SELECT id, m4[100], m4[200], m4[300] FROM ${testTable} ORDER BY id"
    qt_select_m5 "SELECT id, m5[10000], m5[20000], m5[30000] FROM ${testTable} ORDER BY id"
}
