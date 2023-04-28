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
    def testTable = "tbl_test_map_function"
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

    // check result
    qt_select2 "SELECT * FROM ${testTable} ORDER BY id"

    // map construct
    qt_select_map1 "SELECT map('k11', 1000, 'k22', 2000)"
    qt_select_map2 "SELECT map(1000, 'k11', 2000, 'k22')"
    qt_select_map3 "SELECT map()"

    // map element_at
    qt_select_element1 "SELECT map('k11', 1000, 'k22', 2000)['k11']"
    qt_select_element2 "SELECT map('k11', 1000, 'k22', 2000)['k22']"
    qt_select_element3 "SELECT map('k11', 1000, 'k22', 2000)['nokey']"
    qt_select_element4 "SELECT map('k11', 1000, 'k22', 2000)['']"
    qt_select_element5 "SELECT map(1000, 'k11', 2000, 'k22')[1000]"
    qt_select_element6 "SELECT map(1000, 'k11', 2000, 'k22')[2000]"
    qt_select_element7 "SELECT map(1000, 'k11', 2000, 'k22')[3000]"
    qt_select_element8 "SELECT map('k11', 1000, 'k22', 2000)[NULL]"
    qt_select_element9 "SELECT {'k11':1000, 'k22':2000}['k11']"
    qt_select_element10 "SELECT {'k11':1000, 'k22':2000}['k22']"
    qt_select_element11 "SELECT {'k11':1000, 'k22':2000}['nokey']"
    qt_select_element12 "SELECT {'k11':1000, 'k22':2000}[NULL]"
    qt_select_element13 "SELECT {1000:'k11', 2000:'k22'}[1000]"
    qt_select_element14 "SELECT {1000:'k11', 2000:'k22'}[2000]"
    qt_select_element15 "SELECT {1000:'k11', 2000:'k22'}[3000]"
    qt_select_element16 "SELECT {1000:'k11', 2000:'k22'}[NULL]"
    qt_select_element101 "SELECT id, m, m['k1'] FROM ${testTable} ORDER BY id"
    qt_select_element102 "SELECT id, m, m['k2'] FROM ${testTable} ORDER BY id"
    qt_select_element103 "SELECT id, m, m['  11amory  '] FROM ${testTable} ORDER BY id"
    qt_select_element104 "SELECT id, m, m['beat'] FROM ${testTable} ORDER BY id"
    qt_select_element105 "SELECT id, m, m[' clever '] FROM ${testTable} ORDER BY id"
    qt_select_element106 "SELECT id, m, m['  33,amory  '] FROM ${testTable} ORDER BY id"
    qt_select_element107 "SELECT id, m, m[' bet '] FROM ${testTable} ORDER BY id"
    qt_select_element108 "SELECT id, m, m[' cler '] FROM ${testTable} ORDER BY id"
    qt_select_element109 "SELECT id, m, m['  1,amy  '] FROM ${testTable} ORDER BY id"
    qt_select_element110 "SELECT id, m, m[' k2 '] FROM ${testTable} ORDER BY id"
    qt_select_element111 "SELECT id, m, m['null'] FROM ${testTable} ORDER BY id"
    qt_select_element112 "SELECT id, m, m[''] FROM ${testTable} ORDER BY id"
    qt_select_element113 "SELECT id, m, m[NULL] FROM ${testTable} ORDER BY id"
    qt_select_element114 "SELECT id, m, m['nokey'] FROM ${testTable} ORDER BY id"

    // map size
    qt_select_map_size1 "SELECT map_size(map('k11', 1000, 'k22', 2000))"
    qt_select_map_size2 "SELECT id, m, map_size(m) FROM ${testTable} ORDER BY id"

    // map_contains_key
    qt_select_map_contains_key1 "SELECT map_contains_key(map('k11', 1000, 'k22', 2000), 'k11')"
    qt_select_map_contains_key2 "SELECT map_contains_key(map('k11', 1000, 'k22', 2000), 'k22')"
    qt_select_map_contains_key3 "SELECT map_contains_key(map('k11', 1000, 'k22', 2000), 'nokey')"
    qt_select_map_contains_key4 "SELECT map_contains_key(map('k11', 1000, 'k22', 2000), '')"
    qt_select_map_contains_key5 "SELECT map_contains_key(map('k11', 1000, 'k22', 2000), NULL)"
    qt_select_map_contains_key101 "SELECT id, m, map_contains_key(m, 'k1') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key102 "SELECT id, m, map_contains_key(m, 'k2') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key103 "SELECT id, m, map_contains_key(m, '  11amory  ') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key104 "SELECT id, m, map_contains_key(m, 'beat') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key105 "SELECT id, m, map_contains_key(m, ' clever ') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key106 "SELECT id, m, map_contains_key(m, '  33,amory  ') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key107 "SELECT id, m, map_contains_key(m, ' bet ') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key108 "SELECT id, m, map_contains_key(m, ' cler ') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key109 "SELECT id, m, map_contains_key(m, '  1,amy  ') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key110 "SELECT id, m, map_contains_key(m, ' k2 ') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key111 "SELECT id, m, map_contains_key(m, 'null') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key112 "SELECT id, m, map_contains_key(m, '') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key113 "SELECT id, m, map_contains_key(m, NULL) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_key114 "SELECT id, m, map_contains_key(m, 'nokey') FROM ${testTable} ORDER BY id"

    // map_contains_value
    qt_select_map_contains_value1 "SELECT map_contains_value(map('k11', 1000, 'k22', 2000), 1000)"
    qt_select_map_contains_value2 "SELECT map_contains_value(map('k11', 1000, 'k22', 2000), 2000)"
    qt_select_map_contains_value3 "SELECT map_contains_value(map('k11', 1000, 'k22', 2000), 100)"
    qt_select_map_contains_value4 "SELECT map_contains_value(map('k11', 1000, 'k22', 2000), NULL)"
    qt_select_map_contains_value101 "SELECT id, m, map_contains_value(m, 23) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value102 "SELECT id, m, map_contains_value(m, 20) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value103 "SELECT id, m, map_contains_value(m, 66) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value104 "SELECT id, m, map_contains_value(m, 31) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value105 "SELECT id, m, map_contains_value(m, 300) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value106 "SELECT id, m, map_contains_value(m, 41) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value107 "SELECT id, m, map_contains_value(m, 400) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value108 "SELECT id, m, map_contains_value(m, 33) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value109 "SELECT id, m, map_contains_value(m, 2) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value110 "SELECT id, m, map_contains_value(m, 26) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value111 "SELECT id, m, map_contains_value(m, 90) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value112 "SELECT id, m, map_contains_value(m, 33) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value113 "SELECT id, m, map_contains_value(m, 1) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value114 "SELECT id, m, map_contains_value(m, 2) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value115 "SELECT id, m, map_contains_value(m, 0) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value116 "SELECT id, m, map_contains_value(m, -1) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_value117 "SELECT id, m, map_contains_value(m, NULL) FROM ${testTable} ORDER BY id"

    // map_keys
    qt_select_map_keys1 "SELECT map_keys(map('k11', 1000, 'k22', 2000))"
    qt_select_map_keys2 "SELECT id, m, map_keys(m) FROM ${testTable} ORDER BY id"

    // map_values
    qt_select_map_values1 "SELECT map_values(map('k11', 1000, 'k22', 2000))"
    qt_select_map_values2 "SELECT id, m, map_values(m) FROM ${testTable} ORDER BY id"


    testTable = "tbl_test_map_function2"
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
    qt_select_count "SELECT COUNT(m1), COUNT(m2), COUNT(m3), COUNT(m4), COUNT(m5)  FROM ${testTable}"
}
