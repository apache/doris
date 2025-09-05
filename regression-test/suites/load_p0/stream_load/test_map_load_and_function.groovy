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

    sql "sync"
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
    // map nested
    qt_select_map4 "select m from (SELECT map(1000, 'k11', 2000, 'k22') as m) t"

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

    // map_contains_entry: basic tests
    qt_select_map_contains_entry1 "SELECT map_contains_entry(map('k11', 1000, 'k22', 2000), 'k11', 1000)"
    qt_select_map_contains_entry2 "SELECT map_contains_entry(map('k11', 1000, 'k22', 2000), 'k22', 2000)"
    qt_select_map_contains_entry3 "SELECT map_contains_entry(map('k11', 1000, 'k22', 2000), 'k11', 2000)"
    qt_select_map_contains_entry4 "SELECT map_contains_entry(map('k11', 1000, 'k22', 2000), 'k33', 1000)"
    qt_select_map_contains_entry5 "SELECT map_contains_entry(map('k11', 1000, 'k22', 2000), 'nokey', 1000)"
    qt_select_map_contains_entry6 "SELECT map_contains_entry(map('k11', 1000, 'k22', 2000), '', 1000)"
    qt_select_map_contains_entry7 "SELECT map_contains_entry(map('k11', 1000, 'k22', 2000), NULL, 1000)"
    qt_select_map_contains_entry8 "SELECT map_contains_entry(map('k11', 1000, 'k22', 2000), 'k11', NULL)"
    qt_select_map_contains_entry9 "SELECT map_contains_entry(map('k11', 1000, 'k22', 2000), NULL, NULL)"
    qt_select_map_contains_entry10 "SELECT map_contains_entry(map('', 0), '', 0)"
    qt_select_map_contains_entry11 "SELECT map_contains_entry(map(), 'k1', 100)"  
    qt_select_map_contains_entry12 "SELECT map_contains_entry(map('k1', 100), '', 100)"
    qt_select_map_contains_entry13 "SELECT map_contains_entry(map(NULL, 100), NULL, 100)"
    qt_select_map_contains_entry14 "SELECT map_contains_entry(map('k1', NULL), 'k1', NULL)"
    qt_select_map_contains_entry15 "SELECT map_contains_entry(map(NULL, NULL), NULL, NULL)"
    qt_select_map_contains_entry16 "SELECT map_contains_entry(NULL, NULL, NULL)"

    // map_contains_entry: tests with actual data from first table
    qt_select_map_contains_entry101 "SELECT id, m, map_contains_entry(m, 'k1', 23) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry102 "SELECT id, m, map_contains_entry(m, 'k2', 20) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry103 "SELECT id, m, map_contains_entry(m, '  11amory  ', 66) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry104 "SELECT id, m, map_contains_entry(m, 'beat', 31) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry105 "SELECT id, m, map_contains_entry(m, ' clever ', 300) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry106 "SELECT id, m, map_contains_entry(m, '  33,amory  ', 41) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry107 "SELECT id, m, map_contains_entry(m, ' bet ', 400) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry108 "SELECT id, m, map_contains_entry(m, ' cler ', 33) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry109 "SELECT id, m, map_contains_entry(m, '  1,amy  ', 2) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry110 "SELECT id, m, map_contains_entry(m, ' k2 ', 26) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry111 "SELECT id, m, map_contains_entry(m, 'null', 90) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry112 "SELECT id, m, map_contains_entry(m, '', 33) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry113 "SELECT id, m, map_contains_entry(m, NULL, 1) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry114 "SELECT id, m, map_contains_entry(m, 'nokey', 100) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry115 "SELECT id, m, map_contains_entry(m, 'k1', NULL) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry116 "SELECT id, m, map_contains_entry(m, NULL, NULL) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_var2 "SELECT id, m, map_keys(m)[1], map_values(m)[1], map_contains_entry(m, map_keys(m)[1], map_values(m)[1]) FROM ${testTable} ORDER BY id"

    // map_contains_entry: tests with duplicate keys
    qt_select_map_contains_entry_dup1 "SELECT map_contains_entry(map('k1', 100, 'k1', 200), 'k1', 100)"
    qt_select_map_contains_entry_dup2 "SELECT map_contains_entry(map('k1', 100, 'k1', 200), 'k1', 200)"
    qt_select_map_contains_entry_dup3 "SELECT map_contains_entry(map('k1', 100, 'k1', 200), 'k1', 300)"
    qt_select_map_contains_entry_dup4 "SELECT map_contains_entry(map('k1', 100, 'k2', 200, 'k1', 300), 'k1', 100)"
    qt_select_map_contains_entry_dup5 "SELECT map_contains_entry(map('k1', 100, 'k2', 200, 'k1', 300), 'k1', 300)"
    qt_select_map_contains_entry_dup6 "SELECT map_contains_entry(map('k1', 100, 'k2', 200, 'k1', 300), 'k2', 200)"
    qt_select_map_contains_entry_dup7 "SELECT map_contains_entry(map(1, 'a', 2, 'b', 1, 'c'), 1, 'a')"
    qt_select_map_contains_entry_dup8 "SELECT map_contains_entry(map(1, 'a', 2, 'b', 1, 'c'), 1, 'c')"
    qt_select_map_contains_entry_dup9 "SELECT map_contains_entry(map(1, 'a', 2, 'b', 1, 'c'), 1, 'b')"
    qt_select_map_contains_entry_dup10 "SELECT map_contains_entry(map('k1', NULL, 'k1', 100), 'k1', NULL)"
    qt_select_map_contains_entry_dup11 "SELECT map_contains_entry(map('k1', NULL, 'k1', 100), 'k1', 100)"
    qt_select_map_contains_entry_dup12 "SELECT map_contains_entry(map(NULL, 100, NULL, 200), NULL, 100)"
    qt_select_map_contains_entry_dup13 "SELECT map_contains_entry(map(NULL, 100, NULL, 200), NULL, 200)"
    qt_select_map_contains_entry_dup14 "SELECT map_contains_entry(map('a', 1, 'b', 2, 'a', 3, 'c', 4, 'a', 5), 'a', 1)"
    qt_select_map_contains_entry_dup15 "SELECT map_contains_entry(map('a', 1, 'b', 2, 'a', 3, 'c', 4, 'a', 5), 'a', 3)"
    qt_select_map_contains_entry_dup16 "SELECT map_contains_entry(map('a', 1, 'b', 2, 'a', 3, 'c', 4, 'a', 5), 'a', 5)"
    qt_select_map_contains_entry_dup17 "SELECT map_contains_entry(map('a', 1, 'b', 2, 'a', 3, 'c', 4, 'a', 5), 'a', 2)"

    // map_contains_entry: tests with time/date type
    qt_select_map_contains_entry_date1 "SELECT map_contains_entry(map('2023-01-01', CAST('2023-01-01' AS DATE), '2023-12-31', CAST('2023-12-31' AS DATE)), '2023-01-01', CAST('2023-01-01' AS DATE))"
    qt_select_map_contains_entry_date2 "SELECT map_contains_entry(map('2023-01-01', CAST('2023-01-01' AS DATE), '2023-12-31', CAST('2023-12-31' AS DATE)), '2023-01-01', CAST('2023-01-02' AS DATE))"
    qt_select_map_contains_entry_date3 "SELECT map_contains_entry(map(CAST('2023-01-01' AS DATE), 'start', CAST('2023-12-31' AS DATE), 'end'), CAST('2023-01-01' AS DATE), 'start')"
    qt_select_map_contains_entry_date4 "SELECT map_contains_entry(map(CAST('2023-01-01' AS DATE), 'start', CAST('2023-12-31' AS DATE), 'end'), CAST('2023-01-01' AS DATE), 'end')"
    qt_select_map_contains_entry_datetime1 "SELECT map_contains_entry(map('dt1', CAST('2023-01-01 10:30:00' AS DATETIME), 'dt2', CAST('2023-12-31 23:59:59' AS DATETIME)), 'dt1', CAST('2023-01-01 10:30:00' AS DATETIME))"
    qt_select_map_contains_entry_datetime2 "SELECT map_contains_entry(map('dt1', CAST('2023-01-01 10:30:00' AS DATETIME), 'dt2', CAST('2023-12-31 23:59:59' AS DATETIME)), 'dt1', CAST('2023-01-01 10:30:01' AS DATETIME))"
    qt_select_map_contains_entry_datetime3 "SELECT map_contains_entry(map(CAST('2023-01-01 10:30:00' AS DATETIME), 'morning', CAST('2023-12-31 23:59:59' AS DATETIME), 'night'), CAST('2023-01-01 10:30:00' AS DATETIME), 'morning')"
    qt_select_map_contains_entry_datetime4 "SELECT map_contains_entry(map(CAST('2023-01-01 10:30:00' AS DATETIME), 'morning', CAST('2023-12-31 23:59:59' AS DATETIME), 'night'), CAST('2023-01-01 10:30:00' AS DATETIME), 'night')"
    qt_select_map_contains_entry_datev2_1 "SELECT map_contains_entry(map('start', datev2('2023-01-01'), 'end', datev2('2023-12-31')), 'start', datev2('2023-01-01'))"
    qt_select_map_contains_entry_datev2_2 "SELECT map_contains_entry(map('start', datev2('2023-01-01'), 'end', datev2('2023-12-31')), 'start', datev2('2023-01-02'))"
    qt_select_map_contains_entry_datev2_3 "SELECT map_contains_entry(map(datev2('2023-01-01'), 100, datev2('2023-12-31'), 200), datev2('2023-01-01'), 100)"
    qt_select_map_contains_entry_datev2_4 "SELECT map_contains_entry(map(datev2('2023-01-01'), 100, datev2('2023-12-31'), 200), datev2('2023-01-01'), 200)"
    qt_select_map_contains_entry_datetimev2_1 "SELECT map_contains_entry(map('event1', CAST('2023-01-01 10:30:00.123' AS DATETIMEV2(3)), 'event2', CAST('2023-12-31 23:59:59.999' AS DATETIMEV2(3))), 'event1', CAST('2023-01-01 10:30:00.123' AS DATETIMEV2(3)))"
    qt_select_map_contains_entry_datetimev2_2 "SELECT map_contains_entry(map('event1', CAST('2023-01-01 10:30:00.123' AS DATETIMEV2(3)), 'event2', CAST('2023-12-31 23:59:59.999' AS DATETIMEV2(3))), 'event1', CAST('2023-01-01 10:30:00.124' AS DATETIMEV2(3)))"
    qt_select_map_contains_entry_datetimev2_3 "SELECT map_contains_entry(map(CAST('2023-01-01 10:30:00.123' AS DATETIMEV2(3)), 'A', CAST('2023-12-31 23:59:59.999' AS DATETIMEV2(3)), 'B'), CAST('2023-01-01 10:30:00.123' AS DATETIMEV2(3)), 'A')"
    qt_select_map_contains_entry_datetimev2_4 "SELECT map_contains_entry(map(CAST('2023-01-01 10:30:00.123' AS DATETIMEV2(3)), 'A', CAST('2023-12-31 23:59:59.999' AS DATETIMEV2(3)), 'B'), CAST('2023-01-01 10:30:00.123' AS DATETIMEV2(3)), 'B')"

    // map_contains_entry: test all type combination (N^2 auto-generated)
    def allTypes = [
        [type: "STRING", testKey: "'str1'", testValue: "'str2'"],
        [type: "BOOLEAN", testKey: "true", testValue: "false"],
        [type: "TINYINT", testKey: "CAST(1 AS TINYINT)", testValue: "CAST(2 AS TINYINT)"],
        [type: "SMALLINT", testKey: "CAST(100 AS SMALLINT)", testValue: "CAST(200 AS SMALLINT)"],
        [type: "INT", testKey: "CAST(1000 AS INT)", testValue: "CAST(2000 AS INT)"],
        [type: "BIGINT", testKey: "CAST(10000 AS BIGINT)", testValue: "CAST(20000 AS BIGINT)"],
        [type: "LARGEINT", testKey: "CAST(100000 AS LARGEINT)", testValue: "CAST(200000 AS LARGEINT)"],
        [type: "FLOAT", testKey: "CAST(1.1 AS FLOAT)", testValue: "CAST(2.2 AS FLOAT)"],
        [type: "DOUBLE", testKey: "CAST(10.1 AS DOUBLE)", testValue: "CAST(20.2 AS DOUBLE)"],
        [type: "DATE", testKey: "CAST('2023-01-01' AS DATE)", testValue: "CAST('2023-12-31' AS DATE)"],
        [type: "DATETIME", testKey: "CAST('2023-01-01 10:30:00' AS DATETIME)", testValue: "CAST('2023-12-31 23:59:59' AS DATETIME)"],
        [type: "DATEV2", testKey: "datev2('2023-01-01')", testValue: "datev2('2023-12-31')"],
        [type: "DATETIMEV2(3)", testKey: "CAST('2023-01-01 10:30:00.123' AS DATETIMEV2(3))", testValue: "CAST('2023-12-31 23:59:59.999' AS DATETIMEV2(3))"],
        [type: "CHAR(5)", testKey: "CAST('abc' AS CHAR(5))", testValue: "CAST('xyz' AS CHAR(5))"],
        [type: "VARCHAR(10)", testKey: "CAST('hello' AS VARCHAR(10))", testValue: "CAST('world' AS VARCHAR(10))"],
        [type: "IPV4", testKey: "CAST('192.168.1.1' AS IPV4)", testValue: "CAST('192.168.1.2' AS IPV4)"],
        [type: "IPV6", testKey: "CAST('2001:db8::1' AS IPV6)", testValue: "CAST('2001:db8::2' AS IPV6)"],
    ]

    def testCounter = 1
    for (int i = 0; i < allTypes.size(); i++) {
        for (int j = 0; j < allTypes.size(); j++) {
            def keyType = allTypes[i]
            def valueType = allTypes[j]
            def testName = "qt_select_map_contains_entry_type_combo_${testCounter}"
            def mapExpr = "map(${keyType.testKey}, ${valueType.testValue})"
            def sqlQuery = "SELECT map_contains_entry(${mapExpr}, ${keyType.testKey}, ${valueType.testValue})"
            
            def sqlResult = sql sqlQuery
            assertTrue(sqlResult[0][0], "MapContainsEntry: Combination ${testCounter} failed, KeyType: ${keyType.type}, ValueType: ${valueType.type}")
            testCounter++
        }
    }

    // map_contains_entry: decimal-only tests
    def decimalTypes = [
        [type: "DECIMALV2(10,2)", testKey: "CAST(123.45 AS DECIMALV2(10,2))", testValue: "CAST(234.56 AS DECIMALV2(10,2))"],
        [type: "DECIMAL(7,3)",    testKey: "CAST(123.456 AS DECIMAL(7,3))",    testValue: "CAST(234.567 AS DECIMAL(7,3))"],
        [type: "DECIMAL(10,2)",   testKey: "CAST(123.45 AS DECIMAL(10,2))",    testValue: "CAST(234.56 AS DECIMAL(10,2))"],
        [type: "DECIMAL(30,10)",  testKey: "CAST(12345.6789 AS DECIMAL(30,10))", testValue: "CAST(23456.7891 AS DECIMAL(30,10))"]
    ]

    for (def d : decimalTypes) {
        def mapExpr = "map(${d.testKey}, ${d.testValue})"
        def sqlQuery = "SELECT map_contains_entry(${mapExpr}, ${d.testKey}, ${d.testValue})"
        def sqlResult = sql sqlQuery
        assertTrue(sqlResult[0][0], "MapContainsEntry: Decimal tests failed for ${d.type}")
    }

    // map_entries: basic tests
    qt_select_map_entries1 "SELECT map_entries(map('k11', 1000, 'k22', 2000))"
    qt_select_map_entries2 "SELECT map_entries(map())"
    qt_select_map_entries3 "SELECT map_entries(map('k1', 100))"
    qt_select_map_entries4 "SELECT map_entries(map('', 0))"
    qt_select_map_entries5 "SELECT map_entries(map(NULL, 100))"
    qt_select_map_entries6 "SELECT map_entries(map('k1', NULL))"
    qt_select_map_entries7 "SELECT map_entries(map(NULL, NULL))"
    qt_select_map_entries8 "SELECT map_entries(NULL)"

    // map_entries: tests with actual data from first table
    qt_select_map_entries_table1 "SELECT id, m, map_entries(m) FROM ${testTable} ORDER BY id"

    // map_entries: tests with duplicate keys
    qt_select_map_entries_dup1 "SELECT map_entries(map('k1', 100, 'k1', 200))"
    qt_select_map_entries_dup2 "SELECT map_entries(map('k1', 100, 'k2', 200, 'k1', 300))"
    qt_select_map_entries_dup3 "SELECT map_entries(map(1, 'a', 2, 'b', 1, 'c'))"
    qt_select_map_entries_dup4 "SELECT map_entries(map('k1', NULL, 'k1', 100))"
    qt_select_map_entries_dup5 "SELECT map_entries(map(NULL, 100, NULL, 200))"
    qt_select_map_entries_dup6 "SELECT map_entries(map('k1', 100, 'k2', 200, 'k2', 300, 'k1', 400))"
    qt_select_map_entries_dup7 "SELECT map_entries(map('', 100, '', 200))"
    qt_select_map_entries_dup8 "SELECT map_entries(map('k1', 'v1', 'k1', 'v2', 'k1', 'v3'))"
    qt_select_map_entries_dup9 "SELECT map_entries(map('a', 1, 'b', 2, 'a', 3, 'c', 4, 'a', 5))"

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

    // map_contains_entry: tests with different data types
    qt_select_map_contains_entry_m1_1 "SELECT id, m1, map_contains_entry(m1, 'k1', 100) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m1_2 "SELECT id, m1, map_contains_entry(m1, 'k2', 200) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m1_3 "SELECT id, m1, map_contains_entry(m1, 'k1', 200) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m1_4 "SELECT id, m1, map_contains_entry(m1, 'k3', 100) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m1_5 "SELECT id, m1, map_contains_entry(m1, NULL, 100) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m1_6 "SELECT id, m1, map_contains_entry(m1, 'k1', NULL) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m2_1 "SELECT id, m2, map_contains_entry(m2, 100, 'k1') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m2_2 "SELECT id, m2, map_contains_entry(m2, 200, 'k2') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m2_3 "SELECT id, m2, map_contains_entry(m2, 100, 'k2') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m2_4 "SELECT id, m2, map_contains_entry(m2, 300, 'k1') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m2_5 "SELECT id, m2, map_contains_entry(m2, NULL, 'k1') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m2_6 "SELECT id, m2, map_contains_entry(m2, 100, NULL) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m3_1 "SELECT id, m3, map_contains_entry(m3, 'k1', 'v1') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m3_2 "SELECT id, m3, map_contains_entry(m3, 'k2', 'v2') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m3_3 "SELECT id, m3, map_contains_entry(m3, 'k1', 'v2') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m3_4 "SELECT id, m3, map_contains_entry(m3, 'k3', 'v1') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m3_5 "SELECT id, m3, map_contains_entry(m3, NULL, 'v1') FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m3_6 "SELECT id, m3, map_contains_entry(m3, 'k1', NULL) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m4_1 "SELECT id, m4, map_contains_entry(m4, 100, 10000) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m4_2 "SELECT id, m4, map_contains_entry(m4, 200, 20000) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m4_3 "SELECT id, m4, map_contains_entry(m4, 100, 20000) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m4_4 "SELECT id, m4, map_contains_entry(m4, 300, 10000) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m4_5 "SELECT id, m4, map_contains_entry(m4, NULL, 10000) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m4_6 "SELECT id, m4, map_contains_entry(m4, 100, NULL) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m5_1 "SELECT id, m5, map_contains_entry(m5, 10000, 100) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m5_2 "SELECT id, m5, map_contains_entry(m5, 20000, 200) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m5_3 "SELECT id, m5, map_contains_entry(m5, 10000, 200) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m5_4 "SELECT id, m5, map_contains_entry(m5, 30000, 100) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m5_5 "SELECT id, m5, map_contains_entry(m5, NULL, 100) FROM ${testTable} ORDER BY id"
    qt_select_map_contains_entry_m5_6 "SELECT id, m5, map_contains_entry(m5, 10000, NULL) FROM ${testTable} ORDER BY id"

    // map_entries: tests with different data types
    qt_select_map_entries_m1 "SELECT id, m1, map_entries(m1) FROM ${testTable} ORDER BY id"
    qt_select_map_entries_m2 "SELECT id, m2, map_entries(m2) FROM ${testTable} ORDER BY id"
    qt_select_map_entries_m3 "SELECT id, m3, map_entries(m3) FROM ${testTable} ORDER BY id"
    qt_select_map_entries_m4 "SELECT id, m4, map_entries(m4) FROM ${testTable} ORDER BY id"
    qt_select_map_entries_m5 "SELECT id, m5, map_entries(m5) FROM ${testTable} ORDER BY id"

    // Tests for NULL values
    testTable = "tbl_test_map_function_null"
    sql "DROP TABLE IF EXISTS ${testTable}"
    sql """
        CREATE TABLE IF NOT EXISTS ${testTable} (
            id INT,
            `m1` MAP<STRING, INT> NULL,
            `m2` MAP<INT, STRING> NULL,
            `m3` MAP<STRING, STRING> NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 10
        PROPERTIES("replication_num" = "1");
        """
    sql """INSERT INTO ${testTable} VALUES(1, {'k1':100, 'k2':200}, {100:'k1', 200:'k2'}, {'k1':'v1', 'k2':'v2'})"""
    sql """INSERT INTO ${testTable} VALUES(2, NULL, NULL, NULL)"""

    // map_contains_entry: tests with NULL maps
    qt_select_map_contains_entry_null "SELECT id, map_contains_entry(m1, 'k1', 100), map_contains_entry(m2, 100, 'k1'), map_contains_entry(m3, 'k1', 'v1') FROM ${testTable} ORDER BY id"

    // map_entries: tests with NULL maps
    qt_select_map_entries_null "SELECT id, map_entries(m1), map_entries(m2), map_entries(m3) FROM ${testTable} ORDER BY id"
}
