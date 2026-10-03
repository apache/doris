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

suite("test_utf8_varchar_truncation", "p0") {
    def tableName = "test_utf8_varchar_truncation"
    def csvFile = "test_utf8_varchar_truncation.csv"

    // ============================================================
    // Test 1: Non-strict mode — multi-byte UTF-8 strings should be
    //          truncated at valid UTF-8 character boundaries within
    //          the VARCHAR byte limit, and all rows should load
    //          successfully.
    //
    //          This covers the exact bug examples from #64334:
    //          - Chinese character '中' (3 bytes) truncation
    //          - Turkish character 'ı' U+0131 (2 bytes) truncation
    // ============================================================
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            name VARCHAR(10)
        )
        ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    streamLoad {
        table "${tableName}"
        file """${csvFile}"""
        set 'column_separator', '|'
        set 'columns', 'id, name'
        set 'strict_mode', 'false'
        set 'max_filter_ratio', '0'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            assertEquals(10, json.NumberLoadedRows)
            assertEquals(0, json.NumberFilteredRows)
        }
    }

    // Verify each row's truncated value at the UTF-8 character boundary.
    // These are the exact bug-report scenarios from issue #64334.
    //
    // Row 2: "中1234567890" (13 bytes) → 中(3B) + 1234567(7B) = 10 bytes
    def r2 = sql "SELECT name FROM ${tableName} WHERE id = 2"
    assertEquals("中1234567", r2[0][0])

    // Row 3: "abcıdefghij" (12 bytes) → abc(3B) + ı(2B) + defgh(5B) = 10 bytes
    def r3 = sql "SELECT name FROM ${tableName} WHERE id = 3"
    assertEquals("abcıdefgh", r3[0][0])

    // Row 4: "正常数据" (12 bytes) → 据(3B) crosses pos 10 boundary
    //        Walk back from continuation byte to 正(3B)+常(3B)+数(3B) = 9 bytes
    def r4 = sql "SELECT name FROM ${tableName} WHERE id = 4"
    assertEquals("正常数", r4[0][0])

    // Row 5: "abı456789中" (13 bytes) → 中 starts at pos 10, excluded
    //        ab(2B) + ı(2B) + 456789(6B) = 10 bytes
    def r5 = sql "SELECT name FROM ${tableName} WHERE id = 5"
    assertEquals("abı456789", r5[0][0])

    // Row 7: "中中中中" (12 bytes) → 4th 中 crosses pos 10
    //        Walk back: 中(3B)+中(3B)+中(3B) = 9 bytes
    def r7 = sql "SELECT name FROM ${tableName} WHERE id = 7"
    assertEquals("中中中", r7[0][0])

    // Row 8: "ııııııııııı" (11 copies, 22 bytes) → 6th ı starts at pos 10
    //        5 × ı(2B) = 10 bytes
    def r8 = sql "SELECT name FROM ${tableName} WHERE id = 8"
    assertEquals("ııııı", r8[0][0])

    // Rows within limit should remain unchanged
    def r1 = sql "SELECT name FROM ${tableName} WHERE id = 1"
    assertEquals("hello", r1[0][0])
    def r6 = sql "SELECT name FROM ${tableName} WHERE id = 6"
    assertEquals("test123456", r6[0][0])
    def r9 = sql "SELECT name FROM ${tableName} WHERE id = 9"
    assertEquals("短数据", r9[0][0])
    def r10 = sql "SELECT name FROM ${tableName} WHERE id = 10"
    assertEquals("ok", r10[0][0])

    // Regression recording: full table scan
    qt_sql_non_strict "SELECT id, name FROM ${tableName} ORDER BY id"

    sql """ DROP TABLE IF EXISTS ${tableName} """

    // ============================================================
    // Test 2: Strict mode — over-limit UTF-8 strings should be
    //          filtered out (rejected with error), same behavior
    //          as over-limit ASCII strings.
    // ============================================================
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            name VARCHAR(10)
        )
        ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    streamLoad {
        table "${tableName}"
        file """${csvFile}"""
        set 'column_separator', '|'
        set 'columns', 'id, name'
        set 'strict_mode', 'true'
        set 'max_filter_ratio', '0.6'

        check { result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            def json = parseJson(result)
            assertEquals("success", json.Status.toLowerCase())
            assertEquals(10, json.NumberTotalRows)
            // Rows 2,3,4,5,7,8 exceed the 10-byte VARCHAR limit → 6 filtered
            assertEquals(4, json.NumberLoadedRows)
            assertEquals(6, json.NumberFilteredRows)
            assertTrue(result.contains("ErrorURL"))
        }
    }

    // Verify only rows within the byte limit were loaded
    def strict_results = sql "SELECT id, name FROM ${tableName} ORDER BY id"
    assertEquals(4, strict_results.size())
    assertEquals(1, strict_results[0][0] as int)
    assertEquals("hello", strict_results[0][1])
    assertEquals(6, strict_results[1][0] as int)
    assertEquals("test123456", strict_results[1][1])
    assertEquals(9, strict_results[2][0] as int)
    assertEquals("短数据", strict_results[2][1])
    assertEquals(10, strict_results[3][0] as int)
    assertEquals("ok", strict_results[3][1])

    // Regression recording: strict mode full table scan
    qt_sql_strict "SELECT id, name FROM ${tableName} ORDER BY id"

    sql """ DROP TABLE IF EXISTS ${tableName} """
}
