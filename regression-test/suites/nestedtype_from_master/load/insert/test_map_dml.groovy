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

suite("test_map_dml", "load") {
    // define a sql table
    def testTable = "tbl_test_map_string_int"
    def testTable01 = "tbl_test_map_normal"

    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL COMMENT "",
              `k2` Map<STRING, INT> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            )
            """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
        
        // insert 1 row to check whether the table is ok
        def result2 = sql "INSERT INTO ${testTable} VALUES (6, {'amory': 6, 'is': 38, 'cl': 0})"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def create_test_table01 = {testTablez ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable01} (
              `k1` int(11) NULL,
              `k2` Map<smallint(6), string> NULL,
              `k3` Map<int(11), string> NULL,
              `k4` array<bigint(20)> NULL,
              `k5` Map<date, int> NULL,
              `k6` Map<datetime, string> NULL,
              `k7` Map<float, int> NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2",
            "disable_auto_compaction" = "false"
            )
            """
        
        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")
        

        def result2 = sql """ INSERT INTO ${testTable01} VALUES (100, {1: '1', 2: '2', 3:'3'}, {32767: '32767', 32768: '32768', 32769: '32769'},
                        [65534, 65535, 65536], {'2022-07-13': 1}, {'2022-07-13 12:30:00': '2022-07-13 12:30:00'},
                        {0.33: 33, 0.67: 67})
                        """

        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }


    // case1: string_int for map
    try {
        def res = sql "DROP TABLE IF EXISTS ${testTable}"
        create_test_table.call(testTable)
        sql "INSERT INTO ${testTable} VALUES (1, {' amory ': 6, 'happy': 38})"

        // select the table and check whether the data is correct
        qt_select "SELECT * FROM ${testTable} ORDER BY k1"
        qt_select_count "SELECT COUNT(k1) FROM ${testTable}"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

    // case2: normal key val type for map
    try {
        def res = sql "DROP TABLE IF EXISTS ${testTable01}"

        create_test_table01.call(testTable)
        // select the table and check whether the data is correct
        qt_select "SELECT * FROM ${testTable01} ORDER BY k1"
        qt_select "SELECT COUNT(k2), COUNT(k3), COUNT(k4),COUNT(k5),COUNT(k6), COUNT(k7) FROM ${testTable01}"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable01}")
    }
}
