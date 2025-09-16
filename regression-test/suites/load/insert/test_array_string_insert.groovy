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

suite("test_array_string_insert", "load") {
    // define a sql table
    def testTable = "tbl_test_array_string_insert"

    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL COMMENT "",
              `k2` ARRAY<CHAR(5)> NULL COMMENT "",
              `k3` ARRAY<CHAR(5)> NOT NULL COMMENT "",
              `k4` ARRAY<ARRAY<CHAR(5)>> NULL COMMENT ""
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
    }

    def test_insert_array_string = {
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_test_table.call(testTable)

        sql "set enable_insert_strict = true"

        // ARRAY<char> too long
        def exception_str = isGroupCommitMode() ? "too many filtered rows" : "Insert has filtered data in strict mode"
        test {
            sql "INSERT INTO ${testTable} VALUES (1, ['12345','123456'], [], NULL)"
            exception exception_str
        }

        // NULL for NOT NULL column
        test {
            sql "INSERT INTO ${testTable} VALUES (2, ['12345','123'], NULL, NULL)"
            exception exception_str
        }

        // ARRAY<ARRAY<char>> too long
        test {
            sql "INSERT INTO ${testTable} VALUES (3, NULL, ['4'], [['123456'],['222']])"
            exception exception_str
        }

        // normal insert
        sql "INSERT INTO ${testTable} VALUES (4, ['12345','123'], ['4'], NULL)"
        sql "INSERT INTO ${testTable} VALUES (5, NULL, [NULL, '4'], NULL)"
        sql "INSERT INTO ${testTable} VALUES (6, NULL, ['4'], [['123'],['222']])"
        sql "INSERT INTO ${testTable} VALUES (7, NULL, ['4'], [['12345',NULL],['222']])"

        // select the table and check whether the data is correct
        qt_select "select * from ${testTable} order by k1"
        qt_select_count "select count(k2), count(k3), count(k4) from ${testTable}"
    }
    
    test_insert_array_string();
}
