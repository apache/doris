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

suite("test_map_show_create", "query") {
    // define a sql table
    def testTable = "test_map_show_create"

    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL,
              `k2` MAP<SMALLINT(6), STRING> NULL,
              `k3` MAP<INT(11), STRING> NULL,
              `k4` MAP<DATE, INT> NULL,
              `k5` MAP<DATETIME, STRING> NULL,
              `k6` Map<FLOAT, INT> NULL
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

        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable} VALUES (100, {1: '1', 2: '2', 3:'3'}, 
                        {32767: '32767', 32768: '32768', 32769: '32769'}, {'2022-07-13': 1}, 
                        {'2022-07-13 12:30:00': '2022-07-13 12:30:00'}, {0.33: 33, 0.67: 67})
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_test_table.call(testTable)

        def res = sql "SHOW CREATE TABLE ${testTable}"
        assertTrue(res.size() != 0)
        qt_select "select count(k2), count(k3), count(k4), count(k5), count(k6) from ${testTable}"
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

}
