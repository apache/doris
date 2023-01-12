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

suite("test_array_show_create", "query") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_vectorized_engine=true"
    sql "SET enable_fallback_to_original_planner=false" 
    // define a sql table
    def testTable = "test_array_show_create"

    def create_test_table = {testTablex ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL COMMENT "",
              `k2` ARRAY<SMALLINT> NOT NULL COMMENT "",
              `k3` ARRAY<INT(11)> NOT NULL COMMENT "",
              `k4` ARRAY<BIGINT> NOT NULL COMMENT "",
              `k5` ARRAY<CHAR> NOT NULL COMMENT "",
              `k6` ARRAY<VARCHAR(20)> NULL COMMENT "",
              `k7` ARRAY<DATE> NOT NULL COMMENT "",
              `k8` ARRAY<DATETIME> NOT NULL COMMENT "",
              `k9` ARRAY<FLOAT> NOT NULL COMMENT "",
              `k10` ARRAY<DOUBLE> NOT NULL COMMENT "",
              `k11` ARRAY<DECIMAL(20, 6)> NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`k1`)
            DISTRIBUTED BY HASH(`k1`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
            );
            """

        // DDL/DML return 1 row and 3 column, the only value is update row count
        assertTrue(result1.size() == 1)
        assertTrue(result1[0].size() == 1)
        assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

        // insert 1 row to check whether the table is ok
        def result2 = sql """ INSERT INTO ${testTable} VALUES
                        (100, [1, 2, 3], [32767, 32768, 32769], [65534, 65535, 65536], ['a', 'b', 'c'], ["hello", "world"],
                        ['2022-07-13'], ['2022-07-13 12:30:00'], [0.33, 0.67], [3.1415926, 0.878787878], [4, 5.5, 6.67])
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        create_test_table.call(testTable)

        qt_select "show create table ${testTable}"
    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

}
