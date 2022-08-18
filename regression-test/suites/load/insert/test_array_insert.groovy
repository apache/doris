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

suite("test_array_insert", "load") {
    // define a sql table
    def testTable = "tbl_test_array_insert"
    
    def create_test_table = {testTablex, enable_vectorized_flag ->
        // multi-line sql
        sql "ADMIN SET FRONTEND CONFIG ('enable_array_type' = 'true')"
        
        if (enable_vectorized_flag) {
            sql """ set enable_vectorized_engine = true """
        }

        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
              `k1` INT(11) NULL COMMENT "",
              `k2` ARRAY<LARGEINT> NULL COMMENT ""
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
        def result2 = sql "INSERT INTO ${testTable} VALUES (100, [9223372036854775808])"
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }
    
    // case1: enable_vectorized_flag = false
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table.call(testTable, false)
        sql "INSERT INTO ${testTable} VALUES (1, [117341182548128045443221445, 170141183460469231731687303715884105727])"

        // select the table and check whether the data is correct
        qt_select "select * from ${testTable} order by k1"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
    
    // case2: enable_vectorized_flag = true
    try {
        sql "DROP TABLE IF EXISTS ${testTable}"
        
        create_test_table.call(testTable, true)
        sql "INSERT INTO ${testTable} VALUES (1, [117341182548128045443221446, 170141183460469231731687303715884105727])"

        // select the table and check whether the data is correct
        qt_select "select * from ${testTable} order by k1"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable}")
    }

}