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
    def testTable01 = "tbl_test_array_insert01"
    def testTable02 = "tbl_test_array_insert02"

    def create_test_table = {testTablex, enable_vectorized_flag ->
        
        if (enable_vectorized_flag) {
            sql """ set enable_vectorized_engine = true """
        } else {
            sql """ set enable_vectorized_engine = false """
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
    
    def create_test_table01 = {testTabley ->
        
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable01} (
              `k1` int(11) NULL,
              `k2` smallint(6) NULL,
              `k3` int(11) NULL,
              `k4` bigint(20) NULL,
              `k5` char(20) NULL,
              `k6` varchar(50) NULL,
              `k7` date NULL,
              `k8` datetime NULL,
              `k9` float NULL,
              `k10` double NULL,
              `k11` decimal(20, 6) NULL,
              `k12` boolean NULL,
              `k13` text NULL
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
        def result2 = sql """ INSERT INTO ${testTable01} VALUES (200, 2, 32768, 65535, 'b', "hello", 
                        '2022-07-13', '2022-07-13 12:30:00', 0.67, 3.1415926, 6.67, false, 'happy life')
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    def create_test_table02 = {testTablez ->
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${testTable02} (
              `k1` int(11) NULL,
              `k2` array<smallint(6)> NULL,
              `k3` array<int(11)> NULL,
              `k4` array<bigint(20)> NULL,
              `k5` array<char(20)> NULL,
              `k6` array<varchar(50)> NULL,
              `k7` array<date> NULL,
              `k8` array<datetime> NULL,
              `k9` array<float> NULL,
              `k10` array<double> NULL,
              `k11` array<decimal(20, 6)> NULL,
              `k12` array<boolean> NULL,
              `k13` array<text> NULL
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
        def result2 = sql """ INSERT INTO ${testTable02} VALUES (100, [1, 2, 3], [32767, 32768, 32769], [65534, 65535, 65536], 
                        ['a', 'b', 'c'], ["hello", "world"], ['2022-07-13'], ['2022-07-13 12:30:00'], [0.33, 0.67], [3.1415926, 0.878787878],
                        [4, 5.5, 6.67], [true, false], ['happy life'])
                        """
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

    // case3: test the collect_list result to insert
    try {
        sql "DROP TABLE IF EXISTS ${testTable01}"
        sql "DROP TABLE IF EXISTS ${testTable02}"

        create_test_table01.call(testTable01)
        create_test_table02.call(testTable02)
        
        sql """INSERT INTO ${testTable02} select k1, collect_list(k2),  collect_list(k3), collect_list(k4), collect_list(k5), collect_list(k6),
            collect_list(k7), collect_list(k8), collect_list(k9), collect_list(k10), collect_list(k11), collect_list(k12), collect_list(k13) 
            from ${testTable01} group by k1;
        """
        // select the table and check whether the data is correct
        qt_select "select * from ${testTable02} order by k1"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable01}")
        try_sql("DROP TABLE IF EXISTS ${testTable02}")
    }

    // case4: test the array_sort(collect_list()) result to insert and select
    try {
        sql "DROP TABLE IF EXISTS ${testTable01}"
        sql "DROP TABLE IF EXISTS ${testTable02}"

        create_test_table01.call(testTable01)
        create_test_table02.call(testTable02)
        
        sql """INSERT INTO ${testTable02} select k1, array_sort(collect_list(k2)),  array_sort(collect_list(k3)), array_sort(collect_list(k4)), 
            array_sort(collect_list(k5)), array_sort(collect_list(k6)), array_sort(collect_list(k7)), array_sort(collect_list(k8)),
            array_sort(collect_list(k9)), array_sort(collect_list(k10)), array_sort(collect_list(k11)), array_sort(collect_list(k12)),
            array_sort(collect_list(k13)) from ${testTable01} group by k1;
        """
        // select the table and check whether the data is correct
        qt_select "select * from ${testTable02} order by k1"
        qt_select "select array_avg(k2), array_avg(k3), array_avg(k4), array_avg(k9), array_avg(k10) from ${testTable02} order by k1"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable01}")
        try_sql("DROP TABLE IF EXISTS ${testTable02}")
    }

    // case5: test to insert 'array<boolean>'
    try {
        sql "DROP TABLE IF EXISTS ${testTable02}"

        create_test_table02.call(testTable02)
        
        sql """INSERT INTO ${testTable02} VALUES (200, [1, 2, 3], [32767, 32768, 32769], [65534, 65535, 65536], 
                ['a', 'b', 'c'], ["hello", "world"], ['2022-07-13'], ['2022-07-13 12:30:00'], [0.33, 0.67], [3.1415926, 0.878787878],
                [4, 5.5, 6.67], '[1, 0, 1, 0]', ['happy life'])
            """
        // select the table and check whether the data is correct
        qt_select "select * from ${testTable02} order by k1"

    } finally {
        try_sql("DROP TABLE IF EXISTS ${testTable02}")
    }
}
