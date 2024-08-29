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
suite("test_materialized_view_array", "rollup") {
    def tableName = "tbl_test_materialized_view_array"

    def create_test_table = {testTable ->
        
        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
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
        def result2 = sql """ INSERT INTO ${tableName} VALUES (100, [1, 2, 3], [32767, 32768, 32769], [65534, 65535, 65536], 
                        ['a', 'b', 'c'], ["hello", "world"], ['2022-07-13'], ['2022-07-13 12:30:00'], [0.33, 0.67], [3.1415926, 0.878787878],
                        [4, 5.5, 6.67], [true, false], ['happy life'])
                        """
        assertTrue(result2.size() == 1)
        assertTrue(result2[0].size() == 1)
        assertTrue(result2[0][0] == 1, "Insert should update 1 rows")
    }

    try {
        sql "DROP TABLE IF EXISTS ${tableName}"
        
        create_test_table.call(tableName)
        test {
            sql "CREATE MATERIALIZED VIEW idx AS select k2,k1, k3, k4, k5 from ${tableName}"
            exception "errCode = 2, detailMessage = The ARRAY column[`mv_k2` array<smallint> NULL] not support to create materialized view"
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
