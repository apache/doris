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
suite("test_materialized_view_struct", "rollup") {
    def tableName = "tbl_test_materialized_view_struct"

    def create_test_table = {testTable ->

        def result1 = sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
              `k1` int(11) NULL,
              `k2` struct<f1:smallint(6)> NULL,
              `k3` struct<f1:int(11)> NULL,
              `k4` struct<f1:bigint(20)> NULL,
              `k5` struct<f1:char(20)> NULL,
              `k6` struct<f1:varchar(50)> NULL,
              `k7` struct<f1:date> NULL,
              `k8` struct<f1:datetime> NULL,
              `k9` struct<f1:float> NULL,
              `k10` struct<f1:double> NULL,
              `k11` struct<f1:decimal(20, 6)> NULL,
              `k12` struct<f1:boolean> NULL,
              `k13` struct<f1:text> NULL
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
        def result2 = sql """ INSERT INTO ${tableName} VALUES (100, {128}, {32768}, {65535}, {'a'}, {"Doris"}, {'2022-07-13'},
                        {'2022-07-13 12:30:00'}, {0.33}, {3.1415926}, {6.67}, {true}, {'how do you do'})
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
            exception "errCode = 2, detailMessage = The STRUCT column[`mv_k2` struct<f1:smallint> NULL] not support to create materialized view"
        }
    } finally {
        try_sql("DROP TABLE IF EXISTS ${tableName}")
    }
}
