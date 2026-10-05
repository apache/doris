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

suite("ann_index_on_mow") {
    sql "drop table if exists ann_index_on_mow"
    test {
        sql """
            CREATE TABLE ann_index_on_mow (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                value INT NULL COMMENT "",
                INDEX ann_idx (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance",
                    "dim" = "3"
                )
            ) ENGINE=OLAP
            UNIQUE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            );
        """
    }

    sql "insert into ann_index_on_mow values (1, [1.0, 2.0, 3.0], 11),(2, [4.0, 5.0, 6.0], 22),(3, [7.0, 8.0, 9.0], 33)"

    qt_sql_1 "select * from ann_index_on_mow order by id"

    qt_sql_2 "select id, l2_distance_approximate(vec, [1.0, 2.0, 3.0]) as dist from ann_index_on_mow order by dist limit 1;"

    sql "insert into ann_index_on_mow values (1, [10.0, 20.0, 30.0], 111),(2, [40.0, 50.0, 60.0], 222),(3, [70.0, 80.0, 90.0], 333)"

    qt_sql_3 "select * from ann_index_on_mow order by id"

    qt_sql_4 "select id, l2_distance_approximate(vec, [10.0, 20.0, 30.0]) as dist from ann_index_on_mow order by dist limit 1;"

    qt_sql_5 "select * from ann_index_on_mow order by id"

    sql "insert into ann_index_on_mow (id, vec, __DORIS_DELETE_SIGN__) values (1, [10.0, 20.0, 30.0], 1),(2, [40.0, 50.0, 60.0], 1),(3, [70.0, 80.0, 90.0], 1);"

    qt_sql_6 "select * from ann_index_on_mow order by id"

    sql "set show_hidden_columns=true;"
    qt_sql_7 "select * from ann_index_on_mow order by id"
    sql "set show_hidden_columns=false;"

    qt_sql_8 "select id, l2_distance_approximate(vec, [10.0, 20.0, 30.0]) as dist from ann_index_on_mow order by dist limit 1;"

    sql "drop table ann_index_on_mow;"
}
