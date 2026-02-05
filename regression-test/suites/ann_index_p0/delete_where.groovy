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

suite("delete_where_with_ann") {
    sql "set enable_common_expr_pushdown=true;"
    sql "drop table if exists delete_where_with_ann"
    test {
        sql """
            CREATE TABLE delete_where_with_ann (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
                value INT NULL COMMENT "",
                INDEX ann_idx (vec) USING ANN PROPERTIES(
                    "index_type" = "hnsw",
                    "metric_type" = "l2_distance",
                    "dim" = "3"
                )
            ) ENGINE=OLAP
            DUPLICATE KEY(id) COMMENT "OLAP"
            DISTRIBUTED BY HASH(id) BUCKETS AUTO
            PROPERTIES (
                "replication_num" = "1"
            );
        """
    }

    sql "insert into delete_where_with_ann values (1, [1.0, 2.0, 3.0], 11),(2, [4.0, 5.0, 6.0], 22),(3, [7.0, 8.0, 9.0], 33)"

    qt_sql_1 "select * from delete_where_with_ann order by id"

    sql "delete from delete_where_with_ann where id = 1"
    
    qt_sql_2 "select * from delete_where_with_ann order by id"

    qt_sql_3 "select id, l2_distance_approximate(vec, [1.0, 2.0, 3.0]) as dist from delete_where_with_ann order by dist limit 2;"
}