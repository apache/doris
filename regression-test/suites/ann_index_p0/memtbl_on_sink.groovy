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

suite("memtbl_on_sink") {
    sql "set enable_common_expr_pushdown=true;"
    sql "drop table if exists memtbl_on_sink"
    test {
        sql """
            CREATE TABLE memtbl_on_sink (
                id INT NOT NULL COMMENT "",
                vec ARRAY<FLOAT> NOT NULL COMMENT "",
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

    sql "insert into memtbl_on_sink values (1, [1.0, 2.0, 3.0])"

    qt_sql_0 "select * from memtbl_on_sink order by id"

    // Insert with invalid array
    test {
        sql """
            INSERT INTO memtbl_on_sink VALUES (1, [1.0])
        """
        exception "[INVALID_ARGUMENT]"
    }

    sql "truncate table memtbl_on_sink;"
    sql "set enable_memtable_on_sink_node=false;"

    sql "insert into memtbl_on_sink values (1, [1.0, 2.0, 3.0])"

    qt_sql_1 "select * from memtbl_on_sink order by id"

    // Insert with invalid array
    test {
        sql """
            INSERT INTO memtbl_on_sink VALUES (1, [1.0])
        """
        exception "[INVALID_ARGUMENT]"
    }

    qt_sql_2 "select id, l2_distance_approximate(vec, [1.0, 2.0, 3.0]) as dist from memtbl_on_sink order by dist limit 2;"

}