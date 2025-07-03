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

suite("test_multicast_sink") {
    multi_sql """
            drop table if exists table_1_undef_partitions2_keys3_properties4_distributed_by5;
            CREATE TABLE `table_1_undef_partitions2_keys3_properties4_distributed_by5` (
              `col_int_undef_signed` int NULL,
              `col_int_undef_signed_not_null` int NOT NULL,
              `col_date_undef_signed` date NULL,
              `col_date_undef_signed_not_null` date NOT NULL,
              `col_varchar_10__undef_signed` varchar(10) NULL,
              `col_varchar_10__undef_signed_not_null` varchar(10) NOT NULL,
              `col_varchar_1024__undef_signed` varchar(1024) NULL,
              `col_varchar_1024__undef_signed_not_null` varchar(1024) NOT NULL,
              `pk` int NULL
            ) ENGINE=OLAP
            DUPLICATE KEY(`col_int_undef_signed`, `col_int_undef_signed_not_null`, `col_date_undef_signed`)
            DISTRIBUTED BY HASH(`pk`) BUCKETS 10
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
            );
            insert into table_1_undef_partitions2_keys3_properties4_distributed_by5 values(3, 6, '2023-12-17', '2023-12-17', 'ok', 'v', 'want', 'z', 0);
            set enable_nereids_distribute_planner=true;
            set parallel_pipeline_task_num = 1;
            """

    for (def i in 0..<100) {
        test {
            sql """
            WITH cte1 AS(
                SELECT t1.`pk`
                FROM table_1_undef_partitions2_keys3_properties4_distributed_by5 AS t1
                ORDER BY t1.pk
            )
            SELECT cte1.`pk` AS pk1
            FROM cte1
            LEFT OUTER JOIN cte1 AS alias1
            ON cte1 . `pk` = alias1 . `pk`
            WHERE cte1.`pk` < 3
            LIMIT 66666666
            """
            result([[0]])
        }
    }
}
