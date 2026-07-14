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

// Regression test for https://github.com/apache/doris/issues/57348
// RewriteCteChildren used to NPE on producerOutputs because the
// second-pass RewriteCteChildren ran after ClearContextStatus wiped
// cteIdToOutputIds without re-collecting it.
suite("test_nested_cte_with_set_op") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"

    sql "drop table if exists test_sales_57348"

    sql """
        CREATE TABLE `test_sales_57348` (
            `id` int,
            `item` varchar(64),
            `amount` double,
            `year` int,
            `month` int
        ) ENGINE=OLAP
        DUPLICATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into test_sales_57348 values
            (1, 'a', 10.0, 2023, 1),
            (2, 'b', 20.0, 2024, 2),
            (3, 'c', 30.0, 2025, 3);
    """

    // The original repro: nested CTEs with EXCEPT plus an unused CTE whose
    // body contains scalar subqueries over the other CTEs, joined by UNION ALL.
    // Before the fix this threw NullPointerException in RewriteCteChildren during
    // the second rewrite pass. The bug surfaces at planning time, so a successful
    // execution alone proves the fix. We avoid qt_ here to keep the test
    // self-contained (no .out baseline to maintain for double-precision results).
    sql """
        with
        tbl1 as (select amount from test_sales_57348 where year = 2023
                 EXCEPT
                 select amount from test_sales_57348 where year = 2024),
        tbl2 as (select amount from test_sales_57348 where year = 2024
                 EXCEPT
                 select amount from test_sales_57348 where year = 2025),
        total as (select (select count(1) from tbl1), (select count(1) from tbl2))
        select * from tbl1 UNION ALL select * from tbl2
        order by amount;
    """

    sql "drop table if exists test_sales_57348"
}
