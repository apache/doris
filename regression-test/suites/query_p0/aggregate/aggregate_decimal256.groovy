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

suite("aggregate_decimal256") {
    sql "set enable_nereids_planner = true;"
    sql "set enable_fallback_to_original_planner = false;"
    sql "set enable_decimal256 = true;"
    sql "drop table if exists test_aggregate_decimal256_sum;"
    sql """ create table test_aggregate_decimal256_sum(k1 int, v1 decimal(38, 6), v2 decimal(38, 6))
                DUPLICATE KEY(`k1`, `v1`, `v2`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                properties("replication_num" = "1"); """
    
    sql """insert into test_aggregate_decimal256_sum values 
            (1, 1.000000, 99999999999999999999999999999999.999999),
            (1, 1.000000, 99999999999999999999999999999999.999999),
            (1, 1.000000, -999999.200002),
            (1, 1.000000, 999999.200002),
            (2, 11.000000, 99999999999999999999999999999999.999999),
            (2, 11.000000, 99999999999999999999999999999999.999999),
            (2, 11.000000, -999999.200002),
            (2, 11.000000, 999999.200002);"""
    sql "sync"

    qt_sql_sum_1 """ select k1, sum(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_sum where v1 = 1 group by k1 order by 1, 2; """
    qt_sql_sum_2 """ select k1, sum(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_sum where v1 = 11 group by k1 order by 1, 2; """
    qt_sql_sum_3 """ select k1, sum(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_sum group by k1 order by 1, 2; """
    qt_sql_sum_4 """
        select
                k1,
                sum(sum_val)
        from
                (
                        (
                                select
                                        k1,
                                        sum(cast(v2 as decimalv3(39, 6))) as sum_val
                                from
                                        test_aggregate_decimal256_sum
                                where
                                        v1 = 1
                                group by k1
                        )
                        union
                        all (
                                select
                                        k1,
                                        sum(cast(v2 as decimalv3(39, 6))) as sum_val
                                from
                                        test_aggregate_decimal256_sum
                                where
                                        v1 = 11
                                group by k1
                        )
                ) union1 group by k1
        order by 1, 2;
    """

    qt_sql_sum_5 """ select cast(v2 as decimalv3(39, 6)) v2_cast, sum(k1) from test_aggregate_decimal256_sum group by v2_cast order by 1, 2; """

    sql """insert into test_aggregate_decimal256_sum values 
            (1, 1.000000, -999999.200002),
            (1, 1.000000, 999999.200002),
            (2, 11.000000, -999999.200002),
            (2, 11.000000, 999999.200002);"""
    sql "sync"
    qt_sql_sum_6 """ select cast(v1 as decimalv3(39, 6)) v1_cast, cast(v2 as decimalv3(39, 6)) v2_cast, sum(k1) from test_aggregate_decimal256_sum group by v1_cast, v2_cast order by 1, 2, 3; """
    qt_sql_sum_7 """ select sum(cast(v1 as decimalv3(39, 6))) from test_aggregate_decimal256_sum order by 1; """
    qt_sql_sum_8 """ select sum(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_sum order by 1; """
    qt_sql_sum_9 """ select sum(cast(v1 as decimalv3(39, 6))), sum(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_sum order by 1, 2; """

    sql "drop table if exists test_aggregate_decimal256_avg;"
    sql """ create table test_aggregate_decimal256_avg(k1 int, v1 decimal(38, 6), v2 decimal(38, 6))
                DUPLICATE KEY(`k1`, `v1`, `v2`)
                DISTRIBUTED BY HASH(`k1`) BUCKETS 10
                properties("replication_num" = "1"); """
    
    sql """insert into test_aggregate_decimal256_avg values 
            (1, 1.000000, 99999999999999999999999999999999.999999),
            (1, 1.000000, 99999999999999999999999999999999.999999),
            (1, 1.000000, -999999.200002),
            (1, 1.000000, 999999.200002),
            (2, 11.000000, 99999999999999999999999999999999.999999),
            (2, 11.000000, 99999999999999999999999999999999.999999),
            (2, 11.000000, -999999.200002),
            (2, 11.000000, 999999.200002);"""
    sql "sync"
    qt_sql_avg_1 """ select k1, avg(cast(v2 as decimalv3(76, 6))) from test_aggregate_decimal256_avg where v1 = 1 group by k1 order by 1, 2; """
    qt_sql_avg_2 """ select k1, avg(cast(v2 as decimalv3(76, 6))) from test_aggregate_decimal256_avg where v1 = 11 group by k1 order by 1, 2; """
    qt_sql_avg_3 """ select k1, avg(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg group by k1 order by 1, 2; """
    qt_sql_avg_4 """
        select
                k1,
                avg(avg_val)
        from
                (
                        (
                                select
                                        k1,
                                        avg(cast(v2 as decimalv3(39, 6))) as avg_val
                                from
                                        test_aggregate_decimal256_avg
                                where
                                        v1 = 1
                                group by k1
                        )
                        union
                        all (
                                select
                                        k1,
                                        avg(cast(v2 as decimalv3(39, 6))) as avg_val
                                from
                                        test_aggregate_decimal256_avg
                                where
                                        v1 = 11
                                group by k1
                        )
                ) union1 group by k1
        order by 1, 2;
    """
    qt_sql_avg_5 """ select avg(cast(v1 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1; """
    qt_sql_avg_6 """ select avg(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1; """
    qt_sql_avg_7 """ select avg(cast(v1 as decimalv3(39, 6))), avg(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1, 2; """

    qt_sql_max_1 """ select k1, max(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg group by k1 order by 1, 2; """
    qt_sql_max_2 """ select max(cast(v1 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1; """
    qt_sql_max_3 """ select max(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1; """
    qt_sql_max_4 """ select max(cast(v1 as decimalv3(39, 6))), max(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1; """

    qt_sql_min_1 """ select k1, min(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg group by k1 order by 1, 2; """
    qt_sql_min_2 """ select min(cast(v1 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1; """
    qt_sql_min_3 """ select min(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1; """
    qt_sql_min_4 """ select min(cast(v1 as decimalv3(39, 6))), min(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1; """

    qt_sql_count_1 """ select k1, count(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg group by k1 order by 1, 2; """
    qt_sql_count_2 """ select k1, count(cast(v1 as decimalv3(39, 6))), count(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg group by k1 order by 1, 2, 3; """
    qt_sql_count_3 """ select count(cast(v1 as decimalv3(39, 6))), count(cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg order by 1, 2; """

    // qt_sql_distinct_count_1 """ select k1, count(distinct cast(v1 as decimalv3(39, 6))) from test_aggregate_decimal256_avg group by k1 order by 1, 2;"""
    // qt_sql_distinct_count_2 """ select k1, count(distinct cast(v1 as decimalv3(39, 6))), count(distinct cast(v2 as decimalv3(39, 6))) from test_aggregate_decimal256_avg group by k1 order by 1, 2, 3;"""
}
