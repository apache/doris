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

suite("decompose_repeat") {
//    sql "set disable_nereids_rules='DECOMPOSE_REPEAT';"
    sql "drop table if exists t1;"
    sql "create table t1(a int, b int, c int, d int) distributed by hash(a) properties('replication_num'='1');"
    sql "insert into t1 values(1,2,3,4),(1,2,3,3),(1,2,1,1),(1,3,2,2);"
    order_qt_sum "select a,b,c,sum(d) from t1 group by rollup(a,b,c);"
    order_qt_agg_func_gby_key_same_col "select a,b,c,d,sum(d) from t1 group by rollup(a,b,c,d);"
    order_qt_multi_agg_func "select a,b,c,sum(d),sum(c),max(a) from t1 group by rollup(a,b,c,d);"
    // maybe this problem:DORIS-24075
//    order_qt_nest_rewrite """
//    select a,b,c,c1 from (
//    select a,b,c,d,sum(d) c1 from t1 group by grouping sets((a,b,c),(a,b,c,d),(a),(a,b,c,c))
//    ) t group by rollup(a,b,c,c1);
//    """
    order_qt_upper_ref """
    select c1+10,a,b,c from (select a,b,c,sum(d) c1 from t1 group by rollup(a,b,c)) t group by c1+10,a,b,c;
    """
    order_qt_another_cte """
        with cte1 as (select 1 as c1 union all select 2)
        select c1 from (
        select c1,1 c2, 2 c3 from cte1 union select c1, 2,3 from cte1
        ) t
        group by rollup(c1,c2,c3);
    """
    order_qt_choose_max_group """
    select min(a+b) from t1 group by grouping sets((a,b),(b,c),(a));
    """
    order_qt_only_output_grouping_id """
    select a from t1 group by grouping sets ((),(),(),(a)) order by a;
    """
    order_qt_sum0_count "select a,b,c,d,sum0(d) c1, count(d) c3 from t1 group by grouping sets((a,b,c),(d),(d,a),(a,b,c,d));"
    order_qt_choose_max_group """
    select a,b,c,d,sum(d) c1 from t1 group by grouping sets((a,b,c),(d),(d,a),(a,b,c,d));
    """
    order_qt_multi_grouping_func """
    select a,b,c,d,count(d) c1, grouping(d),grouping_id(c) from t1 group by grouping sets((a,b,c),(d),(d,a),(a,b,c,d));
    """
    order_qt_grouping_func "select a,b,c,d,sum(d),grouping_id(a) from t1 group by grouping sets((a,b,c),(a,b,c,d),(a),(a,b,c,c))"
    // negative case
    order_qt_avg "select a,b,c,d,avg(d) from t1 group by grouping sets((a,b,c),(a,b,c,d),(a),(a,b,c,c));"
    order_qt_distinct "select a,b,c,d,sum(distinct d) from t1 group by grouping sets((a,b,c),(a,b,c,d),(a),(a,b,c,c));"
    order_qt_less_equal_than_3 "select a,b,c,d,sum(distinct d) from t1 group by grouping sets((a,b,c),(a,b,c,d),());"

    // test guard
    sql "set enable_decimal256=true;"
    multi_sql """
    drop view if exists test_guard;
    create view test_guard as select a,b,c,d,sum(d) as c1, grouping_id(a) as c2 from t1 group by grouping sets((a,b,c),(a,b,c,d),(a),(a,b,c,c));
    """
    sql "set enable_decimal256=false;"
    order_qt_guard "select * from test_guard;"

    // rollup,cube
    order_qt_rollup "select a,b,c,d,sum(d),grouping_id(a) from t1 group by rollup(a,b,c,d)"
    order_qt_cube "select a,b,c,d,sum(d),grouping_id(a) from t1 group by cube(a,b,c,d)"
    order_qt_cube_add "select a,b,c,d,sum(d)+100+grouping_id(a) from t1 group by cube(a,b,c,d);"
    order_qt_cube_sum_parm_add "select a,b,c,d,sum(a+1),grouping_id(a) from t1 group by cube(a,b,c,d);"

    // grouping scalar functions add more test
    order_qt_grouping_only_in_max "select a,b,c, grouping(c) from t1 group by grouping sets((a,b,c),(a,b),(a),());"
    order_qt_grouping_id_only_in_max_c_d "select a,b,c, grouping_id(a,b,c,d) from t1 group by grouping sets((a,b,c,d),(a,b),(a),());"
    order_qt_grouping_id_only_in_max_d "select a,b,c, grouping_id(a,b,c,d) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a),());"
    order_qt_multi_grouping_func "select a,b,c,d, grouping_id(a,b,c), grouping_id(c,b,a), grouping_id(c,a,b), grouping_id(a,a) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a),());"
    
    // more test cases for grouping scalar function bug(added by ai)
    // Test case: grouping function with partial parameters only in max group
    order_qt_grouping_partial_only_in_max "select a,b,c,d, grouping_id(a,c,d) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a,b),());"
    // Test case: multiple grouping functions, some can optimize and some cannot
    order_qt_mixed_grouping_func_1 "select a,b,c,d, grouping(a), grouping_id(b,c,d) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a),());"
    // Test case: grouping function with all parameters exist in other groups (should optimize)
    order_qt_grouping_all_in_other "select a,b,c,d, grouping_id(a,b) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a,b),(a),());"
    // Test case: grouping function with same column repeated
    order_qt_grouping_dup_col "select a,b,c,d, grouping_id(a,b,a,c,a) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a),());"
    // Test case: both grouping and grouping_id with different parameters
    order_qt_mixed_grouping_both "select a,b,c,d, grouping(a), grouping(b), grouping_id(a,b,c), grouping_id(c,d) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a),());"
    // Test case: grouping function with columns in different positions
    order_qt_grouping_different_pos "select a,b,c,d, grouping_id(b,d) from t1 group by grouping sets((a,b,c,d),(a,b),(a,c),());"
    // Test case: nested case with grouping functions that reference only-max columns
    order_qt_grouping_nested_case "select a,b,c,d, case when grouping(d) = 1 then 0 else 1 end from t1 group by grouping sets((a,b,c,d),(a,b,c),(a),());"
    // Test case: grouping function parameter mix - one in max only, others in all groups
    order_qt_grouping_mixed_params_1 "select a,b,c,d, grouping_id(a,b,d) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a,b),(a),());"
    // Test case: grouping function with single parameter that exists in multiple groups
    order_qt_grouping_single_param_multi "select a,b,c,d, grouping(c) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a,c),());"
    // Test case: multiple grouping_id functions with different parameter combinations
    order_qt_grouping_multi_combinations "select a,b,c,d, grouping_id(a), grouping_id(a,b), grouping_id(a,b,c), grouping_id(a,b,c,d) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a,b),(a),());"
    // Test case: grouping function where max group is not first
    order_qt_grouping_max_not_first "select a,b,c,d, grouping_id(c,d) from t1 group by grouping sets((a,b),(a,b,c),(a,b,c,d),());"
    // Test case: complex case with aggregation function and grouping function
    order_qt_grouping_with_agg "select a,b,c,d, sum(d), grouping_id(a,b,c) from t1 group by grouping sets((a,b,c,d),(a,b,c),(a),());"

    // test empty grouping set
    multi_sql """drop table if exists t_repeat_pick_shuffle_key;
    create table t_repeat_pick_shuffle_key(a int, b int, c int, d int);
    alter table t_repeat_pick_shuffle_key modify column a set stats ('row_count'='300000', 'ndv'='10', 'num_nulls'='0', 'min_value'='1', 'max_value'='300000', 'data_size'='2400000');
    alter table t_repeat_pick_shuffle_key modify column b set stats ('row_count'='300000', 'ndv'='100', 'num_nulls'='0', 'min_value'='1', 'max_value'='300000', 'data_size'='2400000');
    alter table t_repeat_pick_shuffle_key modify column c set stats ('row_count'='300000', 'ndv'='1000', 'num_nulls'='0', 'min_value'='1', 'max_value'='300000', 'data_size'='2400000');
    alter table t_repeat_pick_shuffle_key modify column d set stats ('row_count'='300000', 'ndv'='10000', 'num_nulls'='0', 'min_value'='1', 'max_value'='300000', 'data_size'='2400000');"""
    sql "select 2 from t_repeat_pick_shuffle_key group by grouping sets((),(),(),());"
    sql "select a,b,c,d from t_repeat_pick_shuffle_key group by rollup(a,b,c,d);"
    sql "select a,b,c,d from t_repeat_pick_shuffle_key group by cube(a,b,c,d);"
    sql "select a,b,c,d from t_repeat_pick_shuffle_key group by grouping sets((a,b,c,d),(b,c,d),(c),(c,a));"

    multi_sql """
         drop table if exists test_repeat_hash_b_ndv100;
         CREATE TABLE `test_repeat_hash_b_ndv100` (
          `a` bigint NULL,
          `b` bigint NULL,
          `c` bigint NULL,
          `d` bigint NULL,
          `e` bigint NULL
        ) ENGINE=OLAP
        DUPLICATE KEY(`a`, `b`, `c`)
        DISTRIBUTED BY HASH(`b`) BUCKETS 32
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
        set disable_nereids_rules='prune_empty_partition';
        insert into test_repeat_hash_b_ndv100 values(1,2,3,4,5),(1,2,3,45,5);
        insert into test_repeat_hash_b_ndv100 values(1,3,3,4,5),(1,3,3,45,5);
    """
    qt_satisfy_distribute """select count(*) from (select a,b,c,SUM(a), COUNT(b), MIN(c), MAX(a), ANY_VALUE(b) from test_repeat_hash_b_ndv100 group by rollup(a,b,c)) t;"""
    sql "set decompose_repeat_shuffle_index_in_max_group=0;"
    qt_satisfy_but_use_other_shuffle_key """select count(*) from (select a,b,c,SUM(a), COUNT(b), MIN(c), MAX(a), ANY_VALUE(b) from test_repeat_hash_b_ndv100 group by rollup(a,b,c)) t;"""
}