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

suite("test_agg_skew_hint") {
    sql "set runtime_filter_mode=OFF"
    sql "set disable_join_reorder=true;"
    sql "drop table if exists test_skew_hint"
    sql "create table test_skew_hint (a int, b int, c int) distributed by hash(a) properties('replication_num'='1');"
    sql "insert into test_skew_hint(a,b,c) values(1,2,3),(1,2,4),(1,3,4),(2,3,5),(2,4,5),(3,4,5),(3,5,6),(3,6,7),(3,7,8),(3,8,9),(3,10,11);"
    qt_hint "select a , count(distinct [skew] b) from test_skew_hint group by a order by 1,2"
    qt_hint_other_agg_func "select a , count(distinct [skew] b), count(a) from test_skew_hint group by a order by 1,2,3"
    qt_hint_other_agg_func_expr "select a , count(distinct [skew] b+1) from test_skew_hint group by a order by 1,2"
    qt_hint_same_column_with_group_by "select b , count(distinct [skew] b) from test_skew_hint group by b order by 1,2"
    qt_hint_same_column_with_group_by_expr "select b , count(distinct [skew] b+1) from test_skew_hint group by b order by 1,2"
    qt_hint_grouping "select a , count(distinct [skew] b)from test_skew_hint group by grouping sets((a),(c),()) order by 1,2"
    qt_hint_other_agg_func_grouping "select a , count(distinct [skew] b), count(a) from test_skew_hint group by grouping sets((a),(c),()) order by 1,2,3"
    qt_hint_other_agg_func_expr_grouping "select a , count(distinct [skew] b+1) from test_skew_hint group by grouping sets((a),(c),()) order by 1,2"
    qt_hint_same_column_with_group_by_grouping "select b , count(distinct [skew] b) from test_skew_hint group by grouping sets((a),(b),()) order by 1,2"
    qt_hint_same_column_with_group_by_expr_grouping "select b , count(distinct [skew] b+1) from test_skew_hint group by grouping sets((a),(b),()) order by 1,2"
    qt_hint_multi_column "select a , count(distinct [skew] b,c)from test_skew_hint group by a order by 1,2"

    qt_hint_sum "select b , sum(distinct [skew] a) from test_skew_hint group by b order by 1,2;"
    qt_hint_sum_expr "select b , sum(distinct [skew] a+1) from test_skew_hint group by b order by 1,2;"
    qt_hint_sum_grouping "select b , sum(distinct [skew] a) from test_skew_hint group by grouping sets((b), (c,b)) order by 1,2;"
    qt_hint_sum0 "select b , sum0(distinct [skew] a) from test_skew_hint group by b order by 1,2;"
    qt_hint_sum0_expr "select b , sum0(distinct [skew] abs(a)-100) from test_skew_hint group by b order by 1,2;"
    qt_hint_sum0_grouping "select b , sum0(distinct [skew] a) from test_skew_hint group by grouping sets((b), (c,b)) order by 1,2;"
    // qt_hint_group_concat, group_concat without order by, the results will be different.
    sql "select b , group_concat(distinct [skew] cast(c as string)) from test_skew_hint group by b order by 1,2;"
    // qt_hint_group_concat_expr
    sql "select b , group_concat(distinct [skew] CONCAT(cast(c as string),'abc')) from test_skew_hint group by b order by 1,2;"
    sql "select b , group_concat(distinct [skew] cast(a as string)) from test_skew_hint group by grouping sets((b), (c,b)) order by 1,2;"
    // group_concat with order by will not be rewritten
    qt_hint_groupconcat_orderby "select b , group_concat(distinct [skew] cast(c as string) order by c) from test_skew_hint group by b order by 1,2;"

    sql "drop table if exists test_skew_hint2"
    sql """create table test_skew_hint2 (a int, b int, c int,d varchar(10)) distributed by hash(a) properties('replication_num'='1');"""
    sql """
    INSERT INTO test_skew_hint2(a,b,c) VALUES 
    (1, NULL, 3), (2, NULL, 5), (3, NULL, 7),
    (4,5,6),(4,5,7),(4,5,8),
    (5,0,0),(5,0,0),(5,0,0); 
    """

    qt_hint_null_count """
    SELECT a, count(distinct [skew] b) 
    FROM test_skew_hint2 
    GROUP BY a 
    ORDER BY 1,2;
    """

    qt_hint_multi_agg """
    SELECT a, 
        count(distinct [skew] b), 
        sum(distinct [skew] c)
    FROM test_skew_hint2 
    GROUP BY a 
    ORDER BY 1,2,3;
    """

    qt_hint_having """
    SELECT a, count(distinct [skew] b) as cnt 
    FROM test_skew_hint2 
    GROUP BY a 
    HAVING cnt >= 1 
    ORDER BY 1,2;
    """


    qt_hint_case_expr """
    SELECT a, 
        sum(distinct [skew] CASE WHEN b > 3 THEN c*10 ELSE b END)
    FROM test_skew_hint2 
    GROUP BY a 
    ORDER BY 1,2;
    """

    qt_hint_case_expr """
    SELECT a, 
        sum0(distinct [skew] CASE WHEN b > 3 THEN c*10 ELSE b END)
    FROM test_skew_hint2 
    GROUP BY a 
    ORDER BY 1,2;"""

    qt_hint_global_agg """
    SELECT 
        count(distinct [skew] b),
        sum(distinct [skew] a),
        sum0(distinct [skew] c)
    FROM test_skew_hint2;
    """

    qt_hint_single_distinct """
    SELECT a, count(distinct [skew] b) 
    FROM test_skew_hint2 
    WHERE a = 4 
    GROUP BY a 
    ORDER BY 1,2;
    """

    qt_hint_sum0_zero """
    SELECT c, sum0(distinct [skew] b) 
    FROM test_skew_hint2 
    WHERE b is null
    GROUP BY c 
    ORDER BY 1,2;
    """

    qt_hint_multi_col_expr """
    SELECT a, 
        count(distinct [skew] b + c)
    FROM test_skew_hint2 
    GROUP BY a 
    ORDER BY 1,2;
    """

    qt_hint_multi_group_key """
    SELECT a, c, 
        sum(distinct [skew] b)
    FROM test_skew_hint2 
    GROUP BY a, c 
    ORDER BY 1,2,3;
    """

    qt_hint_const_expr """
    SELECT a, 
        sum(distinct [skew] b + 10)
    FROM test_skew_hint2 
    GROUP BY a 
    ORDER BY 1,2;
    """

    qt_hint_mixed_distinct """
    SELECT a, 
        count(distinct [skew] b),
        count(distinct c)
    FROM test_skew_hint2 
    GROUP BY a 
    ORDER BY 1,2,3;
    """

    qt_group_concat_null """
    SELECT b, group_concat(distinct [skew] cast(c as STRING))
    FROM test_skew_hint2
    WHERE b IS NULL and c IS NULL
    GROUP BY b order by 1,2;"""
    qt_all_null "SELECT sum(distinct [skew] d),group_concat(distinct [skew] d), count(distinct [skew] d) FROM test_skew_hint2 group by b order by 1,2,3;"
    qt_all_null_sum0 "SELECT sum0(distinct [skew] d) FROM test_skew_hint2 group by b order by 1;"
    qt_subquery_skew """
    SELECT a FROM test_skew_hint2
    WHERE c > (
            SELECT sum0(distinct [skew] b)
                    FROM test_skew_hint2
                    WHERE b IS NOT NULL
    ) order by 1"""

    sql "set skew_rewrite_agg_bucket_num = 65535"
    qt_hint_variable "select a , count(distinct [skew] b)from test_skew_hint group by a order by 1,2"

    qt_hint_agg_join "select t1.a , count(distinct [skew] t2.b) from test_skew_hint t1 left join test_skew_hint t2 on t1.a=t2.a group by t1.a order by 1,2"
    qt_hint_agg_agg "select a , count(distinct [skew] b)from (select a, count(distinct [skew] b) as b from test_skew_hint group by a) t group by a order by 1,2"
    qt_hint_multi_count_distinct "select a, count(distinct [skew] b), count(distinct [skew] c) as b from test_skew_hint group by a order by 1,2"

    qt_test_min_hint "select b , min(distinct [skew] a)from test_skew_hint group by b order by 1,2;"
    qt_test_wrong_hint "select b , count(distinct [skew2] a)from test_skew_hint group by b order by 1,2;"
    qt_not_rewrite "select b , count(distinct [skew] a)from test_skew_hint group by b,a order by 1,2;"

    // shape
    sql "set skew_rewrite_agg_bucket_num = 1024"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    qt_shape_hint "explain shape plan select a , count(distinct [skew] b)from test_skew_hint group by a"
    qt_shape_hint_other_agg_func "explain shape plan select a , count(distinct [skew] b), count(a) from test_skew_hint group by a"
    qt_shape_hint_other_agg_func_expr "explain shape plan select a , count(distinct [skew] b+1) from test_skew_hint group by a"
    qt_shape_hint_same_column_with_group_by "explain shape plan select b , count(distinct [skew] b) from test_skew_hint group by b"
    qt_shape_hint_same_column_with_group_by_expr "explain shape plan select b , count(distinct [skew] b+1) from test_skew_hint group by b"
    qt_shape_hint_grouping "explain shape plan select a , count(distinct [skew] b)from test_skew_hint group by grouping sets((a),(c),())"
    qt_shape_hint_other_agg_func_grouping "explain shape plan select a , count(distinct [skew] b), count(a) from test_skew_hint group by grouping sets((a),(c),())"
    qt_shape_hint_other_agg_func_expr_grouping "explain shape plan select a , count(distinct [skew] b+1) from test_skew_hint group by grouping sets((a),(c),())"
    qt_shape_hint_same_column_with_group_by_grouping "explain shape plan select b , count(distinct [skew] b) from test_skew_hint group by grouping sets((a),(b),())"
    qt_shape_hint_same_column_with_group_by_expr_grouping "explain shape plan select b , count(distinct [skew] b+1) from test_skew_hint group by grouping sets((a),(b),())"
    qt_shape_hint_multi_column "explain shape plan select a , count(distinct [skew] b,c)from test_skew_hint group by a"
    qt_shape_hint_sum "explain shape plan select b , sum(distinct [skew] a) from test_skew_hint group by b;"
    qt_shape_hint_sum_expr "explain shape plan select b , sum(distinct [skew] a+1) from test_skew_hint group by b;"
    qt_shape_hint_sum_grouping "explain shape plan select b , sum(distinct [skew] a) from test_skew_hint group by grouping sets((b), (c,b));"
    qt_shape_hint_sum0 "explain shape plan select b , sum0(distinct [skew] a) from test_skew_hint group by b;"
    qt_shape_hint_sum0_expr "explain shape plan select b , sum0(distinct [skew] abs(a)-100) from test_skew_hint group by b;"
    qt_shape_hint_sum0_grouping "explain shape plan select b , sum0(distinct [skew] a) from test_skew_hint group by grouping sets((b), (c,b));"
    qt_shape_hint_groupconcat "explain shape plan select b , group_concat(distinct [skew] cast(c as string)) from test_skew_hint group by b;"
    qt_shape_hint_groupconcat_expr "explain shape plan select b , group_concat(distinct [skew] CONCAT(cast(c as string),'abc')) from test_skew_hint group by b;"
    qt_shape_hint_groupconcat_grouping "explain shape plan select b , group_concat(distinct [skew] cast(a as string)) from test_skew_hint group by grouping sets((b), (c,b));"
    qt_shape_hint_groupconcat_orderby "explain shape plan select b , group_concat(distinct [skew] cast(c as string) order by c) from test_skew_hint group by b;"

    sql "set skew_rewrite_agg_bucket_num = 65535"
    qt_shape_hint_variable "explain shape plan select a , count(distinct [skew] b)from test_skew_hint group by a"

    qt_shape_hint_agg_join "explain shape plan select t1.a , count(distinct [skew] t2.b) from test_skew_hint t1 left join test_skew_hint t2 on t1.a=t2.a group by t1.a"
    qt_shape_hint_agg_agg "explain shape plan select a , count(distinct [skew] b)from (select a, count(distinct [skew] b) as b from test_skew_hint group by a) t group by a"
    qt_shape_hint_multi_count_distinct "explain shape plan select a, count(distinct [skew] b), count(distinct [skew] c) as b from test_skew_hint group by a"

    qt_shape_test_min_hint "explain shape plan select b , min(distinct [skew] a)from test_skew_hint group by b;"
    qt_shape_test_wrong_hint "explain shape plan select b , count(distinct [skew2] a)from test_skew_hint group by b;"
    qt_shape_not_rewrite "explain shape plan select b , count(distinct [skew] a)from test_skew_hint group by b,a;"
}