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

suite("push_down_count_through_join") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"
    sql "set DISABLE_NEREIDS_RULES='ONE_PHASE_AGGREGATE_WITHOUT_DISTINCT, ONE_PHASE_AGGREGATE_SINGLE_DISTINCT_TO_MULTI'"

    sql """
        DROP TABLE IF EXISTS count_t;
    """

    sql """
    CREATE TABLE IF NOT EXISTS count_t(
      `id` int(32),
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "insert into count_t values (1, 1, 'a')"
    sql "insert into count_t values (2, null, 'a')"
    sql "insert into count_t values (3, 1, null)"
    sql "insert into count_t values (4, 2, 'b')"
    sql "insert into count_t values (5, null, 'b')"
    sql "insert into count_t values (6, 2, null)"
    sql "insert into count_t values (7, 3, 'c')"
    sql "insert into count_t values (8, null, 'c')"
    sql "insert into count_t values (9, 3, null)"
    sql "insert into count_t values (10, null, null)"
    sql "analyze table count_t with sync;"
    qt_groupby_pushdown_basic """
        explain shape plan select count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_join """
        explain shape plan select count(t1.score) from count_t t1 left join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_right_join """
        explain shape plan select count(t1.score) from count_t t1 right join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_full_join """
        explain shape plan select count(t1.score) from count_t t1 full join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_semi_join """
        explain shape plan select count(t1.score) from count_t t1 inner join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_anti_join """
        explain shape plan select count(t1.score) from count_t t1 left anti join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_complex_conditions """
        explain shape plan select count(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id and t1.name < t2.name group by t1.name;
    """

    qt_groupby_pushdown_with_aggregate """
        explain shape plan select count(t1.score), avg(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_subquery """
        explain shape plan select count(t1.score) from (select * from count_t where score > 10) t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_outer_join """
        explain shape plan select count(t1.score) from count_t t1 left join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_deep_subquery """
        explain shape plan select count(t1.score) from (select * from (select * from count_t) count_t where score > 10) t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_having """
        explain shape plan select count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name having count(t1.score) > 100;
    """

    qt_groupby_pushdown_mixed_aggregates """
        explain shape plan select count(t1.score), count(*), max(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_multi_table_join_1 """
        explain shape plan select count(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id join count_t t3 on t1.name = t3.name group by t1.name;
    """

    qt_groupby_pushdown_with_order_by """
        explain shape plan select count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name order by t1.name;
    """

    qt_groupby_pushdown_multiple_equal_conditions """
        explain shape plan select count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_with_aggregate """
        explain shape plan select max(t1.score), count(t2.score) from count_t t1 join count_t t2 on t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_non_aggregate_selection """
        explain shape plan select t1.name, count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_non_aggregate_selection_with_aggregate """
        explain shape plan select t1.name, count(t1.score), count(t2.score) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

     qt_groupby_pushdown_with_where_clause """
        explain shape plan select count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id and t1.score > 50 group by t1.name;
    """

    qt_groupby_pushdown_varied_aggregates """
        explain shape plan select count(t1.score), avg(t1.id), count(t2.name) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_with_order_by_limit """
        explain shape plan select count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name order by count(t1.score) limit 10;
    """

    qt_groupby_pushdown_alias_multiple_equal_conditions """
        explain shape plan select count(t1_alias.score) from count_t t1_alias join count_t t2_alias on t1_alias.id = t2_alias.id and t1_alias.name = t2_alias.name group by t1_alias.name;
    """

    qt_groupby_pushdown_complex_join_condition """
        explain shape plan select count(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id and t1.score = t2.score and t1.name <> t2.name group by t1.name;
    """

    qt_groupby_pushdown_function_processed_columns """
        explain shape plan select count(LENGTH(t1.name)) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_nested_queries """
        explain shape plan select count(t1.score) from (select * from count_t where score > 20) t1 join (select * from count_t where id < 100) t2 on t1.id = t2.id group by t1.name;
    """

    /* COUNT(*) */
    qt_groupby_pushdown_basic """
        explain shape plan select count(*) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_join """
        explain shape plan select count(*) from count_t t1 left join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_right_join """
        explain shape plan select count(*) from count_t t1 right join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_full_join """
        explain shape plan select count(*) from count_t t1 full join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_semi_join """
        explain shape plan select count(*) from count_t t1 inner join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_anti_join """
        explain shape plan select count(*) from count_t t1 left anti join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_complex_conditions """
        explain shape plan select count(*) from count_t t1 join count_t t2 on t1.id = t2.id and t1.name < t2.name group by t1.name;
    """

    qt_groupby_pushdown_with_aggregate """
        explain shape plan select count(*) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_subquery """
        explain shape plan select count(*) from (select * from count_t where score > 10) t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_outer_join """
        explain shape plan select count(*) from count_t t1 left join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_deep_subquery """
        explain shape plan select count(*) from (select * from (select * from count_t) count_t where score > 10) t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_having """
        explain shape plan select count(*) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name having count(*) > 100;
    """

    qt_groupby_pushdown_multi_table_join_2 """
        explain shape plan select count(*) from count_t t1 join count_t t2 on t1.id = t2.id join count_t t3 on t1.name = t3.name group by t1.name;
    """

    qt_groupby_pushdown_with_order_by """
        explain shape plan select count(*) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name order by t1.name;
    """

    qt_groupby_pushdown_multiple_equal_conditions """
        explain shape plan select count(*) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_non_aggregate_selection """
        explain shape plan select t1.name, count(*) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

     qt_groupby_pushdown_with_where_clause """
        explain shape plan select count(*) from count_t t1, count_t t2 where t1.id = t2.id and t1.score > 50 group by t1.name;
    """

    qt_groupby_pushdown_varied_aggregates """
        explain shape plan select count(*), avg(t1.id), count(t2.name) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_with_order_by_limit """
        explain shape plan select count(*) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name order by count(*) limit 10;
    """

    qt_groupby_pushdown_complex_join_condition """
        explain shape plan select count(*) from count_t t1 join count_t t2 on t1.id = t2.id and t1.score = t2.score and t1.name <> t2.name group by t1.name;
    """

    qt_groupby_pushdown_nested_queries """
        explain shape plan select count(*) from (select * from count_t where score > 20) t1 join (select * from count_t where id < 100) t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_basic """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_left_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1 left join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_right_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1 right join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_full_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1 full join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_left_semi_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1 inner join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_left_anti_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1 left anti join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_complex_conditions """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id and t1.name < t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score), avg(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_subquery """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from (select * from count_t where score > 10) t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_outer_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1 left join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_deep_subquery """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from (select * from (select * from count_t) count_t where score > 10) t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_having """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name having count(t1.score) > 100;
    """

    qt_with_hint_groupby_pushdown_mixed_aggregates """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score), count(*), max(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_multi_table_join_1 """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id join count_t t3 on t1.name = t3.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_order_by """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name order by t1.name;
    """

    qt_with_hint_groupby_pushdown_multiple_equal_conditions """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_equal_conditions_with_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  max(t1.score), count(t2.score) from count_t t1 join count_t t2 on t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_equal_conditions_non_aggregate_selection """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  t1.name, count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_equal_conditions_non_aggregate_selection_with_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  t1.name, count(t1.score), count(t2.score) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_where_clause """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id and t1.score > 50 group by t1.name;
    """

    qt_with_hint_groupby_pushdown_varied_aggregates """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score), avg(t1.id), count(t2.name) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_order_by_limit """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name order by count(t1.score) limit 10;
    """

    qt_with_hint_groupby_pushdown_alias_multiple_equal_conditions """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1_alias.score) from count_t t1_alias join count_t t2_alias on t1_alias.id = t2_alias.id and t1_alias.name = t2_alias.name group by t1_alias.name;
    """

    qt_with_hint_groupby_pushdown_complex_join_condition """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from count_t t1 join count_t t2 on t1.id = t2.id and t1.score = t2.score and t1.name <> t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_function_processed_columns """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(LENGTH(t1.name)) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_nested_queries """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(t1.score) from (select * from count_t where score > 20) t1 join (select * from count_t where id < 100) t2 on t1.id = t2.id group by t1.name;
    """

    /* COUNT(*) */
    qt_with_hint_groupby_pushdown_basic """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_left_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 left join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_right_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 right join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_full_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 full join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_left_semi_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 inner join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_left_anti_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 left anti join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_complex_conditions """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 join count_t t2 on t1.id = t2.id and t1.name < t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_subquery """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from (select * from count_t where score > 10) t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_outer_join """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 left join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_deep_subquery """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from (select * from (select * from count_t) count_t where score > 10) t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_having """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name having count(*) > 100;
    """

    qt_with_hint_groupby_pushdown_multi_table_join_2 """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 join count_t t2 on t1.id = t2.id join count_t t3 on t1.name = t3.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_order_by """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name order by t1.name;
    """

    qt_with_hint_groupby_pushdown_multiple_equal_conditions """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_equal_conditions_non_aggregate_selection """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  t1.name, count(*) from count_t t1, count_t t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_where_clause """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1, count_t t2 where t1.id = t2.id and t1.score > 50 group by t1.name;
    """

    qt_with_hint_groupby_pushdown_varied_aggregates """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*), avg(t1.id), count(t2.name) from count_t t1 join count_t t2 on t1.id = t2.id group by t1.name;
    """

    qt_with_hint_groupby_pushdown_with_order_by_limit """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1, count_t t2 where t1.id = t2.id group by t1.name order by count(*) limit 10;
    """

    qt_with_hint_groupby_pushdown_complex_join_condition """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from count_t t1 join count_t t2 on t1.id = t2.id and t1.score = t2.score and t1.name <> t2.name group by t1.name;
    """

    qt_with_hint_groupby_pushdown_nested_queries """
        explain shape plan select /*+ USE_CBO_RULE(push_down_agg_through_join) */  count(*) from (select * from count_t where score > 20) t1 join (select * from count_t where id < 100) t2 on t1.id = t2.id group by t1.name;
    """
}
