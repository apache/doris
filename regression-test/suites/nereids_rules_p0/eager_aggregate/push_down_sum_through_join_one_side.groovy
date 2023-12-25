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

suite("push_down_sum_through_join_one_side") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
        DROP TABLE IF EXISTS sum_t_one_side;
    """

    sql """
    CREATE TABLE IF NOT EXISTS sum_t_one_side(
      `id` int(32),
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql "insert into sum_t_one_side values (1, 1, 'a')"
    sql "insert into sum_t_one_side values (2, null, 'a')"
    sql "insert into sum_t_one_side values (3, 1, null)"
    sql "insert into sum_t_one_side values (4, 2, 'b')"
    sql "insert into sum_t_one_side values (5, null, 'b')"
    sql "insert into sum_t_one_side values (6, 2, null)"
    sql "insert into sum_t_one_side values (7, 3, 'c')"
    sql "insert into sum_t_one_side values (8, null, 'c')"
    sql "insert into sum_t_one_side values (9, 3, null)"
    sql "insert into sum_t_one_side values (10, null, null)"

    sql "SET ENABLE_NEREIDS_RULES=push_down_sum_through_join_one_side"

    qt_groupby_pushdown_basic """
        explain shape plan select sum(t1.score) from sum_t_one_side t1, sum_t_one_side t2 where t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_join """
        explain shape plan select sum(t1.score) from sum_t_one_side t1 left join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_right_join """
        explain shape plan select sum(t1.score) from sum_t_one_side t1 right join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_full_join """
        explain shape plan select sum(t1.score) from sum_t_one_side t1 full join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_semi_join """
        explain shape plan select sum(t1.score) from sum_t_one_side t1 inner join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_left_anti_join """
        explain shape plan select sum(t1.score) from sum_t_one_side t1 left anti join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_complex_conditions """
        explain shape plan select sum(t1.score) from sum_t_one_side t1 join sum_t_one_side t2 on t1.id = t2.id and t1.name < t2.name group by t1.name;
    """

    qt_groupby_pushdown_with_aggregate """
        explain shape plan select sum(t1.score), avg(t1.score) from sum_t_one_side t1 join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_subquery """
        explain shape plan select sum(t1.score) from (select * from sum_t_one_side where score > 10) t1 join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_outer_join """
        explain shape plan select sum(t1.score) from sum_t_one_side t1 left join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_deep_subquery """
        explain shape plan select sum(t1.score) from (select * from (select * from sum_t_one_side) sum_t_one_side where score > 10) t1 join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_having """
        explain shape plan select sum(t1.score) from sum_t_one_side t1, sum_t_one_side t2 where t1.id = t2.id group by t1.name having sum(t1.score) > 100;
    """

    qt_groupby_pushdown_mixed_aggregates """
        explain shape plan select sum(t1.score), count(*), max(t1.score) from sum_t_one_side t1 join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_multi_table_join """
        explain shape plan select sum(t1.score) from sum_t_one_side t1 join sum_t_one_side t2 on t1.id = t2.id join sum_t_one_side t3 on t1.name = t3.name group by t1.name;
    """

    qt_groupby_pushdown_with_order_by """
        explain shape plan select sum(t1.score) from sum_t_one_side t1, sum_t_one_side t2 where t1.id = t2.id group by t1.name order by t1.name;
    """

    qt_groupby_pushdown_multiple_equal_conditions """
        explain shape plan select sum(t1.score) from sum_t_one_side t1, sum_t_one_side t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_with_aggregate """
        explain shape plan select max(t1.score), sum(t2.score) from sum_t_one_side t1 join sum_t_one_side t2 on t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_non_aggregate_selection """
        explain shape plan select t1.name, sum(t1.score) from sum_t_one_side t1, sum_t_one_side t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

    qt_groupby_pushdown_equal_conditions_non_aggregate_selection_with_aggregate """
        explain shape plan select t1.name, sum(t1.score), sum(t2.score) from sum_t_one_side t1, sum_t_one_side t2 where t1.id = t2.id and t1.name = t2.name group by t1.name;
    """

     qt_groupby_pushdown_with_where_clause """
        explain shape plan select sum(t1.score) from sum_t_one_side t1, sum_t_one_side t2 where t1.id = t2.id and t1.score > 50 group by t1.name;
    """

    qt_groupby_pushdown_varied_aggregates """
        explain shape plan select sum(t1.score), avg(t1.id), count(t2.name) from sum_t_one_side t1 join sum_t_one_side t2 on t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_with_order_by_limit """
        explain shape plan select sum(t1.score) from sum_t_one_side t1, sum_t_one_side t2 where t1.id = t2.id group by t1.name order by sum(t1.score) limit 10;
    """

    qt_groupby_pushdown_alias_multiple_equal_conditions """
        explain shape plan select sum(t1_alias.score) from sum_t_one_side t1_alias join sum_t_one_side t2_alias on t1_alias.id = t2_alias.id and t1_alias.name = t2_alias.name group by t1_alias.name;
    """

    qt_groupby_pushdown_complex_join_condition """
        explain shape plan select sum(t1.score) from sum_t_one_side t1 join sum_t_one_side t2 on t1.id = t2.id and t1.score = t2.score and t1.name <> t2.name group by t1.name;
    """

    qt_groupby_pushdown_function_processed_columns """
        explain shape plan select sum(LENGTH(t1.name)) from sum_t_one_side t1, sum_t_one_side t2 where t1.id = t2.id group by t1.name;
    """

    qt_groupby_pushdown_nested_queries """
        explain shape plan select sum(t1.score) from (select * from sum_t_one_side where score > 20) t1 join (select * from sum_t_one_side where id < 100) t2 on t1.id = t2.id group by t1.name;
    """
}