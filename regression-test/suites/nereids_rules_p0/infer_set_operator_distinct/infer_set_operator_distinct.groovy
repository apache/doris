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

suite("infer_set_operator_distinct") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql "set enable_parallel_result_sink=false;"

    sql """
        DROP TABLE IF EXISTS t1;
    """
    sql """
        DROP TABLE IF EXISTS t2;
    """
    sql """
        DROP TABLE IF EXISTS t3;
    """
    sql """
        DROP TABLE IF EXISTS t4;
    """

    sql """
    CREATE TABLE IF NOT EXISTS t1(
      `id` int(32) NULL,
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    CREATE TABLE IF NOT EXISTS t2(
      `id` int(32) NULL,
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    CREATE TABLE IF NOT EXISTS t3(
      `id` int(32) NULL,
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """
    sql """
    CREATE TABLE IF NOT EXISTS t4(
      `id` int(32) NULL,
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    qt_union_distinct """
        explain shape plan select * from t1 union select * from t2;
    """

    qt_union_complex_conditions """
        explain shape plan select * from t1 where t1.score > 10 union select * from t2 where t2.name = 'Test';
    """

    qt_multi_union """
        explain shape plan select * from t1 union select * from t2 union select * from t3;
    """

    qt_except_distinct """
        explain shape plan select * from t1 except select * from t2;
    """

    qt_except_with_filter """
        explain shape plan select * from t1 where t1.id > 100 except select * from t2 where t2.id < 50;
    """

    qt_intersect_distinct """
        explain shape plan select * from t1 intersect select * from t2;
    """

    qt_intersect_with_aggregate """
        explain shape plan select AVG(t1.score) from t1 intersect select SUM(t2.score) from t2;
    """

    qt_mixed_set_operators """
        explain shape plan (select * from t1 union select * from t2 except select * from t3) intersect select * from t4;
    """

    qt_join_with_union """
        explain shape plan select t1.* from t1 join t2 on t1.id = t2.id union select * from t3;
    """

    qt_set_operator_with_subquery """
        explain shape plan select * from (select * from t1 where score > 10) sub1 union select * from (select * from t2 where score < 5) sub2;
    """

    qt_nested_union """
        explain shape plan select * from ((select * from t1 union select * from t2) union (select * from t3 union select * from t4)) sub1;
    """

    qt_union_order_limit """
        explain shape plan select * from t1 union select * from t2 order by id limit 10;
    """

    qt_union_inner_join_combination """
        explain shape plan select t1.* from t1 join t2 on t1.id = t2.id union select * from t3;
    """

    qt_union_left_join_combination """
        explain shape plan select t1.* from t1 left join t2 on t1.id = t2.id union select * from t3;
    """

    qt_union_right_join_combination """
        explain shape plan select t1.* from t1 right join t2 on t1.id = t2.id union select * from t3;
    """

    qt_union_full_join_combination """
        explain shape plan select t1.* from t1 full join t2 on t1.id = t2.id union select * from t3;
    """

    qt_union_left_semi_join_combination """
        explain shape plan select t1.* from t1 left semi join t2 on t1.id = t2.id union select * from t3;
    """

    qt_except_with_subquery """
        explain shape plan select * from t1 except select * from (select * from t2 where score > 10) sub1;
    """

    qt_intersect_different_types """
        explain shape plan select name from t1 intersect select name from t2;
    """

    qt_union_complex_aggregate """
        explain shape plan select MAX(score) from t1 where id > 100 union select MIN(score) from t2 where id < 50;
    """

    qt_union_all_distinct """
        explain shape plan select * from t1 union all select * from t2;
    """

    qt_except_complex_subquery """
        explain shape plan select * from (select id, name from t1) sub1 except select * from (select id, name from t2 where score > 20) sub2;
    """

    qt_agg_not_output_groupby """
        explain shape plan select sum(t1.score) from t1 group by t1.id union select sum(t2.score) from t2 group by t2.id
    """

    qt_with_hint_union_distinct """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 union select * from t2;
    """

    qt_with_hint_union_complex_conditions """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 where t1.score > 10 union select * from t2 where t2.name = 'Test';
    """

    qt_with_hint_multi_union """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 union select * from t2 union select * from t3;
    """

    qt_with_hint_except_distinct """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 except select * from t2;
    """

    qt_with_hint_except_with_filter """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 where t1.id > 100 except select * from t2 where t2.id < 50;
    """

    qt_with_hint_intersect_distinct """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 intersect select * from t2;
    """

    qt_with_hint_intersect_with_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ AVG(t1.score) from t1 intersect select SUM(t2.score) from t2;
    """

    qt_with_hint_mixed_set_operators """
        explain shape plan (select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 union select * from t2 except select * from t3) intersect select * from t4;
    """

    qt_with_hint_join_with_union """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_set_operator_with_subquery """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from (select * from t1 where score > 10) sub1 union select * from (select * from t2 where score < 5) sub2;
    """

    qt_with_hint_nested_union """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from ((select * from t1 union select * from t2) union (select * from t3 union select * from t4)) sub1;
    """

    qt_with_hint_union_order_limit """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 union select * from t2 order by id limit 10;
    """

    qt_with_hint_union_inner_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_union_left_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 left join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_union_right_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 right join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_union_full_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 full join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_union_left_semi_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 left semi join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_except_with_subquery """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 except select * from (select * from t2 where score > 10) sub1;
    """

    qt_with_hint_intersect_different_types """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ name from t1 intersect select name from t2;
    """

    qt_with_hint_union_complex_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ MAX(score) from t1 where id > 100 union select MIN(score) from t2 where id < 50;
    """

    qt_with_hint_union_all_distinct """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from t1 union all select * from t2;
    """

    qt_with_hint_except_complex_subquery """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ * from (select id, name from t1) sub1 except select * from (select id, name from t2 where score > 20) sub2;
    """

    qt_with_hint_agg_not_output_groupby """
        explain shape plan select /*+ USE_CBO_RULE(INFER_SET_OPERATOR_DISTINCT) */ sum(t1.score) from t1 group by t1.id union select sum(t2.score) from t2 group by t2.id
    """

    qt_with_hint_no_union_distinct """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 union select * from t2;
    """

    qt_with_hint_no_union_complex_conditions """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 where t1.score > 10 union select * from t2 where t2.name = 'Test';
    """

    qt_with_hint_no_multi_union """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 union select * from t2 union select * from t3;
    """

    qt_with_hint_no_except_distinct """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 except select * from t2;
    """

    qt_with_hint_no_except_with_filter """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 where t1.id > 100 except select * from t2 where t2.id < 50;
    """

    qt_with_hint_no_intersect_distinct """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 intersect select * from t2;
    """

    qt_with_hint_no_intersect_with_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ AVG(t1.score) from t1 intersect select SUM(t2.score) from t2;
    """

    qt_with_hint_no_mixed_set_operators """
        explain shape plan (select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 union select * from t2 except select * from t3) intersect select * from t4;
    """

    qt_with_hint_no_join_with_union """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_no_set_operator_with_subquery """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from (select * from t1 where score > 10) sub1 union select * from (select * from t2 where score < 5) sub2;
    """

    qt_with_hint_no_nested_union """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from ((select * from t1 union select * from t2) union (select * from t3 union select * from t4)) sub1;
    """

    qt_with_hint_no_union_order_limit """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 union select * from t2 order by id limit 10;
    """

    qt_with_hint_no_union_inner_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_no_union_left_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 left join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_no_union_right_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 right join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_no_union_full_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 full join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_no_union_left_semi_join_combination """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ t1.* from t1 left semi join t2 on t1.id = t2.id union select * from t3;
    """

    qt_with_hint_no_except_with_subquery """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 except select * from (select * from t2 where score > 10) sub1;
    """

    qt_with_hint_no_intersect_different_types """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ name from t1 intersect select name from t2;
    """

    qt_with_hint_no_union_complex_aggregate """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ MAX(score) from t1 where id > 100 union select MIN(score) from t2 where id < 50;
    """

    qt_with_hint_no_union_all_distinct """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from t1 union all select * from t2;
    """

    qt_with_hint_no_except_complex_subquery """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ * from (select id, name from t1) sub1 except select * from (select id, name from t2 where score > 20) sub2;
    """

    qt_with_hint_no_agg_not_output_groupby """
        explain shape plan select /*+ USE_CBO_RULE(NO_INFER_SET_OPERATOR_DISTINCT) */ sum(t1.score) from t1 group by t1.id union select sum(t2.score) from t2 group by t2.id
    """

}
