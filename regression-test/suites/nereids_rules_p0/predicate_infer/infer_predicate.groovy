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

suite("infer_predicate") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET disable_join_reorder=true"
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
        DROP TABLE IF EXISTS t
    """
    sql """
    CREATE TABLE IF NOT EXISTS t(
      `id` int(32),
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    qt_infer_predicate_basic_join """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id where t1.score > 10;
    """

    qt_infer_predicate_join_with_filter """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id where t1.score > 10 and t2.name = 'Alice';
    """

    qt_infer_predicate_left_join """
        explain shape plan select * from t t1 left join t t2 on t1.id = t2.id where t1.score > 20;
    """

    qt_infer_predicate_right_join """
        explain shape plan select * from t t1 right join t t2 on t1.id = t2.id where t2.score > 20;
    """

    qt_infer_predicate_full_outer_join """
        explain shape plan select * from t t1 full outer join t t2 on t1.id = t2.id where t1.name = 'Test' or t2.name = 'Test';
    """

    qt_infer_predicate_left_semi_join """
        explain shape plan select * from t t1 left semi join t t2 on t1.id = t2.id where t1.score > 20;
    """

     qt_infer_predicate_left_anti_join """
        explain shape plan select * from t t1 left semi join t t2 on t1.id = t2.id where t1.score > 20;
    """

    qt_infer_predicate_from_subquery"""
        explain shape plan select * from (select * from t t1 where t1.id = 1) t join t t2 on t.id = t2.id;
    """

    qt_infer_predicate_multi_level_join """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id join t t3 on t2.id = t3.id where t3.name = 'Test';
    """

    qt_infer_predicate_join_with_project_limit """
        explain shape plan select t1.* from (select * from t t1 limit 10) t1 join t t2 on t1.id = t2.id where t2.score > 20;
    """

    qt_infer_predicate_with_union """
        explain shape plan select * from ((select * from t t1 where t1.id = 1) union (select * from t t2)) t join t t3 on t.id = t3.id;
    """

    qt_infer_predicate_with_except """
        explain shape plan select * from (select * from t t1 except select * from t t2) t join t t3 on t.id = t3.id;
    """

    qt_infer_predicate_with_subquery """
        explain shape plan select * from t t1 where t1.id in (select id from t where score > 60);
    """

    qt_infer_predicate_complex_condition """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id and t1.score > t2.score where t1.name = 'Test';
    """

    qt_infer_predicate_with_window_function """
        explain shape plan select t1.id, sum(t1.score) over (partition by t1.id) from t t1 join t t2 on t1.id = t2.id where t2.name = 'Charlie';
    """

    qt_infer_predicate_with_aggregate """
        explain shape plan select sum(t1.score) from t t1 join t t2 on t1.id = t2.id group by t1.id having t1.id > 70;
    """

    qt_infer_predicate_complex_and_or_logic """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id where t1.score > 80 or (t2.name = 'Dave' and t1.id < 50);
    """

    qt_infer_predicate_multiple_join_filter """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id and t1.name = t2.name where t1.score > 90 and t2.score < 60
    """

    qt_infer_predicate_join_with_not_exists """
        explain shape plan select * from t t1 where not exists (select * from t t2 where t2.id = t1.id and t2.score > 100);
    """

    qt_infer_predicate_complex_subquery """
        explain shape plan select * from t t1 where t1.id in (select id from t t2 where t2.score > 110 and t2.name = 'Frank');
    """

    qt_infer_predicate_join_with_function_processed """
        explain shape plan select * from t t1 join t t2 on LENGTH(t1.name) = LENGTH(t2.name) where t1.score > 120;
    """

    qt_infer_predicate_nested_subqueries """
        explain shape plan select * from (select * from (select * from t where score > 130) t1 where id < 70) t2 where name = 'George';
    """

    qt_infer_predicate_join_with_aggregate_having """
        explain shape plan select t1.id, sum(t1.score) from t t1 join t t2 on t1.id = t2.id group by t1.id having sum(t2.score) > 140;
    """

    qt_infer_predicate_mixed_join_types """
        explain shape plan select * from t t1 left join t t2 on t1.id = t2.id full join t t3 on t1.id = t3.id where t3.score > 150;
    """

    qt_infer_predicate_join_with_distinct """
        explain shape plan select distinct t1.id from t t1 join t t2 on t1.id = t2.id where t1.score > 160;
    """

    qt_infer_predicate_join_with_case_when """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id where CASE WHEN t1.score > 170 THEN 'high' ELSE 'low' END = 'high';
    """

    qt_infer_predicate_self_join """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id where t1.score > 10;
    """

    qt_infer_predicate_complex_multitable_join """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id join t t3 on t1.id = t3.id where t1.score > 20 and t3.name = 'Helen';
    """

    qt_infer_predicate_aggregate_subquery """
        explain shape plan select * from (select id, sum(score) as total from t group by id) t_agg where t_agg.total > 30;
    """

    qt_infer_predicate_join_with_function """
        explain shape plan select * from t t1 join t t2 on ABS(t1.score - t2.score) < 40;
    """

    qt_infer_predicate_subquery_filter """
        explain shape plan select * from t t1 where t1.id in (select id from t where score > 50);
    """

    qt_infer_predicate_with_not_operator """
        explain shape plan select * from t t1 where NOT (t1.score > 60);
    """

    qt_infer_predicate_complex_nested_subquery """
        explain shape plan select * from (select * from t where score in (select score from t where score > 80)) t1 where t1.id > 10;
    """

    qt_infer_predicate_multi_join_subquery_aggregate """
        explain shape plan select t1.id, AVG(t2.score) from t t1 join t t2 on t1.id = t2.id join t t3 on t1.id = t3.id where t2.id in (select id from t where score > 100) group by t1.id;
    """

    qt_infer_predicate_multi_join_complex_condition_not_exists """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id join t t3 on t1.id = t3.id where t1.score > 110 and NOT EXISTS (select id from t t4 where t4.id = t3.id);
    """

    qt_infer_predicate_multi_join_complex_subquery """
        explain shape plan select * from t t1 join t t2 on t1.id = t2.id join (select * from t where score > 130) t3 on t1.id = t3.id;
    """

    qt_infer_predicate_multi_join_with_having_clause """
        explain shape plan select t1.id, sum(t2.score) from t t1 join t t2 on t1.id = t2.id join t t3 on t1.id = t3.id group by t1.id having sum(t3.score) > 150;
    """

    // qt_infer_predicate_multi_join_distinct_group_by """
    //     explain shape plan select distinct t1.id from t t1 join t t2 on t1.id = t2.id join t t3 on t1.id = t3.id where t2.score > 140 group by t1.id, t2.name;
    // """

    // qt_infer_predicate_multi_join_window_function_partition """
    //     explain shape plan select t1.id, sum(t2.score) over (partition by t1.id) from t t1 join t t2 on t1.id = t2.id join t t3 on t1.id = t3.id where t3.name = 'a' group by t1.id;
    // """


    // test cast case, so we need more data type table
    sql """
    CREATE TABLE IF NOT EXISTS t1(
      `id` int(32),
      `time` datetime(6) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS t2(
      `id` int(64),
      `time` date NULL,
      `name` string NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS t3(
      `id` int(16),
      `time` datetime(0) NULL,
      `name` text NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    sql """
    CREATE TABLE IF NOT EXISTS t4(
      `id` int(8),
      `time` datetime(3) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    // 测试能上拉，并且能推导 t1.id = 1 -> t2.id = 1
    qt_infer0 """
        explain shape plan select * from (select * from t1 where t1.id =1) t1 join t2 on t1.id = t2.id;
    """

    // 测试多个 inner join，谓词 t1.id = 1 是否能成功推导 t2.id = 1 和 t3.id = 1
    qt_infer1 """
        explain shape plan select * from t1 join t2 on t1.id = t2.id join t t3 on t1.id = t3.id where t1.id = 1;
    """

    // 测试 left outer join, condition t1.id = 1 是否能成功推导 t2.id = 1
    qt_infer2 """
        explain shape plan select * from t1 left join t2 on t1.id = t2.id and t1.id = 1;
    """

    // 测试 full outer join, condition t1.id = 1 是否能成功推导 t2.id = 1
    qt_infer3 """
        explain shape plan select * from t1 full outer join t2 on t1.id = t2.id and t1.id = 1;
    """

    // 测试 left semi join, condition t1.id = 1 是否能成功推导 t2.id = 1
    qt_infer4 """
        explain shape plan select * from t1 left semi join t2 on t1.id = t2.id and t1.id = 1;
    """

    // 测试多个 inner join 中间有 project/limit，谓词 t1.id = 1 是否能成功推导 t2.id = 1 和 t3.id = 1
    qt_infer5 """
        explain shape plan select * from (select t1.id from t1 limit 10) t1 join t2 on t1.id = t2.id join t t3 on t1.id = t3.id where t1.id = 1;
    """

    // 测试左推右(t1.id = 1 -> t2.id = 1)，右推左(t2.name = 'bob' -> t1.name = 'bob')
    qt_infer6 """
        explain shape plan select * from t1 left join t2 on t1.id = t2.id and t1.id = 1 and t1.name = t2.name and t2.name = 'bob';
    """

    // bushy tree, 谓词都在最底部，需要一层层 pullup, 并能推导出所有的情况，应该是 2 * 2 * 2 * 2 = 16 种
    qt_infer7 """
        explain shape plan select * from
        (select t1.id from (select * from t1 where t1.id > 1) t1 join (select * from t2 where t2.id < 9) t2 on t1.id = t2.id ) t12
        join
        (select t3.id from (select * from t3 where t3.id != 3) t3 join (select * from t4 where t4.id != 4) t4 on t3.id = t4.id ) t34
        on t12.id = t34.id;
    """

    // 不等条件的推导
    qt_infer8 """
        explain shape plan select * from t1 join t2 on t1.id != t2.id where t1.id = 1;
    """

    // 测试 infer predicate 是否能推出正确类型， 2147483648 超过了 Int32 的最大值, 但是不超过 Int64 的最大值，用这个值测试类型是否能推导正确
    qt_infer9 """
        explain shape plan select * from (select * from t1 where t1.id = 2147483648) t1 join t2 on t1.id = t2.id;
    """

    // 测试 cast = cast
    qt_infer10 """
        explain shape plan select * from (select * from t1 where t1.id = 2147483648) t1 join t2 on cast(t1.id as smallint) = cast(t2.id as smallint);
    """

      // 测试 cast = cast
    qt_infer11 """
        explain shape plan select * from (select * from t1 where t1.id = 2147483648) t1 join t2 on cast(t1.id as largeint) = cast(t2.id as largeint);
    """
}
