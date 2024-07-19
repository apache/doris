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

import java.util.stream.Collectors

suite("eliminate_outer_join") {
    sql "SET enable_nereids_planner=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET enable_fallback_to_original_planner=false"
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"
    sql 'set be_number_for_test=3'
    sql "set enable_parallel_result_sink=false;"

    sql """
        DROP TABLE IF EXISTS t
    """
    sql """
    CREATE TABLE IF NOT EXISTS t(
      `id` int(32) NULL,
      `score` int(64) NULL,
      `name` varchar(64) NULL
    ) ENGINE = OLAP
    DISTRIBUTED BY HASH(id) BUCKETS 4
    PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
    );
    """

    def variables = sql "show variables"
    def variableString = variables.stream()
            .map { it.toString() }
            .collect(Collectors.joining("\n"))
    logger.info("Variables:\n${variableString}")

    qt_left_outer """
        explain shape plan select * from t t1 left outer join t t2 on t1.id = t2.id where t2.score > 10;
    """

    qt_right_outer """
        explain shape plan select * from t t1 right outer join t t2 on t1.id = t2.id where t1.score > 10;
    """

    qt_full_outer_join """
        explain shape plan select * from t t1 full outer join t t2 on t1.id = t2.id where t1.score > 10;
    """

    qt_full_outer_join """
        explain shape plan select * from t t1 full outer join t t2 on t1.id = t2.id where t2.score > 10;
    """

    qt_full_outer_join """
        explain shape plan select * from t t1 full outer join t t2 on t1.id = t2.id where t1.score > 10 and t2.score > 10;
    """

    qt_left_outer """
        explain shape plan select * from t t1 left outer join t t2 on t1.id = t2.id where t1.score > 10 and t2.score > 10;
    """

    qt_multiple_left_outer_1 """
        explain shape plan select * from t t1 left outer join t t2 on t1.id = t2.id left join t t3 on t1.id = t3.id  where t1.score > 10;
    """

    qt_multiple_left_outer_2 """
        explain shape plan select * from t t1 left outer join t t2 on t1.id = t2.id left join t t3 on t1.id = t3.id  where t2.score > 10;
    """

    qt_multiple_right_outer_1 """
        explain shape plan select * from t t1 right outer join t t2 on t1.id = t2.id right join t t3 on t1.id = t3.id  where t1.score > 10;
    """

    qt_multiple_right_outer_2 """
        explain shape plan select * from t t1 right outer join t t2 on t1.id = t2.id right join t t3 on t1.id = t3.id  where t2.score > 10;
    """

    qt_multiple_full_outer_1 """
        explain shape plan select * from t t1 full outer join t t2 on t1.id = t2.id full join t t3 on t1.id = t3.id  where t1.score > 10;
    """

    qt_multiple_full_outer_2 """
        explain shape plan select * from t t1 full outer join t t2 on t1.id = t2.id full join t t3 on t1.id = t3.id  where t2.score > 10;
    """

    qt_left_outer_join_non_null_assertion """
        explain shape plan select * from t t1 left outer join t t2 on t1.id = t2.id where t1.id is not null and t1.score > 5;
    """

    qt_right_outer_join_non_null_assertion """
        explain shape plan select * from t t1 right outer join t t2 on t1.id = t2.id where t2.id is not null and t2.score > 5;
    """

    qt_full_outer_join_compound_conditions """
        explain shape plan select * from t t1 full outer join t t2 on t1.id = t2.id where t1.score > 5 or t2.score > 5;
    """

    qt_multiple_joins_complex_conditions """
        explain shape plan select * from t t1 left outer join t t2 on t1.id = t2.id left join t t3 on t2.id = t3.id where t1.score > 5 and t3.score is not null;
    """

    qt_using_non_equijoin_conditions """
        explain shape plan select * from t t1 left outer join t t2 on t1.score = t2.score + 10 where t1.id is not null;
    """

    qt_joining_multiple_tables_with_aggregate_functions """
        explain shape plan select t1.id, max(t2.score) from t t1 left outer join t t2 on t1.id = t2.id group by t1.id having count(t2.id) > 1;
    """

    qt_using_subqueries """
        explain shape plan select * from (select t1.* from t t1 left outer join t t2 on t1.id = t2.id) as sub where sub.id is not null;
    """

    qt_left_outer """
        explain shape plan select * from t t1 left outer join t t2 on t1.id = t2.id where t1.score > 10 and t2.name is not null;
    """

    qt_right_outer """
        explain shape plan select * from t t1 right outer join t t2 on t1.id = t2.id where t1.score > 10 and t2.name is not null;
    """

    qt_full_outer """
        explain shape plan select * from t t1 full outer join t t2 on t1.id = t2.id where t1.score > 10 and t2.name is not null;
    """

    qt_self_left_outer """
        explain shape plan select * from t t1 left outer join t t1_alias on t1.id = t1_alias.id where t1_alias.name > '2023-01-01';
    """

    qt_right_outer_aggregate """
        explain shape plan select t2.id, sum(t1.score) from t t1 right outer join t t2 on t1.id = t2.id group by t2.id;
    """

    qt_full_outer_multiple_tables """
        explain shape plan select * from t t1 full outer join t t2 on t1.id = t2.id full join t t3 on t1.id = t3.id where t3.name is null;
    """

    qt_left_outer_with_subquery """
        explain shape plan select * from t t1 left outer join (select id, score from t) as t2 on t1.id = t2.id where t2.score > 20;
    """

    qt_complex_join_conditions """
        explain shape plan select * from t t1 left outer join t t2 on t1.score = t2.score * 2 where t1.id < t2.id;
    """

    qt_multiple_outer_with_window_function """
        explain shape plan select t1.id, rank() over (partition by t2.id order by t1.score) from t t1 left outer join t t2 on t1.id = t2.id;
    """

    qt_join_different_tables_non_null """
        explain shape plan select * from t t1 left outer join t t2 on t1.id = t2.id where t2.name is not null;
    """
}

