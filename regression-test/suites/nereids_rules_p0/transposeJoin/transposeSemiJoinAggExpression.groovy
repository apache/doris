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

suite("transposeSemiJoinAggExpression") {
    sql "set runtime_filter_mode=OFF"
    sql "set disable_join_reorder=false"
    sql "set enable_dphyp_optimizer=false"

    sql "drop table if exists transpose_semi_agg_expression_t"
    sql """
        create table transpose_semi_agg_expression_t (k int not null, v int not null)
        duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num"="1")
    """
    sql "drop table if exists transpose_semi_agg_expression_s"
    sql """
        create table transpose_semi_agg_expression_s (k int not null)
        duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num"="1")
    """
    sql "insert into transpose_semi_agg_expression_t select 1, 1 from numbers(\"number\"=\"64\")"
    sql "insert into transpose_semi_agg_expression_t values (2, -1)"
    sql "insert into transpose_semi_agg_expression_s values (1)"
    sql "analyze table transpose_semi_agg_expression_t with sync"
    sql "analyze table transpose_semi_agg_expression_s with sync"

    // The unmatched negative row must be removed before evaluating assert_true.
    order_qt_non_movable_project """
        select k, sum(checked) from (
            select t.k, cast(assert_true(t.v > 0, 'positive rows only') as int) checked
            from transpose_semi_agg_expression_t t
            left semi join transpose_semi_agg_expression_s s on t.k = s.k
        ) q group by k
    """
    order_qt_non_movable_argument """
        select t.k, sum(cast(assert_true(t.v > 0, 'positive rows only') as int))
        from transpose_semi_agg_expression_t t
        left semi join transpose_semi_agg_expression_s s on t.k = s.k
        group by t.k
    """

    // Inspect the original query directly: the join must stay above aggregation.
    qt_volatile_semi_plan """
        explain shape plan
        select d.k, d.c from (
            select k, count(*) c from transpose_semi_agg_expression_t group by k
        ) d left semi join transpose_semi_agg_expression_s s
        on d.k = s.k and random() < 0.5
    """
    qt_volatile_anti_plan """
        explain shape plan
        select d.k, d.c from (
            select k, count(*) c from transpose_semi_agg_expression_t group by k
        ) d left anti join transpose_semi_agg_expression_s s
        on d.k = s.k and random() < 0.5
    """

    // A surviving group must retain all 64 rows, regardless of the random predicate.
    // Count only invalid results so the expected output is deterministic.
    order_qt_volatile_semi_condition """
        select count(*) from (
            select d.k, d.c from (
                select k, count(*) c from transpose_semi_agg_expression_t group by k
            ) d left semi join transpose_semi_agg_expression_s s
            on d.k = s.k and random() < 0.5
        ) q where k = 1 and c <> 64
    """
    order_qt_volatile_anti_condition """
        select count(*) from (
            select d.k, d.c from (
                select k, count(*) c from transpose_semi_agg_expression_t group by k
            ) d left anti join transpose_semi_agg_expression_s s
            on d.k = s.k and random() < 0.5
        ) q where k = 1 and c <> 64
    """
}
