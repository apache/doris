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

suite("multi_agg_push_down") {
    sql "set eager_aggregation_mode=1"
    sql "set disable_join_reorder=true"
    sql "set runtime_filter_mode=OFF"
    sql "SET fe_debug = true;"

    sql "drop table if exists multi_agg_push_down_t1"
    sql "drop table if exists multi_agg_push_down_t2"
    sql "drop table if exists multi_agg_push_down_t3"

    sql """
        create table multi_agg_push_down_t1 (
            k int not null,
            v int not null
        )
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties ("replication_num" = "1")
    """

    sql """
        create table multi_agg_push_down_t2 (
            k int not null,
            v int not null
        )
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties ("replication_num" = "1")
    """

    sql """
        create table multi_agg_push_down_t3 (
            k int not null,
            v int not null
        )
        duplicate key(k)
        distributed by hash(k) buckets 1
        properties ("replication_num" = "1")
    """

    sql "insert into multi_agg_push_down_t1 values (1, 10), (1, 20), (2, 30), (3, 40)"
    sql "insert into multi_agg_push_down_t2 values (1, 100), (1, 200), (2, 300), (4, 400)"
    sql "insert into multi_agg_push_down_t3 values (1, 1), (1, 2), (2, 3), (4, 4)"
    sql "sync"

    // The inner aggregate is rewritten first and pushes sum(t2.v) through the
    // t2/t3 join. The outer aggregate must then continue rewriting its changed
    // child and push sum(t1.v) through the t1/subquery join.
    qt_child_agg_changed_parent_agg_still_pushes """
        explain shape plan
        select t1.k, s.total_v, sum(t1.v)
        from multi_agg_push_down_t1 t1
        inner join (
            select t2.k, sum(t2.v) as total_v
            from multi_agg_push_down_t2 t2
            inner join multi_agg_push_down_t3 t3 on t2.k = t3.k
            group by t2.k
        ) s on t1.k = s.k
        group by t1.k, s.total_v
    """

    order_qt_child_agg_changed_parent_agg_still_pushes_result """
        select t1.k, s.total_v, sum(t1.v)
        from multi_agg_push_down_t1 t1
        inner join (
            select t2.k, sum(t2.v) as total_v
            from multi_agg_push_down_t2 t2
            inner join multi_agg_push_down_t3 t3 on t2.k = t3.k
            group by t2.k
        ) s on t1.k = s.k
        group by t1.k, s.total_v
        order by t1.k, s.total_v
    """


    // Besides continuing the parent rewrite, allow the parent context to stop
    // at the aggregate on the right and generate another aggregate above it.
    // Both sides of the outer join should therefore be pre-aggregated.
    qt_parent_generates_agg_above_child_agg """
        explain shape plan
        select t1.k, sum(t1.v), sum(s.total_v)
        from multi_agg_push_down_t1 t1
        inner join (
            select t2.k, sum(t2.v) as total_v
            from multi_agg_push_down_t2 t2
            inner join multi_agg_push_down_t3 t3 on t2.k = t3.k
            group by t2.k
        ) s on t1.k = s.k
        group by t1.k
    """

    order_qt_parent_generates_agg_above_child_agg_result """
        select t1.k, sum(t1.v), sum(s.total_v)
        from multi_agg_push_down_t1 t1
        inner join (
            select t2.k, sum(t2.v) as total_v
            from multi_agg_push_down_t2 t2
            inner join multi_agg_push_down_t3 t3 on t2.k = t3.k
            group by t2.k
        ) s on t1.k = s.k
        group by t1.k
        order by t1.k
    """

    // Verify that the changed-child handling composes across more than two
    // aggregate levels: bottom aggregate, middle aggregate, then top aggregate.
    qt_three_level_agg_continues_after_child_rewrite """
        explain shape plan
        select t0.k, m.total_v, m.middle_sum, sum(t0.v)
        from multi_agg_push_down_t1 t0
        inner join (
            select t1.k, s.total_v, sum(t1.v) as middle_sum
            from multi_agg_push_down_t1 t1
            inner join (
                select t2.k, sum(t2.v) as total_v
                from multi_agg_push_down_t2 t2
                inner join multi_agg_push_down_t3 t3 on t2.k = t3.k
                group by t2.k
            ) s on t1.k = s.k
            group by t1.k, s.total_v
        ) m on t0.k = m.k
        group by t0.k, m.total_v, m.middle_sum
    """

    order_qt_three_level_agg_continues_after_child_rewrite_result """
        select t0.k, m.total_v, m.middle_sum, sum(t0.v)
        from multi_agg_push_down_t1 t0
        inner join (
            select t1.k, s.total_v, sum(t1.v) as middle_sum
            from multi_agg_push_down_t1 t1
            inner join (
                select t2.k, sum(t2.v) as total_v
                from multi_agg_push_down_t2 t2
                inner join multi_agg_push_down_t3 t3 on t2.k = t3.k
                group by t2.k
            ) s on t1.k = s.k
            group by t1.k, s.total_v
        ) m on t0.k = m.k
        group by t0.k, m.total_v, m.middle_sum
    """


}
