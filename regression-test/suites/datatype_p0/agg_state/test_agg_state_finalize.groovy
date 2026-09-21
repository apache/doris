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

suite("test_agg_state_finalize") {
    sql "set enable_agg_state=true"
    sql "drop table if exists agg_finalize_input"
    sql """
        create table agg_finalize_input (k int, v int, s string, d decimal(12, 2))
        duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num"="1")
    """
    sql """
        insert into agg_finalize_input values
            (1, 1, 'a', 1.25), (1, 3, 'b', 3.75),
            (2, 10, 'c', 10.00), (3, null, null, null)
    """
    order_qt_rows """
        select k, avg_finalize(avg_state(v)), count_finalize(count_state(v)),
               sum_finalize(sum_state(v)), min_finalize(min_state(v)), max_finalize(max_state(v))
        from agg_finalize_input
    """
    order_qt_grouped """
        select k, avg_finalize(a), count_finalize(c), sum_finalize(s), min_finalize(mi), max_finalize(ma)
        from (select k, avg_combine(v) a, count_combine(v) c, sum_combine(v) s,
                     min_combine(v) mi, max_combine(v) ma from agg_finalize_input group by k) t
    """
    order_qt_empty """
        select avg_finalize(a), count_finalize(c), sum_finalize(s)
        from (select avg_combine(v) a, count_combine(v) c, sum_combine(v) s
              from agg_finalize_input where k < 0) t
    """
    order_qt_decimal """
        select k, avg_finalize(a)
        from (select k, avg_combine(d) a from agg_finalize_input group by k) t
    """
    order_qt_array """
        select k, array_sort(array_agg_finalize(a))
        from (select k, array_agg_combine(s) a from agg_finalize_input group by k) t
    """
    order_qt_parameters """
        select topn_finalize(s) from (select topn_combine('a', 2) s) t
    """
    order_qt_const """
        select number, avg_finalize(avg_state(7)), count_finalize(count_state(cast(null as int)))
        from numbers("number"="3")
    """
    // The finest groups need only a scalar projection; coarser groups merge their states.
    order_qt_rollup """
        with partial as (select k, avg_combine(v) a from agg_finalize_input group by k)
        select k, avg_finalize(a) from partial
        union all
        select null, avg_merge(a) from partial
    """
    sql "drop table if exists agg_finalize_stored"
    sql """
        create table agg_finalize_stored (
            k int, a agg_state<avg(int)> generic, c agg_state<count(int)> generic,
            s agg_state<sum(int)> generic
        ) aggregate key(k) distributed by hash(k) buckets 1
        properties("replication_num"="1")
    """
    sql """
        insert into agg_finalize_stored
        select k, avg_state(v), count_state(v), sum_state(v) from agg_finalize_input
    """
    order_qt_stored """
        select k, avg_finalize(a), count_finalize(c), sum_finalize(s) from agg_finalize_stored
    """
    order_qt_outer_null """
        select n.number, avg_finalize(t.a), count_finalize(t.c)
        from numbers("number"="5") n left join agg_finalize_stored t on n.number=t.k
    """
    order_qt_union """
        select avg_finalize(a) from (select avg_union(a) a from agg_finalize_stored) t
    """
    order_qt_alias """
        select var_pop_finalize(variance_state(v)), variance_finalize(var_pop_state(v))
        from agg_finalize_input
    """
    for (def query : ["select sum_finalize(avg_state(1))", "select avg_finalize(sum_state(1))"]) {
        test {
            sql query
            exception "requires a state of"
        }
    }
    test {
        sql "select avg_finalize(1)"
        exception "Can not found function"
    }
}
