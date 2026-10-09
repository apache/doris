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

suite("window_function_spill") {
    sql "drop table if exists window_function_spill_t"
    sql """
        create table window_function_spill_t (
            k int not null,
            g int not null,
            skew int not null,
            o int not null,
            v bigint null,
            d decimal(18, 2) null,
            f double null
        ) duplicate key(k)
        distributed by hash(k) buckets 3
        properties("replication_num" = "1")
    """
    // Partition g = 3 only contains NULL v values; skew puts 75% of the rows into one partition.
    // f is exactly representable so floating-point sums do not depend on the row arrival order.
    sql """
        insert into window_function_spill_t
        select number,
               number % 50,
               case when number < 15000 then 0 else number % 50 end,
               number % 13,
               case when number % 50 = 3 then null else number end,
               case when number % 7 = 0 then null else cast(number as decimal(18, 2)) / 3 end,
               case when number % 11 = 0 then null else number / 4.0 end
        from numbers("number" = "20000")
    """

    // Small Blocks make partitions and peer groups cross Block boundaries.
    sql "set batch_size = 97"
    sql "set parallel_pipeline_task_num = 4"

    def run_queries = { String tag ->
        // Full-partition aggregates over high-cardinality partitions.
        "order_qt_${tag}_high_cardinality"("""
            select count(*), sum(s), sum(c), sum(mn), sum(mx), count(s)
            from (
                select sum(v) over (partition by k) s,
                       count(v) over (partition by k) c,
                       min(v) over (partition by k) mn,
                       max(v) over (partition by k) mx
                from window_function_spill_t) w
        """)
        // Several full-partition aggregates in the same node, including an all-NULL partition.
        "order_qt_${tag}_partition_reduce"("""
            select g, count(*), max(s), max(c), max(mn), max(mx), max(a), max(af), count(s)
            from (
                select g,
                       sum(v) over (partition by g) s,
                       count(v) over (partition by g) c,
                       min(v) over (partition by g) mn,
                       max(v) over (partition by g) mx,
                       avg(d) over (partition by g) a,
                       avg(f) over (partition by g) af
                from window_function_spill_t) w
            group by g
        """)
        "order_qt_${tag}_skew"("""
            select skew, count(*), max(s), max(c), max(af)
            from (
                select skew,
                       sum(v) over (partition by skew order by k rows between
                                    unbounded preceding and unbounded following) s,
                       count(*) over (partition by skew) c,
                       avg(f) over (partition by skew) af
                from window_function_spill_t) w
            group by skew
        """)
        "order_qt_${tag}_no_partition"("""
            select count(*), max(s), min(s), max(c), max(mx), min(mn)
            from (
                select sum(v) over () s,
                       count(*) over () c,
                       max(v) over () mx,
                       min(d) over () mn
                from window_function_spill_t) w
        """)
        // ntile bucket counts are deterministic even when order keys have ties.
        "order_qt_${tag}_ntile"("""
            select g, n4, count(*), count(distinct n1000), max(n1000)
            from (
                select g,
                       ntile(4) over (partition by g order by o) n4,
                       ntile(1000) over (partition by g order by o) n1000
                from window_function_spill_t) w
            where g in (0, 3, 49)
            group by g, n4
        """)
        "order_qt_${tag}_ntile_skew"("""
            select skew, n, count(*)
            from (
                select skew, ntile(3) over (partition by skew order by o, k) n
                from window_function_spill_t) w
            where skew in (0, 1)
            group by skew, n
        """)
        "order_qt_${tag}_peer_group"("""
            select g, o, count(*), max(pr), min(pr), max(cd), min(cd)
            from (
                select g, o,
                       percent_rank() over (partition by g order by o) pr,
                       cume_dist() over (partition by g order by o) cd
                from window_function_spill_t) w
            where g in (0, 3, 49)
            group by g, o
        """)
        "order_qt_${tag}_peer_group_no_partition"("""
            select o, count(*), max(pr), min(pr), max(cd), min(cd)
            from (
                select o,
                       percent_rank() over (order by o) pr,
                       cume_dist() over (order by o) cd
                from window_function_spill_t) w
            group by o
        """)
        "order_qt_${tag}_peer_group_high_cardinality"("""
            select count(*), max(pr), min(pr), max(cd), min(cd)
            from (
                select percent_rank() over (partition by k order by o) pr,
                       cume_dist() over (partition by k order by o) cd
                from window_function_spill_t) w
        """)
        // row_number is not spillable, so the whole node falls back to the in-memory path.
        "order_qt_${tag}_unsupported_fallback"("""
            select g, n4, count(*), max(rn), min(rn)
            from (
                select g,
                       row_number() over (partition by g order by o, k) rn,
                       ntile(4) over (partition by g order by o, k) n4
                from window_function_spill_t) w
            where g < 3
            group by g, n4
        """)
        // Streaming windows keep using the existing path.
        "order_qt_${tag}_streaming"("""
            select g, count(*), max(s), sum(s)
            from (
                select g,
                       sum(v) over (partition by g order by k rows between unbounded preceding
                                    and current row) s
                from window_function_spill_t) w
            where g < 5
            group by g
        """)
        "order_qt_${tag}_rows"("""
            select * from (
                select k, g, v,
                       sum(v) over (partition by g) s,
                       ntile(2) over (partition by g order by k) n,
                       percent_rank() over (partition by g order by o) pr
                from window_function_spill_t
                where k < 12) w
        """)
    }

    sql "set enable_spill = false"
    run_queries("no_spill")

    sql "set enable_spill = true"
    sql "set enable_force_spill = false"
    run_queries("spill_in_memory")

    sql "set enable_force_spill = true"
    sql "set spill_min_revocable_mem = 1"
    run_queries("force_spill")
}
