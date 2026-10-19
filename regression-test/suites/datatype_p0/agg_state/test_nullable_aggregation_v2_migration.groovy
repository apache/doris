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

suite("test_nullable_aggregation_v2_migration") {
    sql "set enable_agg_state=true"
    sql "drop table if exists nullable_v2_input"
    sql """
        create table nullable_v2_input (
            id int not null, g int, v bigint, d decimal(27,9), w double, s string, b bitmap not null
        ) duplicate key(id) distributed by hash(id) buckets 4
        properties("replication_num"="1")
    """
    sql """
        insert into nullable_v2_input
        select number, number % 7,
               if(number % 7=6 or number % 5=0, null, number % 101),
               if(number % 7=6 or number % 5=0, null, cast(number % 101 as decimal(27,9))/100),
               if(number % 7=6 or number % 11=0, null, cast(number % 5+1 as double)),
               if(number % 7=6 or number % 5=0, null,
                  concat(repeat('wide-',128),cast(number % 11 as string))),
               bitmap_from_string('1,2')
        from numbers("number"="16000")
    """

    // Exercise the same semantics with different aggregation phases and parallelism.
    sql "set parallel_pipeline_task_num=4"
    for (int phase : [1, 2, 4]) {
        sql "set agg_phase=${phase}"
        "order_qt_stage_${phase}"("""
            select g, sum(v), avg(v), sum(d), avg(d), min(v), max(v), count(v), count(*),
                   avg_weighted(cast(v as double),w),
                   avg_weighted(cast(v as double),cast(2 as double)),
                   avg_weighted(cast(2 as double),w), bitmap_to_string(bitmap_intersect(if(v is null,null,b)))
            from nullable_v2_input group by g
        """)
        "order_qt_dense_bitmap_${phase}"("""
            select g,bitmap_to_string(bitmap_intersect(
                if(id%17000=16999,null,b)))
            from nullable_v2_input group by g
        """)
        "order_qt_sparse_bitmap_${phase}"("""
            select g,bitmap_to_string(bitmap_intersect(
                if(id<7,bitmap_from_string('1,2'),null)))
            from nullable_v2_input group by g
        """)
    }
    sql "set agg_phase=2"
    order_qt_ordered_distinct """
        select group_concat(distinct s order by s desc separator ',')
        from (select number id,if(number=3,null,if(number%2=0,'b','a')) s
              from numbers("number"="4")) t
    """
    order_qt_empty """
        select sum(v),avg(v),min(v),max(v),count(v),count(*)
        from nullable_v2_input where id<0
    """
    order_qt_window """
        select id,v,sum(v) over(order by id rows between 1 preceding and current row),
               avg(v) over(order by id rows between 1 preceding and current row)
        from nullable_v2_input where id<24
    """
    // These subqueries finalize first: no input becomes an actual empty SQL Bitmap.
    // Sparse grouped cases above and the BE UTs exercise NULL-only intermediate states.
    order_qt_bitmap_finalized_null_groups """
        select bitmap_to_string(bitmap_intersect(b))
        from (select number part,bitmap_intersect(
                     if(number=1,bitmap_from_string('1,2'),null)) b
              from numbers("number"="12") group by number) p
    """
    order_qt_bitmap_finalized_empty_groups """
        select bitmap_to_string(bitmap_intersect(b))
        from (select number part,bitmap_intersect(
                     if(number=1,bitmap_from_string('1,2'),bitmap_empty())) b
              from numbers("number"="12") group by number) p
    """
    order_qt_combine """
        select g,sum_finalize(s),avg_finalize(a),count_finalize(c),min_finalize(m),
               avg_weighted_finalize(w)
        from (select g,sum_combine(v) s,avg_combine(d) a,count_combine(v) c,
                     min_combine(s) m,avg_weighted_combine(cast(v as double),w) w
              from nullable_v2_input group by g) p
    """

    order_qt_outer_null_state """
        select g,sum_merge(st),avg_weighted_merge(wt)
        from (select g,if(id%3=0,null,sum_state(v)) st,
                     if(id%3=0,null,avg_weighted_state(cast(v as double),w)) wt
              from nullable_v2_input) p group by g
    """

    order_qt_outer_null_union """
        select g,sum_finalize(st),avg_weighted_finalize(wt)
        from (select g,sum_union(if(id%3=0,null,sum_state(v))) st,
                     avg_weighted_union(if(id%3=0,null,
                         avg_weighted_state(cast(v as double),w))) wt
              from nullable_v2_input group by g) p
    """
    order_qt_constant_outer_null """
        select sum_merge(if(true,null,sum_state(cast(1 as bigint)))),
               count_merge(if(true,null,count_state(cast(1 as bigint))))
        from numbers("number"="100")
    """

    sql "drop table if exists nullable_v2_stored_states"
    sql """
        create table nullable_v2_stored_states (
            g int,
            s agg_state<sum(bigint)> generic,
            a agg_state<avg(decimal(27,9))> generic,
            m agg_state<min(string)> generic,
            w agg_state<avg_weighted(double,double)> generic,
            b agg_state<bitmap_intersect(bitmap)> generic,
            c agg_state<collect_set(string)> generic,
            ar agg_state<array_agg(string)> generic,
            mp agg_state<map_agg_v2(string not null,string)> generic
        ) aggregate key(g) distributed by hash(g) buckets 1
        properties("replication_num"="1","disable_auto_compaction"="true")
    """
    // Two rowsets force both storage aggregation and compaction to consume the persisted codecs.
    for (int batch : [0, 1]) {
        sql """
            insert into nullable_v2_stored_states
            select g,sum_state(v),avg_state(d),min_state(s),
                   avg_weighted_state(cast(v as double),w),bitmap_intersect_state(if(v is null,null,b)),
                   collect_set_state(s),array_agg_state(s),map_agg_v2_state(cast(id as string),s)
            from nullable_v2_input where id>=${batch*8000} and id<${(batch+1)*8000}
        """
    }
    for (int read : [0, 1]) {
        if (read==1) {
            trigger_and_wait_compaction("nullable_v2_stored_states", "cumulative")
        }
        "order_qt_storage_${read}"("""
            select g,sum_merge(s),avg_merge(a),min_merge(m),avg_weighted_merge(w),
                   bitmap_to_string(bitmap_intersect_merge(b)),
                   array_size(collect_set_merge(c)),array_size(array_agg_merge(ar)),
                   map_size(map_agg_v2_merge(mp))
            from nullable_v2_stored_states group by g
        """)
        "order_qt_union_${read}"("""
            select g,sum_finalize(s),avg_finalize(a),min_finalize(m),avg_weighted_finalize(w),
                   bitmap_to_string(bitmap_intersect_finalize(b))
            from (select g,sum_union(s) s,avg_union(a) a,min_union(m) m,
                         avg_weighted_union(w) w,bitmap_intersect_union(b) b
                  from nullable_v2_stored_states group by g) p
        """)
    }
    // The published session variable remains accepted, but cannot select a V1 executor.
    for (boolean oldSwitch : [false, true]) {
        sql "set enable_aggregate_function_null_v2=${oldSwitch}"
        "order_qt_retired_switch_${oldSwitch}"("""
            select g,sum(v),avg_weighted(cast(v as double),w)
            from nullable_v2_input group by g
        """)
    }
}
