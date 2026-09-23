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

suite("test_agg_state_union_batch") {
    sql "set enable_agg_state=true"
    sql "drop table if exists agg_state_union_batch_input"
    sql """
        create table agg_state_union_batch_input (
            k int not null, v bigint not null, n bigint, d decimal(27, 9), s string
        ) duplicate key(k) distributed by hash(k) buckets 1
        properties("replication_num"="1")
    """
    sql """
        insert into agg_state_union_batch_input
        select number % 7, number,
               if(number % 7 = 6 or number % 5 = 0, null, number),
               if(number % 7 = 6, null, cast(number as decimal(27, 9)) / 100),
               if(number % 7 = 6, null, concat('value-', cast(number as string)))
        from numbers("number"="10000")
    """
    explain {
        sql "select k, sum_merge(sum_state(v)) from agg_state_union_batch_input group by k"
        contains "sum_merge"
    }
    explain {
        sql "select k, sum_union(sum_state(v)) from agg_state_union_batch_input group by k"
        contains "sum_union"
    }
    // Multiple input batches, repeated groups, multiple state offsets, and an all-NULL group.
    order_qt_merge """
        select k, sum_merge(sum_state(v)), sum_merge(sum_state(n)),
               sum_merge(sum_state(d)), min_merge(min_state(s))
        from agg_state_union_batch_input group by k
    """
    order_qt_direct """
        select k, sum(v), sum(n), sum(d), min(s)
        from agg_state_union_batch_input group by k
    """
    order_qt_union """
        select k, sum_finalize(sv), sum_finalize(sn), sum_finalize(sd), min_finalize(ms)
        from (
            select k, sum_union(sum_state(v)) sv, sum_union(sum_state(n)) sn,
                   sum_union(sum_state(d)) sd, min_union(min_state(s)) ms
            from agg_state_union_batch_input group by k
        ) t
    """
    // ARRAY_AGG owns nontrivial state. Read its result after all batch scratch storage is freed.
    order_qt_array """
        with merged as (
            select k, array_sort(array_agg_merge(array_agg_state(s))) a
            from agg_state_union_batch_input group by k
        ), united as (
            select k, array_sort(array_agg_finalize(a)) a
            from (
                select k, array_agg_union(array_agg_state(s)) a
                from agg_state_union_batch_input group by k
            ) t
        ), direct as (
            select k, array_sort(array_agg(s)) a
            from agg_state_union_batch_input group by k
        )
        select d.k, m.a <=> d.a, u.a <=> d.a
        from direct d join merged m on d.k=m.k join united u on d.k=u.k
    """
}
