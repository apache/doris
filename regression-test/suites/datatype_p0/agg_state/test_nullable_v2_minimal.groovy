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

suite("test_nullable_v2_minimal") {
    sql "set enable_agg_state=true"

    // Two states are enough to detect the lost intersection: {1} and no input.
    // STATE preserves the partial state. Finalizing the NULL row first would instead
    // produce an actual empty SQL Bitmap and would test a different operation.
    for (int phase : [1, 2, 4]) {
        sql "set agg_phase=${phase}"
        for (int parallelism : [1, 2]) {
            sql "set parallel_pipeline_task_num=${parallelism}"
            "order_qt_null_partial_${phase}_${parallelism}"("""
                select bitmap_to_string(bitmap_intersect_merge(s))
                from (select bitmap_intersect_state(if(number=0,to_bitmap(1),null)) s
                      from numbers("number"="2")) t
            """)
            // A valid empty Bitmap must still remove {1} from the intersection.
            "order_qt_empty_bitmap_${phase}_${parallelism}"("""
                select bitmap_to_string(bitmap_intersect_merge(s))
                from (select bitmap_intersect_state(if(number=0,to_bitmap(1),bitmap_empty())) s
                      from numbers("number"="2")) t
            """)
        }
    }

    // Four rows cover every NULL pair. The constant arguments also cover mixed
    // nullable/nonnullable input types without adding more rows.
    order_qt_variadic_positions """
        select avg_weighted(v,w),avg_weighted(v,cast(1 as double)),
               avg_weighted(cast(1 as double),w)
        from (select cast(if(number in (1,3),null,if(number=0,10,20)) as double) v,
                     cast(if(number in (2,3),null,if(number=0,1,2)) as double) w
              from numbers("number"="4")) t
    """

    // One NULL row distinguishes an all-NULL aggregate from COUNT(*).
    order_qt_all_null """
        select sum(v),avg(v),min(v),max(v),count(v),count(*)
        from (select cast(null as bigint) v) t
    """
    // No input must also leave SUM/AVG/MIN/MAX NULL and both counts zero.
    order_qt_empty_input """
        select sum(v),avg(v),min(v),max(v),count(v),count(*)
        from (select cast(null as bigint) v) t where false
    """

    // Three rows move the window from all NULL to valid and back to all NULL.
    order_qt_window """
        select number,sum(v) over(order by number rows between current row and current row),
               avg(v) over(order by number rows between current row and current row),
               avg_weighted(cast(v as double),cast(1 as double))
                   over(order by number rows between current row and current row)
        from (select number,if(number=1,cast(7 as bigint),null) v
              from numbers("number"="3")) t
    """

    // One valid string and one NULL check the generic collection and topn paths.
    order_qt_collections """
        select array_sort(collect_set(s)),topn(s,1)
        from (select if(number=0,'a',null) s from numbers("number"="2")) t
    """

    sql "drop table if exists nullable_v2_minimal_states"
    sql """
        create table nullable_v2_minimal_states (
            g int,
            s agg_state<sum(bigint)> generic,
            c agg_state<count(bigint)> generic,
            b agg_state<bitmap_intersect(bitmap)> generic,
            bn agg_state<bitmap_union(bitmap not null)> generic,
            ar agg_state<array_agg(string)> generic,
            mp agg_state<map_agg_v2(string not null,string)> generic
        ) aggregate key(g) distributed by hash(g) buckets 1
        properties("replication_num"="1","disable_auto_compaction"="true")
    """
    // Exactly two one-row writes create different rowsets. The second write has
    // a NULL-only bitmap partial. This checks String, fixed, Bitmap, Array and Map storage.
    sql """
        insert into nullable_v2_minimal_states
        select 1,sum_state(cast(1 as bigint)),count_state(cast(1 as bigint)),
               bitmap_intersect_state(if(true,to_bitmap(1),null)),bitmap_union_state(bitmap_empty()),
               array_agg_state(cast('a' as string)),map_agg_v2_state('a',cast('a' as string))
    """
    sql """
        insert into nullable_v2_minimal_states
        select 1,sum_state(cast(null as bigint)),count_state(cast(null as bigint)),
               bitmap_intersect_state(if(false,to_bitmap(1),null)),bitmap_union_state(bitmap_empty()),
               array_agg_state(cast(null as string)),map_agg_v2_state('b',cast(null as string))
    """
    for (int compacted : [0, 1]) {
        if (compacted == 1) {
            def tablets = sql_return_maparray "show tablets from nullable_v2_minimal_states"
            def backendIdToIp = [:]
            def backendIdToHttpPort = [:]
            getBackendIpHttpPort(backendIdToIp, backendIdToHttpPort)
            def countRowsets = { tablet ->
                def (code, stdout, stderr) = be_show_tablet_status(
                    backendIdToIp["${tablet.BackendId}"],
                    backendIdToHttpPort["${tablet.BackendId}"], tablet.TabletId)
                assertEquals(0, code)
                return parseJson(stdout.trim()).rowsets.size()
            }
            def beforeCounts = tablets.collectEntries { [(it.TabletId): countRowsets(it)] }
            trigger_and_wait_compaction("nullable_v2_minimal_states", "full")
            // Check the physical merge as well as the SQL values. A successful
            // compaction request alone does not prove that two rowsets were merged.
            tablets.each { tablet ->
                assertTrue(countRowsets(tablet) < beforeCounts[tablet.TabletId])
            }
        }
        "order_qt_storage_${compacted}"("""
            select g,sum_merge(s),count_merge(c),bitmap_to_string(bitmap_intersect_merge(b)),
                   bitmap_count(bitmap_union_merge(bn)),array_sort(array_agg_merge(ar)),
                   map_size(map_agg_v2_merge(mp))
            from nullable_v2_minimal_states group by g
        """)
        "order_qt_union_finalize_${compacted}"("""
            select sum_finalize(s),bitmap_to_string(bitmap_intersect_finalize(b))
            from (select sum_union(s) s,bitmap_intersect_union(b) b
                  from nullable_v2_minimal_states group by g) t
        """)
    }
}
