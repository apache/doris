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

suite("test_timestamp_ns_mv") {
    sql "drop table if exists timestamp_ns_mv_probe"
    sql "drop table if exists timestamp_ns_mv_build"
    for (def tableName : ["timestamp_ns_mv_probe", "timestamp_ns_mv_build"]) {
        sql """
            create table ${tableName} (
                id int,
                dt timestamp_ns
            )
            duplicate key(id)
            distributed by hash(id) buckets 2
            properties("replication_num" = "1")
        """
    }
    sql """
        insert into timestamp_ns_mv_probe values
        (1, '1677-09-21 00:12:43.145224192'),
        (2, '1970-01-01 00:00:00.000000000'),
        (3, '1970-01-01 00:00:00.000000000'),
        (4, '2262-04-11 23:47:16.854775807'),
        (5, null),
        (6, '1970-01-01 00:00:00.000000001')
    """
    sql """
        insert into timestamp_ns_mv_build values
        (11, '1677-09-21 00:12:43.145224192'),
        (12, '2262-04-11 23:47:16.854775807')
    """

    sql "drop materialized view if exists timestamp_ns_sync_mv on timestamp_ns_mv_probe"
    create_sync_mv(context.dbName, "timestamp_ns_mv_probe", "timestamp_ns_sync_mv", """
        select dt as mv_dt, count(*) as row_count
        from timestamp_ns_mv_probe
        group by dt
    """)
    order_qt_sync_mv """
        select dt, count(*)
        from timestamp_ns_mv_probe
        group by dt
        order by dt nulls first
    """

    sql "drop table if exists timestamp_ns_aggregate_mv_base"
    sql """
        create table timestamp_ns_aggregate_mv_base (
            grp int,
            ts_key timestamp_ns,
            ts_value timestamp_ns,
            amount int
        )
        duplicate key(grp, ts_key)
        distributed by hash(grp) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into timestamp_ns_aggregate_mv_base values
        (1, '1970-01-01 00:00:00.000000001', '2024-01-01 00:00:00.123456789', 10),
        (1, '1970-01-01 00:00:00.000000001', '2024-01-01 00:00:00.123456790', 20)
    """
    create_sync_mv(context.dbName, "timestamp_ns_aggregate_mv_base", "timestamp_ns_aggregate_mv", """
        select grp as mv_grp,
               ts_key as mv_ts_key,
               min(ts_value) as mv_min_ts,
               max(ts_value) as mv_max_ts,
               sum(amount) as mv_sum
        from timestamp_ns_aggregate_mv_base
        group by grp, ts_key
    """)
    sql """
        insert into timestamp_ns_aggregate_mv_base values
        (1, '1970-01-01 00:00:00.000000001', '2024-01-01 00:00:00.123456788', 5)
    """
    sql "sync"
    sql "set enable_materialized_view_rewrite = false"
    order_qt_aggregate_mv_base_index_after_incremental_write """
        select grp,
               cast(ts_key as string),
               cast(min(ts_value) as string),
               cast(max(ts_value) as string),
               sum(amount)
        from timestamp_ns_aggregate_mv_base index timestamp_ns_aggregate_mv_base
        group by grp, ts_key
        order by grp, ts_key
    """
    sql "set enable_materialized_view_rewrite = true"

    sql "drop materialized view if exists timestamp_ns_multi_mv"
    sql """
        create materialized view timestamp_ns_multi_mv
        build deferred refresh complete on manual
        distributed by hash(probe_id) buckets 1
        properties("replication_num" = "1")
        as
        select p.id as probe_id, p.dt, b.id as build_id
        from timestamp_ns_mv_probe p
        join timestamp_ns_mv_build b on p.dt = b.dt
    """
    sql "refresh materialized view timestamp_ns_multi_mv complete"
    waitingMTMVTaskFinishedByMvName("timestamp_ns_multi_mv")
    order_qt_multi_table_mv """
        select probe_id, dt, build_id
        from timestamp_ns_multi_mv
        order by probe_id, build_id
    """

    def dateTruncMvSql = """
        select id, date_trunc(dt, 'second') as dt_second
        from timestamp_ns_mv_probe
        where dt >= cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
    """
    def exactBoundaryQuery = """
        select id from timestamp_ns_mv_probe
        where dt >= cast('1970-01-01 00:00:00.000000000' as timestamp_ns)
    """
    async_mv_rewrite_success(context.dbName, dateTruncMvSql, exactBoundaryQuery,
            "timestamp_ns_date_trunc_exact_mv")

    def afterBoundaryQuery = """
        select id from timestamp_ns_mv_probe
        where dt >= cast('1970-01-01 00:00:00.000000001' as timestamp_ns)
    """
    async_mv_rewrite_fail(context.dbName, dateTruncMvSql, afterBoundaryQuery,
            "timestamp_ns_date_trunc_after_mv")
    order_qt_date_trunc_after_boundary "${afterBoundaryQuery} order by id"
}
