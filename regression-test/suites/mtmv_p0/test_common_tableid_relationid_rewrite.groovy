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

suite("test_common_tableid_relationid_rewrite", "mtmv") {
    // verify nested MV rewrite can choose the parent MV when the query starts from
    // base tables and the MV definition is built from already-rewritten child MVs.
    String suiteName = "test_common_tableid_relationid_rewrite"
    String dbName = context.config.getDbNameByFile(context.file)
    String factTable = "${suiteName}_fact_src"
    String dimTable = "${suiteName}_dim_full"
    String dimView = "${suiteName}_v_dim_full_non_double"
    String factMv = "${suiteName}_mv_fact"
    String dimMv = "${suiteName}_mv_dim_full"
    String dimViewMv = "${suiteName}_mv_dim_full_view_non_double"
    String targetMv = "${suiteName}_mv_target"

    sql "use ${dbName}"
    sql "set enable_nereids_planner = true"
    sql "set enable_fallback_to_original_planner = false"
    sql "set enable_materialized_view_rewrite = true"
    sql "set enable_materialized_view_nest_rewrite = true"
    sql "set enable_nereids_timeout = false"
    sql "set materialized_view_rewrite_duration_threshold_ms = 1800000"

    sql """drop materialized view if exists ${targetMv}"""
    sql """drop materialized view if exists ${dimViewMv}"""
    sql """drop materialized view if exists ${dimMv}"""
    sql """drop materialized view if exists ${factMv}"""
    sql """drop view if exists ${dimView}"""
    sql """drop table if exists ${dimTable}"""
    sql """drop table if exists ${factTable}"""

    sql """
        create table ${factTable} (
            dt date not null,
            k varchar(32) not null,
            is_dyn varchar(8),
            sku_type varchar(8)
        ) duplicate key(dt, k)
        partition by range(dt) (
            partition p1 values less than ('2026-02-05')
        )
        distributed by hash(k) buckets 1
        properties (
            "replication_num" = "1"
        )
    """

    sql """
        create table ${dimTable} (
            dt date not null,
            k varchar(32) not null,
            sku_type varchar(8),
            is_dyn varchar(8),
            bu varchar(32),
            mode_flag varchar(8),
            double_flag varchar(8)
        ) unique key(dt, k, sku_type, is_dyn)
        partition by range(dt) (
            partition p1 values less than ('2026-02-05')
        )
        distributed by hash(k) buckets 1
        properties (
            "replication_num" = "1"
        )
    """

    sql """
        create view ${dimView} as
        select dt, k, mode_flag, sku_type
        from ${dimTable}
        where double_flag = '0'
    """

    sql """
        insert into ${factTable} values ('2026-02-04', 'K1', '0', '1')
    """
    sql """
        insert into ${dimTable} values ('2026-02-04', 'K1', '1', '0', 'D1', 'M', '0')
    """

    sql """
        create materialized view ${factMv}
        build deferred refresh complete on manual
        partition by (dt)
        distributed by hash(k) buckets 1
        properties ("replication_num" = "1")
        as
        select dt, k, is_dyn, sku_type
        from ${factTable}
        where sku_type = '1'
    """
    sql """refresh materialized view ${factMv} complete"""
    waitingMTMVTaskFinishedByMvName(factMv)

    sql """
        create materialized view ${dimMv}
        build deferred refresh complete on manual
        partition by (dt)
        distributed by hash(k) buckets 1
        properties ("replication_num" = "1")
        as
        select dt, k, bu, is_dyn, sku_type
        from ${dimTable}
    """
    sql """refresh materialized view ${dimMv} complete"""
    waitingMTMVTaskFinishedByMvName(dimMv)

    sql """
        create materialized view ${dimViewMv}
        build deferred refresh complete on manual
        partition by (dt)
        distributed by hash(k) buckets 1
        properties ("replication_num" = "1")
        as
        select dt, k, mode_flag, sku_type
        from ${dimView}
    """
    sql """refresh materialized view ${dimViewMv} complete"""
    waitingMTMVTaskFinishedByMvName(dimViewMv)

    sql """
        create materialized view ${targetMv}
        build deferred refresh complete on manual
        partition by (dt)
        distributed by hash(k) buckets 1
        properties ("replication_num" = "1")
        as
        select
            t.dt,
            t.k,
            d0.bu as out_bu,
            d1.mode_flag as out_mode
        from ${factMv} t
        left join ${dimMv} d0
            on t.dt = d0.dt
            and t.k = d0.k
            and t.sku_type = d0.sku_type
            and t.is_dyn = d0.is_dyn
        left join ${dimViewMv} d1
            on t.dt = d1.dt
            and t.k = d1.k
            and t.sku_type = d1.sku_type
    """
    sql """refresh materialized view ${targetMv} complete"""
    waitingMTMVTaskFinishedByMvName(targetMv)

    String querySql = """
        select
            t.dt,
            t.k,
            d0.bu as out_bu,
            d1.mode_flag as out_mode
        from ${factTable} t
        left join ${dimTable} d0
            on t.dt = d0.dt
            and t.k = d0.k
            and t.sku_type = d0.sku_type
            and t.is_dyn = d0.is_dyn
        left join ${dimView} d1
            on t.dt = d1.dt
            and t.k = d1.k
            and t.sku_type = d1.sku_type
        where t.dt = '2026-02-04'
          and t.sku_type = '1'
        order by t.k
    """

    // First check rewrite success, then require the target MV itself to be chosen.
    mv_rewrite_success_without_check_chosen(querySql, targetMv)
    explain {
        sql("${querySql}")
        contains(".${targetMv} chose")
    }
    order_qt_common_tableid_relationid_result "${querySql}"
}
