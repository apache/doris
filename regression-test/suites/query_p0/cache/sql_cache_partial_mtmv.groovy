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

suite("sql_cache_partial_mtmv") {
    withGlobalLock("cache_last_version_interval_second") {
        def originalInterval = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'cache_last_version_interval_second'")[0][1]
        sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '0')"
        try {
            sql "set enable_sql_cache=false"
            sql "set enable_strong_consistency_read=true"
            sql "set cloud_table_version_cache_ttl_ms=0"
            sql "set cloud_partition_version_cache_ttl_ms=0"
            sql "set enable_materialized_view_rewrite=true"
            sql "set enable_materialized_view_union_rewrite=true"
            sql "drop materialized view if exists sql_cache_partial_mtmv_view"
            sql "drop table if exists sql_cache_partial_mtmv_base"
            sql """
                create table sql_cache_partial_mtmv_base (dt date, v int)
                duplicate key(dt)
                partition by range(dt) (
                    partition p1 values [('2026-10-27'), ('2026-10-28')),
                    partition p2 values [('2026-10-28'), ('2026-10-29')),
                    partition p3 values [('2026-10-29'), ('2026-10-30'))
                ) distributed by hash(dt) buckets 1
                properties ('replication_num'='1')
            """
            sql """insert into sql_cache_partial_mtmv_base values
                ('2026-10-27', 10), ('2026-10-28', 20), ('2026-10-29', 30)"""
            sql """
                create materialized view sql_cache_partial_mtmv_view
                build immediate refresh auto on manual
                partition by(dt)
                distributed by random buckets 1
                properties ('replication_num'='1', 'grace_period'='0')
                as select dt, v from sql_cache_partial_mtmv_base
            """
            def db = sql("select database()")[0][0].toString()
            waitingMTMVTaskFinished(getJobName(db, "sql_cache_partial_mtmv_view"))
            // p1 becomes stale in the MV; p2 must still be served by the MV.
            sql "insert into sql_cache_partial_mtmv_base values ('2026-10-27', 11)"
            // Select the MV plan even when this tiny table makes a direct scan cheaper.
            def query = """select /*+ use_mv(sql_cache_partial_mtmv_view) */ dt, v
                from sql_cache_partial_mtmv_base
                where dt >= '2026-10-27' and dt < '2026-10-29' order by dt, v"""
            explain {
                sql "physical plan ${query}"
                contains "PhysicalUnion"
                contains "sql_cache_partial_mtmv_view"
                contains "sql_cache_partial_mtmv_base"
            }
            sql "set enable_sql_cache=true"
            order_qt_before_write query
            explain {
                sql "physical plan ${query}"
                contains "PhysicalSqlCache"
            }

            // The physical base scan covered only p1, but the query also depends on base p2.
            sql "insert into sql_cache_partial_mtmv_base values ('2026-10-28', 21)"
            explain {
                sql "physical plan ${query}"
                notContains "PhysicalSqlCache"
            }
            order_qt_after_write query
            explain {
                sql "physical plan ${query}"
                contains "PhysicalSqlCache"
            }
            sql "set enable_sql_cache=false"
            order_qt_without_cache query
        } finally {
            sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '${originalInterval}')"
        }
    }
}
