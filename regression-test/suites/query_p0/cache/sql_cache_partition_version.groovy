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

suite("sql_cache_partition_version") {
    withGlobalLock("cache_last_version_interval_second") {
        def originalInterval = sql("ADMIN SHOW FRONTEND CONFIG LIKE 'cache_last_version_interval_second'")[0][1]
        sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '0')"
        try {
            sql "set enable_sql_cache=true"
            sql "set enable_strong_consistency_read=true"
            sql "set cloud_table_version_cache_ttl_ms=0"
            sql "set cloud_partition_version_cache_ttl_ms=0"
            sql "drop table if exists sql_cache_partition_version"
            sql """
                create table sql_cache_partition_version (dt date, v int)
                duplicate key(dt)
                partition by range(dt) (
                    partition p1 values [('2026-10-27'), ('2026-10-28')),
                    partition p2 values [('2026-10-28'), ('2026-10-29')),
                    partition p3 values [('2026-10-29'), ('2026-10-30'))
                ) distributed by hash(dt) buckets 1
                properties ('replication_num'='1')
            """
            sql "insert into sql_cache_partition_version values ('2026-10-27', 10)"
            def history = "select sum(v) from sql_cache_partition_version where dt = '2026-10-27'"
            def assertCached = { String query ->
                explain {
                    sql "physical plan ${query}"
                    contains "PhysicalSqlCache"
                }
            }
            def assertNotCached = { String query ->
                explain {
                    sql "physical plan ${query}"
                    notContains "PhysicalSqlCache"
                }
            }

            order_qt_history history
            assertCached(history)
            // The first write to an unrelated empty partition must preserve the cached query.
            sql "insert into sql_cache_partition_version values ('2026-10-28', 20)"
            assertCached(history)
            order_qt_unrelated_write history

            sql "insert into sql_cache_partition_version values ('2026-10-27', 30)"
            assertNotCached(history)
            order_qt_relevant_write history
            assertCached(history)

            // p3 is selected by the predicate but removed by empty-partition pruning.
            def includingEmpty = "select sum(v) from sql_cache_partition_version where dt >= '2026-10-27'"
            order_qt_including_empty includingEmpty
            assertCached(includingEmpty)
            def emptyBranch = """select sum(v) from sql_cache_partition_version where dt = '2026-10-27'
                union all select sum(v) from sql_cache_partition_version where dt = '2026-10-29'"""
            order_qt_empty_branch emptyBranch
            assertCached(emptyBranch)
            sql "insert into sql_cache_partition_version values ('2026-10-29', 40)"
            assertNotCached(includingEmpty)
            assertNotCached(emptyBranch)
            order_qt_empty_branch_write emptyBranch
            order_qt_empty_first_write includingEmpty
            assertCached(includingEmpty)
            assertCached(history)

            // A second occurrence of the same table must contribute its partitions as well.
            def selfJoin = """select sum(a.v + b.v) from sql_cache_partition_version a
                join sql_cache_partition_version b on a.v < b.v
                where a.dt = '2026-10-27' and b.dt = '2026-10-28'"""
            order_qt_self_join selfJoin
            assertCached(selfJoin)
            sql "insert into sql_cache_partition_version values ('2026-10-28', 50)"
            assertNotCached(selfJoin)
            order_qt_self_join_write selfJoin
            assertCached(selfJoin)
            assertCached(history)

            // Partition topology changes invalidate even if all scanned versions stay the same.
            sql "alter table sql_cache_partition_version add partition p4 values [('2026-10-30'), ('2026-10-31'))"
            assertNotCached(history)
            order_qt_add_partition history
            assertCached(history)
            sql "truncate table sql_cache_partition_version partition(p1)"
            assertNotCached(history)
            order_qt_truncate_partition history
        } finally {
            sql "ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '${originalInterval}')"
        }
    }
}
