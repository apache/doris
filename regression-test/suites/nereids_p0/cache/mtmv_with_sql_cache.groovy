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

import com.google.common.util.concurrent.Uninterruptibles
import java.util.concurrent.TimeUnit

/*
When to use a direct query and when to use a query rewrite?
If the base table changes, the query won't be able to use the materialized view (mtmv), so it can't be rewritten to hit
it. Instead, it will fall back to the original query.
All ALTER operations make the base table's cache unusable. At the same time, even if the query is successfully
rewritten, it cannot use the materialized view's cache.
In other cases, if the query can hit the materialized view, it will be rewritten because the materialized view hasn't
been refreshed, so its SQL cache is still valid.
If the query cannot hit the materialized view, it will perform a direct query on the original table. In this case, if
the original table has changed, the cache cannot be used. Conversely, if it hasn't changed, the cache can still be used.
 */

class CanRetryException extends IllegalStateException {
    CanRetryException() {
    }

    CanRetryException(String var1) {
        super(var1)
    }

    CanRetryException(String var1, Throwable var2) {
        super(var1, var2)
    }

    CanRetryException(Throwable var1) {
        super(var1)
    }
}

suite("mtmv_with_sql_cache") {
    withGlobalLock("cache_last_version_interval_second") {

        sql """ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '0');"""
        sql """ADMIN SET ALL FRONTENDS CONFIG ('sql_cache_manage_num' = '100000')"""

        def judge_res = { def sql_str ->

            sql "set enable_sql_cache=true"
            def directly_res = sql sql_str
            directly_res.sort { [it[0], it[1]] }
            sql "set enable_sql_cache=false"
            def sql_cache_res = sql sql_str
            sql_cache_res.sort { [it[0], it[1]] }
            sql "set enable_sql_cache=true"

            assertTrue(directly_res.size() == sql_cache_res.size())
            for (int i = 0; i < directly_res.size(); i++) {
                assertTrue(directly_res[i].size() == sql_cache_res[i].size())
                for (int j = 0; j < directly_res[i].size(); j++) {
                    assertTrue(directly_res[i][j] == sql_cache_res[i][j])
                }
            }
        }

        def assertHasCache = { String sqlStr ->
            explain {
                sql ("physical plan ${sqlStr}")
                contains("PhysicalSqlCache")
            }

            judge_res(sqlStr)
        }

        def assertNoCache = { String sqlStr ->
            explain {
                sql ("physical plan ${sqlStr}")
                notContains("PhysicalSqlCache")
            }
        }

        def cur_create_async_partition_mv = { def db, def mv_name, def mv_sql, def partition_col ->

            sql """DROP MATERIALIZED VIEW IF EXISTS ${db}.${mv_name}"""
            sql"""
                CREATE MATERIALIZED VIEW ${db}.${mv_name}
                BUILD IMMEDIATE REFRESH auto ON MANUAL
                PARTITION BY ${partition_col}
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS ${mv_sql}
                """
            def job_name = getJobName(db, mv_name);
            waitingMTMVTaskFinished(job_name)
            sql "analyze table ${db}.${mv_name} with sync;"
            // force meta sync to avoid stale meta data on follower fe
            sql """sync;"""
        }

        def retryTestSqlCache = { int executeTimes, long intervalMillis, Closure<Integer> closure ->
            Throwable throwable = null
            for (int i = 1; i <= executeTimes; ++i) {
                try {
                    return closure(i)
                } catch (CanRetryException t) {
                    logger.warn("Retry failed: $t", t)
                    throwable = t.getCause()
                    Uninterruptibles.sleepUninterruptibly(intervalMillis, TimeUnit.MILLISECONDS)
                } catch (Throwable t) {
                    throwable = t
                    break
                }
            }
            if (throwable != null) {
                throw throwable
            }
            return null
        }

        String dbName = context.config.getDbNameByFile(context.file)

        for (def __ in 0..3) {
            combineFutures(
                    extraThread("testRenameMtmv", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "test_rename_mtmv_"
                            def tb_name1 = prefix_str + "table1"
                            def tb_name2 = prefix_str + "table2"

                            def mv_name1 = prefix_str + "mtmv1"
                            def mv_name2 = prefix_str + "mtmv2"
                            def mv_name3 = prefix_str + "mtmv3"
                            def mv_name4 = prefix_str + "mtmv4"
                            def nested_mv_name1 = prefix_str + "nested_mtmv1"
                            def mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql2 = """
                                    select t2.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    right join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql4 = """
                                    select t1.id as id, t1.value as value1
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def nested_mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${mv_name1} as t1
                                    left join ${mv_name2} as t2
                                    on t1.id = t2.id
                                """
                            def nested_mtmv_sql3 = """
                                    select t1.id as id, t2.value as value
                                    from ${mv_name3} as t1
                                    left join ${mv_name2} as t2
                                    on t1.id = t2.id
                                """

                            createTestTable tb_name1
                            createTestTable tb_name2

                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name3};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name4};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                            cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql1, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name2, mtmv_sql2, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name4, mtmv_sql4, "(id)")
                            cur_create_async_partition_mv(dbName, nested_mv_name1, nested_mtmv_sql1, "(id)")

                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            // Direct Query
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache "select * from ${mv_name4}"
                            assertNoCache "select * from ${nested_mv_name1}"
                            // mtmv rewrite
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2
                            assertNoCache mtmv_sql4
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache "select * from ${mv_name2}"
                            retryUntilHasSqlCache "select * from ${mv_name4}"
                            retryUntilHasSqlCache "select * from ${nested_mv_name1}"
                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache mtmv_sql2
                            retryUntilHasSqlCache mtmv_sql4
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache mtmv_sql2
                            assertHasCache mtmv_sql4
                            assertHasCache nested_mtmv_sql1

                            // rename mtmv
                            sql """ALTER MATERIALIZED VIEW ${mv_name1} rename ${mv_name3};"""
                            assertNoCache "select * from ${mv_name3}"
                            assertNoCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertNoCache nested_mtmv_sql3

                            sql """ALTER MATERIALIZED VIEW ${mv_name3} rename ${mv_name1};"""
                            assertHasCache "select * from ${mv_name1}"  // Since this SQL query hasn't been executed before, so it's still valid now.
                            assertNoCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}" // nested mtmv don't change
                            assertHasCache nested_mtmv_sql1 // Since this SQL query hasn't been executed before, so it's still valid now.

                            retryUntilHasSqlCache mtmv_sql1
                            assertHasCache mtmv_sql1
                        }
                    }),

                    extraThread("testReplaceMtmv", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "test_replace_mtmv_"
                            def tb_name1 = prefix_str + "table1"
                            def tb_name2 = prefix_str + "table2"

                            def mv_name1 = prefix_str + "mtmv1"
                            def mv_name2 = prefix_str + "mtmv2"
                            def mv_name3 = prefix_str + "mtmv3"
                            def mv_name4 = prefix_str + "mtmv4"
                            def nested_mv_name1 = prefix_str + "nested_mtmv1"
                            def mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql2 = """
                                    select t2.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    right join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql4 = """
                                    select t1.id as id, t1.value as value1 
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def nested_mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${mv_name1} as t1
                                    left join ${mv_name2} as t2
                                    on t1.id = t2.id
                                """

                            createTestTable tb_name1
                            createTestTable tb_name2

                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name3};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name4};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                            cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql1, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name2, mtmv_sql2, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name4, mtmv_sql4, "(id)")
                            cur_create_async_partition_mv(dbName, nested_mv_name1, nested_mtmv_sql1, "(id)")

                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            // Direct Query
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache "select * from ${mv_name4}"
                            assertNoCache "select * from ${nested_mv_name1}"
                            // mtmv rewrite
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2
                            assertNoCache mtmv_sql4
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache "select * from ${mv_name2}"
                            retryUntilHasSqlCache "select * from ${mv_name4}"
                            retryUntilHasSqlCache "select * from ${nested_mv_name1}"
                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache mtmv_sql2
                            retryUntilHasSqlCache mtmv_sql4
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache mtmv_sql2
                            assertHasCache mtmv_sql4
                            assertHasCache nested_mtmv_sql1

                            // replace mtmv
                            sql """ALTER MATERIALIZED VIEW ${mv_name1} REPLACE WITH MATERIALIZED VIEW ${mv_name2};"""
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2// -->   "select * from mv1/mv2" --> version change  --> nocache
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertNoCache nested_mtmv_sql1

                            sql """ALTER MATERIALIZED VIEW ${mv_name2} REPLACE WITH MATERIALIZED VIEW ${mv_name1};"""
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2 // -->   "select * from mv1/mv2" --> version change  --> nocache
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache "select * from ${mv_name2}"
                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache mtmv_sql2
                            retryUntilHasSqlCache "select * from ${nested_mv_name1}"
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertHasCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache nested_mtmv_sql1
                        }
                    }),

                    extraThread("testPauseResumeMtmv", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "test_pause_resume_mtmv_"
                            def tb_name1 = prefix_str + "table1"
                            def tb_name2 = prefix_str + "table2"

                            def mv_name1 = prefix_str + "mtmv1"
                            def mv_name2 = prefix_str + "mtmv2"
                            def mv_name3 = prefix_str + "mtmv3"
                            def mv_name4 = prefix_str + "mtmv4"
                            def nested_mv_name1 = prefix_str + "nested_mtmv1"
                            def mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql2 = """
                                    select t2.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    right join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql4 = """
                                    select t1.id as id, t1.value as value1 
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def nested_mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${mv_name1} as t1
                                    left join ${mv_name2} as t2
                                    on t1.id = t2.id
                                """

                            createTestTable tb_name1
                            createTestTable tb_name2

                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name3};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name4};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                            cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql1, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name2, mtmv_sql2, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name4, mtmv_sql4, "(id)")
                            cur_create_async_partition_mv(dbName, nested_mv_name1, nested_mtmv_sql1, "(id)")

                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            // Direct Query
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache "select * from ${mv_name4}"
                            assertNoCache "select * from ${nested_mv_name1}"
                            // mtmv rewrite
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2
                            assertNoCache mtmv_sql4
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache "select * from ${mv_name2}"
                            retryUntilHasSqlCache "select * from ${mv_name4}"
                            retryUntilHasSqlCache "select * from ${nested_mv_name1}"
                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache mtmv_sql2
                            retryUntilHasSqlCache mtmv_sql4
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache mtmv_sql2
                            assertHasCache mtmv_sql4
                            assertHasCache nested_mtmv_sql1

                            // pause/resume mtmv don't affect the SQL cache's operation.
                            sql """PAUSE MATERIALIZED VIEW JOB ON ${mv_name1};"""
                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache nested_mtmv_sql1

                            sql """PAUSE MATERIALIZED VIEW JOB ON ${mv_name4};"""
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache mtmv_sql4

                            sql """RESUME MATERIALIZED VIEW JOB ON ${mv_name1};"""
                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache mtmv_sql1

                            sql """RESUME MATERIALIZED VIEW JOB ON ${mv_name4};"""
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache mtmv_sql4
                        }
                    }),
                    extraThread("testRefreshAutoMtmv", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "test_refresh_auto_mtmv_"
                            def tb_name1 = prefix_str + "table1"
                            def tb_name2 = prefix_str + "table2"

                            def mv_name1 = prefix_str + "mtmv1"
                            def mv_name2 = prefix_str + "mtmv2"
                            def mv_name3 = prefix_str + "mtmv3"
                            def mv_name4 = prefix_str + "mtmv4"
                            def nested_mv_name1 = prefix_str + "nested_mtmv1"
                            def mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql2 = """
                                    select t2.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    right join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql4 = """
                                    select t1.id as id, t1.value as value1 
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def nested_mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${mv_name1} as t1
                                    left join ${mv_name2} as t2
                                    on t1.id = t2.id
                                """

                            createTestTable tb_name1
                            createTestTable tb_name2

                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name3};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name4};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                            cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql1, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name2, mtmv_sql2, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name4, mtmv_sql4, "(id)")
                            cur_create_async_partition_mv(dbName, nested_mv_name1, nested_mtmv_sql1, "(id)")

                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            // Direct Query
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache "select * from ${mv_name4}"
                            assertNoCache "select * from ${nested_mv_name1}"
                            // mtmv rewrite
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2
                            assertNoCache mtmv_sql4
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache "select * from ${mv_name2}"
                            retryUntilHasSqlCache "select * from ${mv_name4}"
                            retryUntilHasSqlCache "select * from ${nested_mv_name1}"
                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache mtmv_sql2
                            retryUntilHasSqlCache mtmv_sql4
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache mtmv_sql2
                            assertHasCache mtmv_sql4
                            assertHasCache nested_mtmv_sql1

                            // To refresh the materialized view to ensure its initial performance is normal.
                            sql "REFRESH MATERIALIZED VIEW ${mv_name1} AUTO;"
                            waitingMTMVTaskFinishedByMvName(mv_name1)

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache nested_mtmv_sql1
                        }
                    }),

                    extraThread("testRefreshCompleteMtmv", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "test_refresh_complete_mtmv_"
                            def tb_name1 = prefix_str + "table1"
                            def tb_name2 = prefix_str + "table2"

                            def mv_name1 = prefix_str + "mtmv1"
                            def mv_name2 = prefix_str + "mtmv2"
                            def mv_name3 = prefix_str + "mtmv3"
                            def mv_name4 = prefix_str + "mtmv4"
                            def nested_mv_name1 = prefix_str + "nested_mtmv1"
                            def mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql2 = """
                                    select t2.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    right join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql4 = """
                                    select t1.id as id, t1.value as value1 
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def nested_mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${mv_name1} as t1
                                    left join ${mv_name2} as t2
                                    on t1.id = t2.id
                                """

                            createTestTable tb_name1
                            createTestTable tb_name2

                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name3};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name4};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                            cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql1, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name2, mtmv_sql2, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name4, mtmv_sql4, "(id)")
                            cur_create_async_partition_mv(dbName, nested_mv_name1, nested_mtmv_sql1, "(id)")

                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            // Direct Query
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache "select * from ${mv_name4}"
                            assertNoCache "select * from ${nested_mv_name1}"
                            // mtmv rewrite
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2
                            assertNoCache mtmv_sql4
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache "select * from ${mv_name2}"
                            retryUntilHasSqlCache "select * from ${mv_name4}"
                            retryUntilHasSqlCache "select * from ${nested_mv_name1}"
                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache mtmv_sql2
                            retryUntilHasSqlCache mtmv_sql4
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache mtmv_sql2
                            assertHasCache mtmv_sql4
                            assertHasCache nested_mtmv_sql1

                            // refresh mtmv complete
                            sql "REFRESH MATERIALIZED VIEW ${mv_name1} complete;"
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache mtmv_sql1
                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache nested_mtmv_sql1
                        }
                    }),
                    extraThread("testInsertOverwriteMtmv", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "test_insert_overwrite_mtmv_"
                            def tb_name1 = prefix_str + "table1"
                            def tb_name2 = prefix_str + "table2"

                            def mv_name1 = prefix_str + "mtmv1"
                            def mv_name2 = prefix_str + "mtmv2"
                            def mv_name3 = prefix_str + "mtmv3"
                            def mv_name4 = prefix_str + "mtmv4"
                            def nested_mv_name1 = prefix_str + "nested_mtmv1"
                            def mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql2 = """
                                    select t2.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    right join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql4 = """
                                    select t1.id as id, t1.value as value1 
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def nested_mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${mv_name1} as t1
                                    left join ${mv_name2} as t2
                                    on t1.id = t2.id
                                """

                            createTestTable tb_name1
                            createTestTable tb_name2

                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name3};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name4};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                            cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql1, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name2, mtmv_sql2, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name4, mtmv_sql4, "(id)")
                            cur_create_async_partition_mv(dbName, nested_mv_name1, nested_mtmv_sql1, "(id)")

                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            // Direct Query
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache "select * from ${mv_name4}"
                            assertNoCache "select * from ${nested_mv_name1}"
                            // mtmv rewrite
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2
                            assertNoCache mtmv_sql4
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache "select * from ${mv_name2}"
                            retryUntilHasSqlCache "select * from ${mv_name4}"
                            retryUntilHasSqlCache "select * from ${nested_mv_name1}"
                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache mtmv_sql2
                            retryUntilHasSqlCache mtmv_sql4
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache mtmv_sql2
                            assertHasCache mtmv_sql4
                            assertHasCache nested_mtmv_sql1

                            // base table insert overwrite
                            sql "INSERT OVERWRITE table ${tb_name1} PARTITION(p5) VALUES (5, 6);"
                            assertHasCache "select * from ${mv_name1}"
                            assertNoCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache nested_mtmv_sql1

                            retryUntilHasSqlCache mtmv_sql1
                            assertHasCache mtmv_sql1

                            sql "REFRESH MATERIALIZED VIEW ${mv_name1} AUTO;"
                            waitingMTMVTaskFinishedByMvName(mv_name1)
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache nested_mtmv_sql1
                        }
                    }),
                    extraThread("testInsertIntoMtmv", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "test_insert_into_mtmv_"
                            def tb_name1 = prefix_str + "table1"
                            def tb_name2 = prefix_str + "table2"

                            def mv_name1 = prefix_str + "mtmv1"
                            def mv_name2 = prefix_str + "mtmv2"
                            def mv_name3 = prefix_str + "mtmv3"
                            def mv_name4 = prefix_str + "mtmv4"
                            def nested_mv_name1 = prefix_str + "nested_mtmv1"
                            def mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql2 = """
                                    select t2.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    right join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql4 = """
                                    select t1.id as id, t1.value as value1 
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def nested_mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${mv_name1} as t1
                                    left join ${mv_name2} as t2
                                    on t1.id = t2.id
                                """

                            createTestTable tb_name1
                            createTestTable tb_name2

                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name3};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name4};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                            cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql1, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name2, mtmv_sql2, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name4, mtmv_sql4, "(id)")
                            cur_create_async_partition_mv(dbName, nested_mv_name1, nested_mtmv_sql1, "(id)")

                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            // Direct Query
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache "select * from ${mv_name4}"
                            assertNoCache "select * from ${nested_mv_name1}"
                            // mtmv rewrite
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2
                            assertNoCache mtmv_sql4
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache "select * from ${mv_name2}"
                            retryUntilHasSqlCache "select * from ${mv_name4}"
                            retryUntilHasSqlCache "select * from ${nested_mv_name1}"
                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache mtmv_sql2
                            retryUntilHasSqlCache mtmv_sql4
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache mtmv_sql2
                            assertHasCache mtmv_sql4
                            assertHasCache nested_mtmv_sql1

                            // add partition
                            sql "alter table ${tb_name1} add partition p6 values[('6'),('7'))"
                            assertHasCache "select * from ${mv_name1}"
                            assertNoCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache nested_mtmv_sql1

                            retryUntilHasSqlCache mtmv_sql1
                            assertHasCache mtmv_sql1

                            // base table insert data
                            sql "insert into ${tb_name1} values(6, 1)"
                            assertHasCache "select * from ${mv_name1}"
                            assertNoCache mtmv_sql1  // mtmv no work -> directly base table -> no cache
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache nested_mtmv_sql1  // nested mtmv no work -> mtmv cache work -> has cache

                            retryUntilHasSqlCache mtmv_sql1
                        }
                    }),
                    extraThread("testAddColumnMtmv", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "test_add_column_mtmv_"
                            def tb_name1 = prefix_str + "table1"
                            def tb_name2 = prefix_str + "table2"

                            def mv_name1 = prefix_str + "mtmv1"
                            def mv_name2 = prefix_str + "mtmv2"
                            def mv_name3 = prefix_str + "mtmv3"
                            def mv_name4 = prefix_str + "mtmv4"
                            def nested_mv_name1 = prefix_str + "nested_mtmv1"
                            def mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql2 = """
                                    select t2.id as id, t2.value as value
                                    from ${tb_name1} as t1
                                    right join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql3 = """
                                    select t2.id as id, t1.value as value1 
                                    from ${tb_name1} as t1
                                    right join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def mtmv_sql4 = """
                                    select t1.id as id, t1.value as value1 
                                    from ${tb_name1} as t1
                                    left join ${tb_name2} as t2
                                    on t1.id = t2.id
                                """
                            def nested_mtmv_sql1 = """
                                    select t1.id as id, t2.value as value
                                    from ${mv_name1} as t1
                                    left join ${mv_name2} as t2
                                    on t1.id = t2.id
                                """

                            createTestTable tb_name1
                            createTestTable tb_name2

                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name1};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name2};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name3};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name4};"""
                            sql """DROP MATERIALIZED VIEW IF EXISTS ${nested_mv_name1};"""
                            cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql1, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name2, mtmv_sql2, "(id)")
                            cur_create_async_partition_mv(dbName, mv_name4, mtmv_sql4, "(id)")
                            cur_create_async_partition_mv(dbName, nested_mv_name1, nested_mtmv_sql1, "(id)")

                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            // Direct Query
                            assertNoCache "select * from ${mv_name1}"
                            assertNoCache "select * from ${mv_name2}"
                            assertNoCache "select * from ${mv_name4}"
                            assertNoCache "select * from ${nested_mv_name1}"
                            // mtmv rewrite
                            assertNoCache mtmv_sql1
                            assertNoCache mtmv_sql2
                            assertNoCache mtmv_sql4
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            retryUntilHasSqlCache "select * from ${mv_name2}"
                            retryUntilHasSqlCache "select * from ${mv_name4}"
                            retryUntilHasSqlCache "select * from ${nested_mv_name1}"
                            retryUntilHasSqlCache mtmv_sql1
                            retryUntilHasSqlCache mtmv_sql2
                            retryUntilHasSqlCache mtmv_sql4
                            retryUntilHasSqlCache nested_mtmv_sql1

                            assertHasCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertHasCache "select * from ${mv_name4}"
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertHasCache mtmv_sql1
                            assertHasCache mtmv_sql2
                            assertHasCache mtmv_sql4
                            assertHasCache nested_mtmv_sql1

                            // recreate mtmv to add column
                            cur_create_async_partition_mv(dbName, mv_name1, mtmv_sql3, "(id)")
                            assertNoCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name2}"
                            assertNoCache mtmv_sql1
                            assertHasCache "select * from ${nested_mv_name1}"
                            assertNoCache nested_mtmv_sql1

                            retryUntilHasSqlCache "select * from ${mv_name1}"
                            assertHasCache "select * from ${mv_name1}"

                            sql "REFRESH MATERIALIZED VIEW ${mv_name2} AUTO;"
                            waitingMTMVTaskFinishedByMvName(mv_name2)
                            assertHasCache "select * from ${mv_name2}"

                            retryUntilHasSqlCache mtmv_sql1
                            assertHasCache mtmv_sql1
                            retryUntilHasSqlCache nested_mtmv_sql1
                            assertHasCache nested_mtmv_sql1
                        }
                    })

            ).get()
        }

    }


}
