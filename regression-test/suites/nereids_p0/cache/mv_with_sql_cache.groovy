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
import java.util.stream.Collectors

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

suite("mv_with_sql_cache") {
    withGlobalLock("cache_last_version_interval_second") {
        sql """ADMIN SET ALL FRONTENDS CONFIG ('cache_last_version_interval_second' = '0');"""
        sql """ADMIN SET ALL FRONTENDS CONFIG ('sql_cache_manage_num' = '100000')"""
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_sql_cache=true"

        def assertHasCache = { String sqlStr ->
            explain {
                sql ("physical plan ${sqlStr}")
                contains("PhysicalSqlCache")
            }
        }

        def assertNoCache = { String sqlStr ->
            explain {
                sql ("physical plan ${sqlStr}")
                notContains("PhysicalSqlCache")
            }
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
                    extraThread("testRenameMv", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "rename_mv_with_sql_cache_"
                            def tb_name1 = prefix_str + "table1"
                            createTestTable tb_name1

                            def mv_name1 = prefix_str + "mv1"
                            def new_mv_name1 = prefix_str + "new_mv1"
                            def mv_name2 = prefix_str + "mv2"

                            def mv_sql1 = """select id as col1, sum(value) as col2 from ${tb_name1} group by id;"""
                            def mv_sql2 = """select id as col3 from ${tb_name1} group by id;"""
                            // select mv
                            def mv_str = """select col1 from ${tb_name1} index ${mv_name1} group by col1"""
                            def new_mv_str = """select col1 from ${tb_name1} index ${new_mv_name1} group by col1"""

                            assertNoCache "select * from ${tb_name1}"
                            sql "select * from ${tb_name1}"
                            assertHasCache "select * from ${tb_name1}"

                            assertNoCache mv_sql1
                            sql mv_sql1
                            assertHasCache mv_sql1

                            assertNoCache mv_sql2
                            sql mv_sql2
                            assertHasCache mv_sql2

                            // create mv
                            create_sync_mv(dbName, tb_name1, mv_name1, mv_sql1)
                            create_sync_mv(dbName, tb_name1, mv_name2, mv_sql2)

                            assertHasCache "select * from ${tb_name1}"
                            assertHasCache mv_sql1
                            assertHasCache mv_sql2

                            assertNoCache mv_str
                            sql mv_str
                            assertHasCache mv_str

                            // alter rename
                            sql """ALTER TABLE ${tb_name1} RENAME ROLLUP ${mv_name1} ${new_mv_name1};"""

                            assertNoCache "select * from ${tb_name1}"
                            assertNoCache mv_sql1
                            assertNoCache mv_sql2
                            assertNoCache new_mv_str

                            sql "select * from ${tb_name1}"
                            sql mv_sql1
                            sql mv_sql2
                            sql new_mv_str

                            assertHasCache "select * from ${tb_name1}"
                            assertHasCache mv_sql1
                            assertHasCache mv_sql2
                            assertHasCache new_mv_str
                        }
                    }),

                    extraThread("testCreateMvWithInsertData", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "create_mv_with_insert_data_sql_cache_"
                            def tb_name1 = prefix_str + "table1"
                            createTestTable tb_name1

                            def mv_name1 = prefix_str + "mv1"
                            def new_mv_name1 = prefix_str + "new_mv1"
                            def mv_name2 = prefix_str + "mv2"

                            def mv_sql1 = """select id as col1, sum(value) as col2 from ${tb_name1} group by id;"""
                            def mv_sql2 = """select id as col3 from ${tb_name1} group by id;"""
                            // select mv
                            def mv_str = """select col1 from ${tb_name1} index ${mv_name1} group by col1"""
                            def new_mv_str = """select col1 from ${tb_name1} index ${new_mv_name1} group by col1"""

                            assertNoCache "select * from ${tb_name1}"
                            sql "select * from ${tb_name1}"
                            assertHasCache "select * from ${tb_name1}"

                            assertNoCache mv_sql1
                            sql mv_sql1
                            assertHasCache mv_sql1

                            assertNoCache mv_sql2
                            sql mv_sql2
                            assertHasCache mv_sql2

                            // create mv
                            create_sync_mv(dbName, tb_name1, mv_name1, mv_sql1)
                            create_sync_mv(dbName, tb_name1, mv_name2, mv_sql2)

                            assertHasCache "select * from ${tb_name1}"
                            assertHasCache mv_sql1
                            assertHasCache mv_sql2

                            assertNoCache mv_str
                            sql mv_str
                            assertHasCache mv_str

                            // insert data
                            sql """insert into ${tb_name1} values (1, 3);"""
                            assertNoCache "select * from ${tb_name1}"
                            sql "select * from ${tb_name1}"
                            assertHasCache "select * from ${tb_name1}"

                            assertNoCache mv_sql1
                            sql mv_sql1
                            assertHasCache mv_sql1

                            assertNoCache mv_sql2
                            sql mv_sql2
                            assertHasCache mv_sql2

                            assertNoCache mv_str
                            sql mv_str
                            assertHasCache mv_str
                        }
                    }),

                    extraThread("testDropMvWithInsertData", {
                        retryTestSqlCache(3, 1000) {
                            def prefix_str = "drop_mv_with_sql_cache_"
                            def tb_name1 = prefix_str + "table1"
                            createTestTable tb_name1

                            def mv_name1 = prefix_str + "mv1"
                            def new_mv_name1 = prefix_str + "new_mv1"
                            def mv_name2 = prefix_str + "mv2"

                            def mv_sql1 = """select id as col1, sum(value) as col2 from ${tb_name1} group by id;"""
                            def mv_sql2 = """select id as col3 from ${tb_name1} group by id;"""
                            // select mv
                            def mv_str = """select col1 from ${tb_name1} index ${mv_name1} group by col1"""
                            def new_mv_str = """select col1 from ${tb_name1} index ${new_mv_name1} group by col1"""

                            assertNoCache "select * from ${tb_name1}"
                            sql "select * from ${tb_name1}"
                            assertHasCache "select * from ${tb_name1}"

                            assertNoCache mv_sql1
                            sql mv_sql1
                            assertHasCache mv_sql1

                            assertNoCache mv_sql2
                            sql mv_sql2
                            assertHasCache mv_sql2

                            // create mv
                            create_sync_mv(dbName, tb_name1, mv_name1, mv_sql1)
                            create_sync_mv(dbName, tb_name1, mv_name2, mv_sql2)

                            assertHasCache "select * from ${tb_name1}"
                            assertHasCache mv_sql1
                            assertHasCache mv_sql2

                            assertNoCache mv_str
                            sql mv_str
                            assertHasCache mv_str

                            // drop mv
                            sql """drop materialized view ${mv_name1} on ${tb_name1};"""

                            assertHasCache "select * from ${tb_name1}"
                            assertHasCache mv_sql1
                            assertHasCache mv_sql2
                        }
                    })

            ).get()
        }
    }





}
