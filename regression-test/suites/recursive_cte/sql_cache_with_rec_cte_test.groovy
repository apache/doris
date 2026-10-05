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

suite("sql_cache_with_rec_cte_test", "rec_cte") {
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
                    // Recursive CTE query without a physical table
                    extraThread("recCteWithoutPhysicalTable", {
                        retryTestSqlCache(3, 1000) {
                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            def level = 10
                            def sql_str = """WITH recursive RecursiveCounter (
                                    N,
                                    Depth
                                )
                                AS
                                (
                                    SELECT 
                                        cast(1 as bigint) AS N, 
                                        cast(1 as bigint) AS Depth
                                    UNION ALL
                                    SELECT 
                                        cast(N + 1 as bigint) AS N, 
                                        cast(Depth + 1 as bigint) AS Depth 
                                    FROM 
                                        RecursiveCounter           
                                    WHERE 
                                        Depth < ${level}
                                )
                                SELECT 
                                    N,
                                    Depth
                                FROM 
                                    RecursiveCounter
                                ORDER BY
                                    Depth;"""

                            sql sql_str
                            assertNoCache sql_str
                        }
                    }),

                    extraThread("recCteWithPhysicalTable", {
                        retryTestSqlCache(3, 1000) {
                            sql "set enable_nereids_planner=true"
                            sql "set enable_fallback_to_original_planner=false"
                            sql "set enable_sql_cache=true"

                            def tb_name1 = "rec_cte_with_physical_table1"
                            def tb_name2 = "rec_cte_with_physical_table2"
                            sql """drop table if exists ${tb_name1}"""
                            sql """CREATE TABLE ${tb_name1} (
                                        k1 INT,
                                        v1 DOUBLE
                                    ) 
                                    DISTRIBUTED BY HASH(k1) BUCKETS 3
                                    PROPERTIES ("replication_num" = "1");"""
                            sql """INSERT INTO ${tb_name1} VALUES (1, 10.5), (1, 20.5), (2, 100.0), (3, 200.0);"""

                            sql """drop table if exists ${tb_name2}"""
                            sql """CREATE TABLE ${tb_name2} (
                                        id INT,
                                        parent_id INT
                                    ) DISTRIBUTED BY HASH(id) BUCKETS 1
                                    PROPERTIES ("replication_num" = "1");"""

                            sql """INSERT INTO ${tb_name2} VALUES (1, 0), (2, 1), (3, 2), (4, 3);"""
                            sql "sync"
                            sleep(10 * 1000)

                            def sql_str = """WITH RECURSIVE hierarchical_sum AS (
                                SELECT 
                                    m.k1 AS id, 
                                    SUM(m.v1) AS total_val, 
                                    cast(0 as int) AS level
                                FROM ${tb_name1} m
                                GROUP BY m.k1
                                HAVING SUM(m.v1) > 10
                                
                                UNION ALL
                                
                                SELECT 
                                    r.id, 
                                    hs.total_val, 
                                    cast(hs.level + 1 as int)
                                FROM ${tb_name2} r
                                JOIN hierarchical_sum hs ON r.parent_id = hs.id
                                WHERE hs.level < 5
                            )
                            SELECT * FROM hierarchical_sum;"""
                            retryUntilHasSqlCache sql_str
                            assertHasCache sql_str
                        }
                    })


            ).get()
        }

    }
}
