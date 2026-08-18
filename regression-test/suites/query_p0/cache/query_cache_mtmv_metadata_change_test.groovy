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

// ===================================================================================
// Query Cache and MTMV metadata change tests
//
// Coverage:
//   - rename MTMV
//   - replace MTMV
//   - drop + recreate MTMV
//
// Each scenario verifies three query paths at the same time:
//   - Direct base-table query without MTMV rewrite
//   - Direct base-table query rewritten to hit MTMV
//   - Query path with nested MTMV
// ===================================================================================

suite("query_cache_mtmv_metadata_change_test") {
    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=true"
        sql "set enable_materialized_view_nest_rewrite=true;"
    }

    def assertHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                assertTrue(profileString.contains("HitCache:  1"))
                assertFalse(profileString.contains("HitCache:  0"))
            }
        }
    }

    def assertNoCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                assertTrue(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    def judgeRes = { def res1, def res2 ->
        assertTrue(res1.size() == res2.size())
        for (int i = 0; i < res1.size(); i++) {
            assertTrue(res1[i].size() == res2[i].size())
            for (int j = 0; j < res1[i].size(); j++) {
                assertTrue(res1[i][j] == res2[i][j])
            }
        }
    }

    def createTableAndInsert = { def tableName ->
        sql """drop table if exists ${tableName}"""
        sql """CREATE TABLE ${tableName} (
                product_id INT NOT NULL,
                city VARCHAR(50) NOT NULL,
                sale_date DATE NOT NULL,
                amount DECIMAL(18, 2) NOT NULL
            )
            DUPLICATE KEY(product_id, city, sale_date)
            PARTITION BY RANGE(sale_date) (
                PARTITION p20251001 VALUES [('2025-10-01'), ('2025-10-02')),
                PARTITION p20251002 VALUES [('2025-10-02'), ('2025-10-03')),
                PARTITION p20251003 VALUES [('2025-10-03'), ('2025-10-04')),
                PARTITION p_other VALUES [('2025-10-04'), ('2025-11-01'))
            )
            DISTRIBUTED BY HASH(product_id) BUCKETS 10
            PROPERTIES (
                "replication_num" = "1"
            );"""
        sql """INSERT INTO ${tableName} (product_id, city, sale_date, amount) VALUES
            (101, 'Beijing', '2025-10-01', 100.00),
            (101, 'Shanghai', '2025-10-01', 150.00),
            (102, 'Beijing', '2025-10-02', 200.00),
            (102, 'Shanghai', '2025-10-02', 250.00),
            (101, 'Beijing', '2025-10-03', 120.00),
            (102, 'Shanghai', '2025-10-03', 300.00);"""
    }

    String dbName = context.config.getDbNameByFile(context.file)
    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {

    combineFutures(
            extraThread("testRenameMtmv", {
                def prefixStr = "qc_rename_mtmv_"
                def tableName = prefixStr + "table1"
                def mvName1 = prefixStr + "mtmv1"
                def mvName2 = prefixStr + "mtmv2"
                def nestedMvName = prefixStr + "nested_mtmv1"

                createTableAndInsert(tableName)
                setSessionVariables()

                def mtmvSql = """
                    SELECT city,sale_date,SUM(amount) AS daily_city_amount FROM ${tableName} GROUP BY city, sale_date;"""
                def nestedMtmvSql = """
                    SELECT city,date_trunc(sale_date, 'MONTH') AS sale_date, SUM(daily_city_amount) AS monthly_city_amount FROM ${mvName1} GROUP BY city, date_trunc(sale_date, 'MONTH');"""
                def selectSql = """
                    SELECT product_id, SUM(amount) AS total_city_amount FROM ${tableName} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY product_id;"""
                def rewrittenSql = """
                    SELECT city, SUM(amount) AS total_city_amount FROM ${tableName} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY city;"""
                def nestedDirectSql = """
                    select city, sum(monthly_city_amount) from ${nestedMvName} group by city;"""
                def nestedRewriteSql = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM ${mvName1} GROUP BY date_trunc(sale_date, 'MONTH');"""
                def mvDirectSql = """
                    select city, avg(daily_city_amount) from ${mvName1} group by city;"""
                def renamedMvDirectSql = """
                    select city, avg(daily_city_amount) from ${mvName2} group by city;"""

                sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName1};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName2};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${nestedMvName};"""
                create_async_mv(dbName, mvName1, mtmvSql)
                create_async_mv(dbName, nestedMvName, nestedMtmvSql)

                assertNoCache selectSql
                assertNoCache rewrittenSql
                assertNoCache nestedRewriteSql
                assertNoCache mvDirectSql
                assertNoCache nestedDirectSql

                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql

                sql """ALTER MATERIALIZED VIEW ${mvName1} rename ${mvName2};"""
                assertHasCache selectSql
                assertNoCache rewrittenSql
                test {
                    sql nestedRewriteSql
                    exception "does not exist"
                }
                test {
                    sql mvDirectSql
                    exception "does not exist"
                }
                assertNoCache renamedMvDirectSql
                assertHasCache nestedDirectSql

                sql """ALTER MATERIALIZED VIEW ${mvName2} rename ${mvName1};"""
                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertNoCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql
            }),

            extraThread("testReplaceMtmv", {
                def prefixStr = "qc_replace_mtmv_"
                def tableName = prefixStr + "table1"
                def mvName1 = prefixStr + "mtmv1"
                def mvName2 = prefixStr + "mtmv2"
                def nestedMvName = prefixStr + "nested_mtmv1"

                createTableAndInsert(tableName)
                setSessionVariables()

                def mtmvSql = """
                    SELECT city,sale_date,SUM(amount) AS daily_city_amount FROM ${tableName} GROUP BY city, sale_date order by 1, 2, 3;"""
                def replacementMtmvSql = """
                    SELECT city,sale_date,count(amount) AS daily_city_amount FROM ${tableName} GROUP BY city, sale_date order by 1, 2, 3;"""
                def nestedMtmvSql = """
                    SELECT city,date_trunc(sale_date, 'MONTH') AS sale_date, SUM(daily_city_amount) AS monthly_city_amount FROM ${mvName1} GROUP BY city, date_trunc(sale_date, 'MONTH') order by 1, 2, 3;"""
                def selectSql = """
                    SELECT product_id, SUM(amount) AS total_city_amount FROM ${tableName} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY product_id order by 1, 2;"""
                def rewrittenSql = """
                    SELECT city, SUM(amount) AS total_city_amount FROM ${tableName} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY city order by 1, 2;"""
                def nestedDirectSql = """
                    select city, sum(monthly_city_amount) from ${nestedMvName} group by city order by 1, 2;"""
                def nestedRewriteSql = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM ${mvName1} GROUP BY date_trunc(sale_date, 'MONTH') order by 1, 2;"""
                def mvDirectSql = """
                    select city, avg(daily_city_amount) from ${mvName1} group by city order by 1, 2;"""
                def replacementMvDirectSql = """
                    select city, avg(daily_city_amount) from ${mvName2} group by city order by 1, 2;"""

                sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName1};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName2};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${nestedMvName};"""
                create_async_mv(dbName, mvName1, mtmvSql)
                create_async_mv(dbName, mvName2, replacementMtmvSql)
                create_async_mv(dbName, nestedMvName, nestedMtmvSql)

                assertNoCache selectSql
                assertNoCache rewrittenSql
                assertNoCache nestedRewriteSql
                assertNoCache mvDirectSql
                assertNoCache nestedDirectSql

                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql

                def selectRes = sql selectSql
                def rewrittenRes = sql rewrittenSql
                def nestedRewriteRes = sql nestedRewriteSql
                def mvDirectRes = sql mvDirectSql
                def nestedDirectRes = sql nestedDirectSql

                sql """ALTER MATERIALIZED VIEW ${mvName1} REPLACE WITH MATERIALIZED VIEW ${mvName2};"""
                assertHasCache selectSql
                assertNoCache rewrittenSql
                assertNoCache nestedRewriteSql
                assertNoCache replacementMvDirectSql
                assertHasCache nestedDirectSql

                sql """ALTER MATERIALIZED VIEW ${mvName2} REPLACE WITH MATERIALIZED VIEW ${mvName1};"""
                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertNoCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql

                judgeRes(selectRes, sql(selectSql))
                judgeRes(rewrittenRes, sql(rewrittenSql))
                judgeRes(nestedRewriteRes, sql(nestedRewriteSql))
                judgeRes(mvDirectRes, sql(mvDirectSql))
                judgeRes(nestedDirectRes, sql(nestedDirectSql))
            }),

            extraThread("testRecreateMtmv", {
                def prefixStr = "qc_recreate_mtmv_"
                def tableName = prefixStr + "table1"
                def mvName1 = prefixStr + "mtmv1"
                def mvName2 = prefixStr + "mtmv2"
                def nestedMvName = prefixStr + "nested_mtmv1"

                createTableAndInsert(tableName)
                setSessionVariables()

                def mtmvSql = """
                    SELECT city,sale_date,SUM(amount) AS daily_city_amount FROM ${tableName} GROUP BY city, sale_date;"""
                def replacementMtmvSql = """
                    SELECT city,sale_date,avg(amount) AS daily_city_amount FROM ${tableName} GROUP BY city, sale_date;"""
                def nestedMtmvSql = """
                    SELECT city,date_trunc(sale_date, 'MONTH') AS sale_date, SUM(daily_city_amount) AS monthly_city_amount FROM ${mvName1} GROUP BY city, date_trunc(sale_date, 'MONTH');"""
                def selectSql = """
                    SELECT product_id, SUM(amount) AS total_city_amount FROM ${tableName} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY product_id;"""
                def rewrittenSql = """
                    SELECT city, SUM(amount) AS total_city_amount FROM ${tableName} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-10-03' GROUP BY city;"""
                def nestedDirectSql = """
                    select city, sum(monthly_city_amount) from ${nestedMvName} group by city;"""
                def nestedRewriteSql = """
                    SELECT date_trunc(sale_date, 'MONTH') AS sale_date,SUM(daily_city_amount) AS monthly_city_amount FROM ${mvName1} GROUP BY date_trunc(sale_date, 'MONTH');"""
                def mvDirectSql = """
                    select city, avg(daily_city_amount) from ${mvName1} group by city;"""

                sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName1};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName2};"""
                sql """DROP MATERIALIZED VIEW IF EXISTS ${nestedMvName};"""
                create_async_mv(dbName, mvName1, mtmvSql)
                create_async_mv(dbName, mvName2, replacementMtmvSql)
                create_async_mv(dbName, nestedMvName, nestedMtmvSql)

                assertNoCache selectSql
                assertNoCache rewrittenSql
                assertNoCache nestedRewriteSql
                assertNoCache mvDirectSql
                assertNoCache nestedDirectSql

                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql

                sql """DROP MATERIALIZED VIEW IF EXISTS ${mvName1}"""
                assertHasCache selectSql
                assertNoCache rewrittenSql
                test {
                    sql nestedRewriteSql
                    exception "does not exist"
                }
                assertHasCache nestedDirectSql

                create_async_mv(dbName, mvName1, mtmvSql)
                assertHasCache selectSql
                assertNoCache rewrittenSql
                assertNoCache nestedRewriteSql
                assertNoCache mvDirectSql
                assertHasCache nestedDirectSql
            })
    ).get()
        })
    }
}
