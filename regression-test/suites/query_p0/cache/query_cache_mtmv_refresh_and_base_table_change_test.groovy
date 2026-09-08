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
// Query Cache tests for MTMV refresh and base-table changes
//
// Coverage:
//   - pause / resume MTMV job
//   - refresh auto / refresh complete
//   - inserting new data into the base table
//   - insert overwrite
//
// Each scenario verifies three query paths at the same time:
//   - Direct base-table query without MTMV rewrite
//   - Direct base-table query rewritten to hit MTMV
//   - Query path with nested MTMV
// ===================================================================================

suite("query_cache_mtmv_refresh_and_base_table_change_test") {
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

    def assertPartHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                assertTrue(profileString.contains("HitCache:  1"))
                assertTrue(profileString.contains("HitCache:  0"))
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

    def curCreateAsyncPartitionMv = { def db, def mvName, def mvSql, def partitionCol ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${db}.${mvName}"""
        sql """
                CREATE MATERIALIZED VIEW ${db}.${mvName}
                BUILD IMMEDIATE REFRESH auto ON MANUAL
                -- PARTITION BY ${partitionCol}
                DISTRIBUTED BY RANDOM BUCKETS 2
                PROPERTIES ('replication_num' = '1')
                AS ${mvSql}
                """
        def jobName = getJobName(db, mvName)
        waitingMTMVTaskFinished(jobName)
        sql "analyze table ${db}.${mvName} with sync;"
        sql """sync;"""
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
            extraThread("testPauseResumeMtmv", {
                def prefixStr = "qc_pause_resume_mtmv_"
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
                curCreateAsyncPartitionMv(dbName, mvName1, mtmvSql, "")
                curCreateAsyncPartitionMv(dbName, mvName2, replacementMtmvSql, "")
                curCreateAsyncPartitionMv(dbName, nestedMvName, nestedMtmvSql, "")

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

                sql """PAUSE MATERIALIZED VIEW JOB ON ${mvName1};"""
                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql

                sql """RESUME MATERIALIZED VIEW JOB ON ${mvName1};"""
                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql
            }),

            extraThread("testRefreshAutoMtmv", {
                def prefixStr = "qc_refresh_auto_mtmv_"
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
                curCreateAsyncPartitionMv(dbName, mvName1, mtmvSql, "")
                curCreateAsyncPartitionMv(dbName, mvName2, replacementMtmvSql, "")
                curCreateAsyncPartitionMv(dbName, nestedMvName, nestedMtmvSql, "")

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

                sql "REFRESH MATERIALIZED VIEW ${mvName1} AUTO;"
                waitingMTMVTaskFinishedByMvName(mvName1)

                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql
            }),

            extraThread("testRefreshCompleteMtmv", {
                def prefixStr = "qc_refresh_complete_mtmv_"
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
                curCreateAsyncPartitionMv(dbName, mvName1, mtmvSql, "")
                curCreateAsyncPartitionMv(dbName, mvName2, replacementMtmvSql, "")
                curCreateAsyncPartitionMv(dbName, nestedMvName, nestedMtmvSql, "")

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

                sql "REFRESH MATERIALIZED VIEW ${mvName1} complete;"
                waitingMTMVTaskFinishedByMvName(mvName1)

                assertHasCache selectSql
                assertNoCache rewrittenSql
                assertNoCache nestedRewriteSql
                assertNoCache mvDirectSql
                assertHasCache nestedDirectSql

                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql
            }),

            extraThread("testBaseInsertDataMtmv", {
                def prefixStr = "qc_base_insert_data_mtmv_"
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
                    SELECT product_id, SUM(amount) AS total_city_amount FROM ${tableName} WHERE sale_date >= '2025-10-01' AND sale_date <= '2025-12-31' GROUP BY product_id;"""
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

                sql "alter table ${tableName} add partition p1 values[('2025-11-01'), ('2025-12-01'))"
                assertHasCache selectSql
                assertHasCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql

                sql "insert into ${tableName} values(111, 'Beijing', '2025-11-01', 500.00)"
                assertPartHasCache selectSql
                assertNoCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql
            }),

            extraThread("testInsertOverwriteMtmv", {
                def prefixStr = "qc_insert_overwrite_mtmv_"
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
                curCreateAsyncPartitionMv(dbName, mvName1, mtmvSql, "")
                curCreateAsyncPartitionMv(dbName, mvName2, replacementMtmvSql, "")
                curCreateAsyncPartitionMv(dbName, nestedMvName, nestedMtmvSql, "")

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

                sql "INSERT OVERWRITE table ${tableName} PARTITION(p20251001) VALUES (101, 'Beijing', '2025-10-01', 500.00);"
                assertPartHasCache selectSql
                assertNoCache rewrittenSql
                assertHasCache nestedRewriteSql
                assertHasCache mvDirectSql
                assertHasCache nestedDirectSql

                sql "REFRESH MATERIALIZED VIEW ${mvName1} AUTO;"
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
