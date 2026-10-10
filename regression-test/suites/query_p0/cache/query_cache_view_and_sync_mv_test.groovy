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
// Query Cache view and synchronous materialized view tests
//
// Coverage:
//   - create view / alter view / drop view
//   - view base-table data changes
//   - synchronous MV create / rename / drop
//
// Focus on how logical view definition changes, base-table data changes, and synchronous MV metadata
// changes affect cache hits and invalidation.
// ===================================================================================

suite("query_cache_view_and_sync_mv_test") {
    def setSessionVariables = {
        sql "set enable_nereids_planner=true"
        sql "set enable_fallback_to_original_planner=false"
        sql "set enable_sql_cache=false"
        sql "set enable_query_cache=true"
    }

    def assertHasCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
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
                logger.info("profileString: " + profileString)
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
                logger.info("profileString: " + profileString)
                assertTrue(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    String dbName = context.config.getDbNameByFile(context.file)
    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {

    combineFutures(
            extraThread("testCreateAndAlterView", {
                def tableName = "query_cache_create_and_alter_view_table"
                def viewName = "query_cache_create_and_alter_view_table_view"

                createTestTable tableName
                sql """drop view if exists ${viewName}"""
                sql """create view ${viewName} (k1, k2) as select id as k1, count(value) as k2 from ${tableName} group by k1;"""

                setSessionVariables()

                def sqlStr = "select k1, sum(k2) from ${viewName} group by k1;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql "alter view ${viewName} as select id as k1, avg(value) as k2 from ${tableName} group by k1;"
                assertNoCache sqlStr
            }),

            extraThread("testCreateAndDropView", {
                def tableName = "query_cache_create_and_drop_view_table"
                def viewName = "query_cache_create_and_drop_view_table_view"
                createTestTable tableName
                sql """drop view if exists ${viewName}"""
                sql """create view ${viewName} (k1, k2) as select id as k1, count(value) as k2 from ${tableName} group by k1;"""

                setSessionVariables()

                def sqlStr = "select k1, sum(k2) from ${viewName} group by k1;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql "drop view ${viewName}"
                test {
                    sql sqlStr
                    exception "does not exist"
                }
            }),

            extraThread("testViewWithDataChange", {
                def tableName = "query_cache_view_with_data_change_table"
                def viewName = "query_cache_view_with_data_change_table_view"
                createTestTable tableName
                sql """drop view if exists ${viewName}"""
                sql """create view ${viewName} (k1, k2) as select id as k1, count(value) as k2 from ${tableName} group by k1;"""

                setSessionVariables()

                def sqlStr = "select k1, sum(k2) from ${viewName} group by k1;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql "insert into ${tableName} values(1, 3)"
                assertPartHasCache sqlStr
            }),

            extraThread("testChangeWithMv", {
                def tableName = "query_cache_change_with_mv_table"
                def mvName1 = "query_cache_change_with_mv_table_mv1"
                def mvName2 = "query_cache_change_with_mv_table_mv2"
                createTestTable tableName

                setSessionVariables()

                def mvSql = """select id as col1, sum(value) as col2 from ${tableName} group by id;"""
                def mvSelectSql = """select col1, count(col2) from ${tableName} index ${mvName1} group by col1"""
                def renamedMvSelectSql = """select col1, count(col2) from ${tableName} index ${mvName2} group by col1"""

                create_sync_mv(dbName, tableName, mvName1, mvSql)
                assertNoCache mvSql
                assertNoCache mvSelectSql
                assertHasCache mvSelectSql

                sql """alter table ${tableName} add partition p6 values[('6'),('7'))"""
                assertHasCache mvSql
                assertHasCache mvSelectSql

                sql "insert into ${tableName} values(6, 1)"
                assertPartHasCache mvSql
                assertHasCache mvSql

                assertPartHasCache mvSelectSql
                assertHasCache mvSelectSql

                sql """ALTER TABLE ${tableName} RENAME ROLLUP ${mvName1} ${mvName2};"""
                assertNoCache mvSql
                assertNoCache renamedMvSelectSql
                assertHasCache renamedMvSelectSql

                sql """drop materialized view ${mvName2} on ${tableName};"""
                assertNoCache mvSql
                test {
                    sql renamedMvSelectSql
                    exception """doesn't have materialized view"""
                }
            })
    ).get()
        })
    }
}
