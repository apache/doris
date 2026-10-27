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
// Query Cache table schema and table lifecycle change tests
//
// Coverage:
//   - drop table / rename table / replace table / truncate table
//   - add column / drop column / modify column
//
// Focus on cache invalidation after object metadata changes, and whether schema changes that do not
// affect current query results
// can still reuse existing cache.
// ===================================================================================

suite("query_cache_table_schema_change_test") {
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

    def noQueryCache = { String sqlStr ->
        String tag = UUID.randomUUID().toString()
        profile(tag) {
            run {
                sql "/* ${tag} */ ${sqlStr}"
                sleep(10 * 1000)
            }

            check { profileString, exception ->
                logger.info("profileString: " + profileString)
                assertFalse(profileString.contains("HitCache:  0"))
                assertFalse(profileString.contains("HitCache:  1"))
            }
        }
    }

    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {

    combineFutures(
            extraThread("testDropTable", {
                def tableName = "query_cache_drop_table_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id order by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql """drop table ${tableName}"""

                test {
                    sql sqlStr
                    exception "does not exist"
                }

                sql """RECOVER TABLE ${tableName};"""
                assertHasCache sqlStr

                sql """drop table ${tableName}"""
                sql """create table ${tableName}
                    (
                        id int,
                        value int
                    )
                    partition by range(id)
                    (
                        partition p1 values[('1'), ('2')),
                        partition p2 values[('2'), ('3')),
                        partition p3 values[('3'), ('4')),
                        partition p4 values[('4'), ('5')),
                        partition p5 values[('5'), ('6'))
                    )
                    distributed by hash(id)
                    properties(
                        'replication_num'='1'
                    );"""
                sql """insert into ${tableName} values (1, 1), (1, 2);"""
                assertNoCache sqlStr
            }),

            extraThread("testRenameTable", {
                def tableName = "query_cache_rename_table_table"
                def newTableName = "new_query_cache_rename_table_table"
                sql """drop table if exists ${newTableName}"""
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id order by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql """ALTER TABLE ${tableName} RENAME ${newTableName};"""

                test {
                    sql sqlStr
                    exception "does not exist"
                }
                sql """ALTER TABLE ${newTableName} RENAME ${tableName};"""
                assertHasCache sqlStr
            }),

            extraThread("testReplaceTable", {
                def tableName = "query_cache_replace_table_table1"
                def replacementTableName = "query_cache_replace_table_table2"
                createTestTable tableName
                createTestTable replacementTableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id order by id;"
                def replacementSqlStr = "select id, count(value) from ${replacementTableName} group by id order by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                assertNoCache replacementSqlStr
                assertHasCache replacementSqlStr

                sql """ALTER TABLE ${tableName} REPLACE WITH TABLE ${replacementTableName};"""
                assertNoCache sqlStr
                assertNoCache replacementSqlStr
            }),

            extraThread("testTruncateTable", {
                def tableName = "query_cache_truncate_table_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id order by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql """truncate table ${tableName}"""
                noQueryCache sqlStr
            }),

            extraThread("testAddColumn", {
                def tableName = "query_cache_add_column_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id order by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql """alter table ${tableName} add column bbb int default '0'"""
                assertHasCache sqlStr
            }),

            extraThread("testDropColumn", {
                def tableName = "query_cache_drop_column_table"
                createTestTable tableName

                setSessionVariables()

                def unaffectedSql = "select id, count(id) from ${tableName} group by id order by id;"
                def droppedColumnSql = "select id, count(value) from ${tableName} group by id order by id;"
                assertNoCache unaffectedSql
                assertHasCache unaffectedSql

                assertNoCache droppedColumnSql
                assertHasCache droppedColumnSql

                sql """alter table ${tableName} drop column value"""
                waitForSchemaChangeDone {
                    sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tableName}' ORDER BY createtime DESC LIMIT 1 """
                    time 600
                }

                assertNoCache unaffectedSql
                test {
                    sql droppedColumnSql
                    exception "Unknown column"
                }
            }),

            extraThread("testModifyColumn", {
                def tableName = "query_cache_modify_column_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql """alter table ${tableName} modify column value bigint"""
                waitForSchemaChangeDone {
                    sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tableName}' ORDER BY createtime DESC LIMIT 1 """
                    time 600
                }

                assertNoCache sqlStr
                assertHasCache sqlStr
            })
    ).get()
        })
    }
}
