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
// Query Cache partition and data change tests
//
// Coverage:
//   - add partition + insert / insert overwrite
//   - drop partition / replace partition / rename partition
//   - stream load / update / delete
//
// This combines overlapping scenarios from the original query_cache_with_table_change and
// query_cache_isolation_and_invalidation_test, keeping a single more focused set of
// partition/data change validations.
// ===================================================================================

suite("query_cache_partition_and_data_change_test") {
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

    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {

    combineFutures(
            extraThread("testAddPartitionAndInsert", {
                def tableName = "query_cache_add_partition_and_insert_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql """alter table ${tableName} add partition p6 values[('6'),('7'))"""
                assertHasCache sqlStr

                sql "insert into ${tableName} values(6, 1)"
                assertPartHasCache sqlStr
            }),

            extraThread("testAddPartitionAndInsertOverwrite", {
                def tableName = "query_cache_add_partition_and_insert_overwrite_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql "INSERT OVERWRITE table ${tableName} PARTITION(p5) VALUES (5, 6);"
                assertPartHasCache sqlStr
                assertHasCache sqlStr

                sql "alter table ${tableName} add partition p6 values[('6'),('7'))"
                assertHasCache sqlStr

                sql "INSERT OVERWRITE table ${tableName} PARTITION(p6) VALUES (6, 6);"
                assertPartHasCache sqlStr
            }),

            extraThread("testDropPartition", {
                def tableName = "query_cache_drop_partition_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} where id = 5 group by id order by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr
                def resultBeforeDrop = sql sqlStr
                assertEquals(1, resultBeforeDrop.size())

                sql """alter table ${tableName} add partition p6 values[('6'),('7'))"""
                assertHasCache sqlStr

                sql """alter table ${tableName} drop partition p6"""
                assertHasCache sqlStr

                sql """alter table ${tableName} drop partition p5"""
                def resultAfterDrop = sql sqlStr
                assertEquals(0, resultAfterDrop.size())
            }),

            extraThread("testReplacePartition", {
                def tableName = "query_cache_replace_partition_table"
                createTestTable tableName

                sql "alter table ${tableName} add temporary partition tp1 values [('1'), ('2'))"
                sql """INSERT INTO ${tableName} TEMPORARY PARTITION(tp1) values (1, 3), (1, 4)"""
                sql "sync"

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql """alter table ${tableName} replace partition (p1) with temporary partition(tp1)"""
                assertPartHasCache sqlStr
            }),

            extraThread("testRenamePartition", {
                def tableName = "query_cache_rename_partition_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql """ALTER TABLE ${tableName} RENAME PARTITION p1 p6;"""
                assertHasCache sqlStr
            }),

            extraThread("testStreamLoad", {
                def tableName = "query_cache_stream_load_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                streamLoad {
                    table tableName
                    set "partitions", "p1"
                    inputIterator([[1, 3], [1, 4]].iterator())
                }
                sql "sync"

                assertPartHasCache sqlStr
            }),

            extraThread("testUpdateData", {
                def tableName = "query_cache_update_data_table"
                createTestTable(tableName, true)

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql "update ${tableName} set value=3 where id=1"
                assertPartHasCache sqlStr
            }),

            extraThread("testDeleteData", {
                def tableName = "query_cache_delete_data_table"
                createTestTable tableName

                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr

                sql "delete from ${tableName} where id = 1"
                assertPartHasCache sqlStr
            })
    ).get()
        })
    }
}
