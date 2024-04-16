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

suite("parse_sql_from_sql_cache") {
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


    sql  "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second' = '10')"

    combineFutures(
        extraThread("testUsePlanCache", {
            createTestTable "test_use_plan_cache"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache"

            // create sql cache
            sql "select * from test_use_plan_cache"

            // use sql cache
            assertHasCache "select * from test_use_plan_cache"
        }),
        extraThread("testAddPartitionAndInsert", {
            createTestTable "test_use_plan_cache2"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache2"
            sql "select * from test_use_plan_cache2"
            assertHasCache "select * from test_use_plan_cache2"

            // add empty partition can use cache
            sql "alter table test_use_plan_cache2 add partition p6 values[('6'),('7'))"
            assertHasCache "select * from test_use_plan_cache2"

            // insert data can not use cache
            sql "insert into test_use_plan_cache2 values(6, 1)"
            assertNoCache "select * from test_use_plan_cache2"
        }),
        extraThread("testDropPartition", {
            createTestTable "test_use_plan_cache3"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache3"
            sql "select * from test_use_plan_cache3"
            assertHasCache "select * from test_use_plan_cache3"

            // drop partition can not use cache
            sql "alter table test_use_plan_cache3 drop partition p5"
            assertNoCache "select * from test_use_plan_cache3"
        }),
        extraThread("testReplacePartition", {
            createTestTable "test_use_plan_cache4"

            sql "alter table test_use_plan_cache4 add temporary partition tp1 values [('1'), ('2'))"

            streamLoad {
                table "test_use_plan_cache4"
                set "temporaryPartitions", "tp1"
                inputIterator([[1, 3], [1, 4]].iterator())
            }
            sql "sync"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache4"
            sql "select * from test_use_plan_cache4"
            assertHasCache "select * from test_use_plan_cache4"

            // replace partition can not use cache
            sql "alter table test_use_plan_cache4 replace partition (p1) with temporary partition(tp1)"
            assertNoCache "select * from test_use_plan_cache4"
        }),
        extraThread("testStreamLoad", {
            createTestTable "test_use_plan_cache5"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache5"
            sql "select * from test_use_plan_cache5"
            assertHasCache "select * from test_use_plan_cache5"

            streamLoad {
                table "test_use_plan_cache5"
                set "partitions", "p1"
                inputIterator([[1, 3], [1, 4]].iterator())
            }

            // stream load can not use cache
            sql "select * from test_use_plan_cache5"
            assertNoCache "select * from test_use_plan_cache5"
        }),
        extraThread("testUpdate",{
            createTestTable "test_use_plan_cache6", uniqueTable=true

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache6"
            sql "select * from test_use_plan_cache6"
            assertHasCache "select * from test_use_plan_cache6"

            sql "update test_use_plan_cache6 set value=3 where id=1"

            // update can not use cache
            sql "select * from test_use_plan_cache6"
            assertNoCache "select * from test_use_plan_cache6"
        }),
        extraThread("testDelete", {
            createTestTable "test_use_plan_cache7"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache7"
            sql "select * from test_use_plan_cache7"
            assertHasCache "select * from test_use_plan_cache7"

            sql "delete from test_use_plan_cache7 where id = 1"

            // delete can not use cache
            sql "select * from test_use_plan_cache7"
            assertNoCache "select * from test_use_plan_cache7"
        }),
        extraThread("testDropTable", {
            createTestTable "test_use_plan_cache8"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache8"
            sql "select * from test_use_plan_cache8"
            assertHasCache "select * from test_use_plan_cache8"

            sql "drop table test_use_plan_cache8"

            // should visible the table has bean deleted
            test {
                sql "select * from test_use_plan_cache8"
                exception "does not exist in database"
            }
        }),
        extraThread("testCreateAndAlterView", {
            createTestTable "test_use_plan_cache9"

            sql "drop view if exists test_use_plan_cache9_view"
            sql "create view test_use_plan_cache9_view as select * from test_use_plan_cache9"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache9_view"
            sql "select * from test_use_plan_cache9_view"
            assertHasCache "select * from test_use_plan_cache9_view"

            // alter view should not use cache
            sql "alter view test_use_plan_cache9_view as select id from test_use_plan_cache9"
            assertNoCache "select * from test_use_plan_cache9_view"
        }),
        extraThread("testDropView", {
            createTestTable "test_use_plan_cache10"

            sql "drop view if exists test_use_plan_cache10_view"
            sql "create view test_use_plan_cache10_view as select * from test_use_plan_cache10"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache10_view"
            sql "select * from test_use_plan_cache10_view"
            assertHasCache "select * from test_use_plan_cache10_view"

            sql "drop view test_use_plan_cache10_view"
            // should visible the view has bean deleted
            test {
                sql "select * from test_use_plan_cache10_view"
                exception "does not exist in database"
            }
        }),
        extraThread("testBaseTableChanged", {
            createTestTable "test_use_plan_cache11"

            sql "drop view if exists test_use_plan_cache11_view"
            sql "create view test_use_plan_cache11_view as select * from test_use_plan_cache11"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache11_view"
            sql "select * from test_use_plan_cache11_view"
            assertHasCache "select * from test_use_plan_cache11_view"

            sql "insert into test_use_plan_cache11 values(1, 3)"

            // base table already changed, can not use cache
            assertNoCache "select * from test_use_plan_cache11_view"
        }),
        extraThread("testNotShareCacheBetweenUsers", {
            sql "drop user if exists test_cache_user1"
            sql "create user test_cache_user1 identified by 'DORIS@2024'"
            def dbName = context.config.getDbNameByFile(context.file)
            sql """GRANT SELECT_PRIV ON *.* TO test_cache_user1"""

            createTestTable "test_use_plan_cache12"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache12"
            sql "select * from test_use_plan_cache12"
            assertHasCache "select * from test_use_plan_cache12"

            sql "sync"


            extraThread("test_cache_user1_thread", {
                connect(user = "test_cache_user1", password="DORIS@2024") {
                    sql "use ${dbName}"
                    sql "set enable_nereids_planner=true"
                    sql "set enable_fallback_to_original_planner=false"
                    sql "set enable_sql_cache=true"

                    assertNoCache "select * from test_use_plan_cache12"
                }
            }).get()
        }),
        extraThread("testAddRowPolicy", {
            def dbName = context.config.getDbNameByFile(context.file)
            sql "set enable_nereids_planner=false"
            try_sql """
                DROP ROW POLICY if exists test_cache_row_policy_2
                ON ${dbName}.test_use_plan_cache13
                FOR test_cache_user2"""
            sql "set enable_nereids_planner=true"

            sql "drop user if exists test_cache_user2"
            sql "create user test_cache_user2 identified by 'DORIS@2024'"
            sql """GRANT SELECT_PRIV ON *.* TO test_cache_user2"""

            createTestTable "test_use_plan_cache13"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "sync"

            extraThread("test_cache_user2_thread", {
                connect(user = "test_cache_user2", password="DORIS@2024") {
                    sql "use ${dbName}"
                    sql "set enable_nereids_planner=true"
                    sql "set enable_fallback_to_original_planner=false"
                    sql "set enable_sql_cache=true"

                    assertNoCache "select * from test_use_plan_cache13"
                    sql "select * from test_use_plan_cache13"
                    assertHasCache "select * from test_use_plan_cache13"
                }
            }).get()

            sql "set enable_nereids_planner=false"
            sql """
                CREATE ROW POLICY test_cache_row_policy_2
                ON ${dbName}.test_use_plan_cache13
                AS RESTRICTIVE TO test_cache_user2
                USING (id = 'concat(id, "**")')"""
            sql "set enable_nereids_planner=true"

            sql "sync"

            // after row policy changed, the cache is invalidate
            extraThread("test_cache_user2_thread2", {
                connect(user = "test_cache_user2", password="DORIS@2024") {
                    sql "use ${dbName}"
                    sql "set enable_nereids_planner=true"
                    sql "set enable_fallback_to_original_planner=false"
                    sql "set enable_sql_cache=true"

                    assertNoCache "select * from test_use_plan_cache13"
                }
            }).get()
        }),
        extraThread("testDropRowPolicy", {
            def dbName = context.config.getDbNameByFile(context.file)
            sql "set enable_nereids_planner=false"
            try_sql """
            DROP ROW POLICY if exists test_cache_row_policy_3
            ON ${dbName}.test_use_plan_cache14
            FOR test_cache_user3"""
            sql "set enable_nereids_planner=true"

            sql "drop user if exists test_cache_user3"
            sql "create user test_cache_user3 identified by 'DORIS@2024'"
            sql """GRANT SELECT_PRIV ON *.* TO test_cache_user3"""

            createTestTable "test_use_plan_cache14"

            sql "set enable_nereids_planner=false"
            sql """
            CREATE ROW POLICY test_cache_row_policy_3
            ON ${dbName}.test_use_plan_cache14
            AS RESTRICTIVE TO test_cache_user3
            USING (id = 'concat(id, "**")')"""
            sql "set enable_nereids_planner=true"

            sql "sync"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            extraThread("test_cache_user3_thread", {
                connect(user = "test_cache_user3", password="DORIS@2024") {
                    sql "use ${dbName}"
                    sql "set enable_nereids_planner=true"
                    sql "set enable_fallback_to_original_planner=false"
                    sql "set enable_sql_cache=true"

                    assertNoCache "select * from test_use_plan_cache14"
                    sql "select * from test_use_plan_cache14"
                    assertHasCache "select * from test_use_plan_cache14"
                }
            }).get()

            sql "set enable_nereids_planner=false"
            try_sql """
            DROP ROW POLICY if exists test_cache_row_policy_3
            ON ${dbName}.test_use_plan_cache14
            FOR test_cache_user3"""
            sql "set enable_nereids_planner=true"

            sql "sync"

            // after row policy changed, the cache is invalidate
            extraThread("test_cache_user3_thread2", {
                connect(user = "test_cache_user3", password="DORIS@2024") {
                    sql "use ${dbName}"
                    sql "set enable_nereids_planner=true"
                    sql "set enable_fallback_to_original_planner=false"
                    sql "set enable_sql_cache=true"

                    assertNoCache "select * from test_use_plan_cache14"
                }
            }).get()
        }),
        extraThread("testRemovePrivilege", {
            def dbName = context.config.getDbNameByFile(context.file)

            createTestTable "test_use_plan_cache15"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "drop user if exists test_cache_user4"
            sql "create user test_cache_user4 identified by 'DORIS@2024'"
            sql "GRANT SELECT_PRIV ON regression_test.* TO test_cache_user4"
            sql "GRANT SELECT_PRIV ON ${dbName}.test_use_plan_cache15 TO test_cache_user4"

            sql "sync"

            extraThread("test_cache_user4_thread", {
                connect(user = "test_cache_user4", password="DORIS@2024") {
                    sql "use ${dbName}"
                    sql "set enable_nereids_planner=true"
                    sql "set enable_fallback_to_original_planner=false"
                    sql "set enable_sql_cache=true"

                    assertNoCache "select * from test_use_plan_cache15"
                    sql "select * from test_use_plan_cache15"
                    assertHasCache "select * from test_use_plan_cache15"
                }
            }).get()

            sql "REVOKE SELECT_PRIV ON ${dbName}.test_use_plan_cache15 FROM test_cache_user4"

            sql "sync"

            // after privileges changed, the cache is invalidate
            extraThread("test_cache_user4_thread2", {
                connect(user = "test_cache_user4", password="DORIS@2024") {
                    sql "set enable_nereids_planner=true"
                    sql "set enable_fallback_to_original_planner=false"
                    sql "set enable_sql_cache=true"

                    test {
                        sql ("select * from ${dbName}.test_use_plan_cache15")
                        exception "Permission denied"
                    }
                }
            }).get()
        }),
        extraThread("testNondeterministic", {
            createTestTable "test_use_plan_cache16"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select random() from test_use_plan_cache16"
            // create sql cache
            sql "select random() from test_use_plan_cache16"
            // can not use sql cache
            assertNoCache "select random() from test_use_plan_cache16"


            assertNoCache "select year(now()) from test_use_plan_cache16"
            sql "select year(now()) from test_use_plan_cache16"
            assertHasCache "select year(now()) from test_use_plan_cache16"


            assertNoCache "select second(now()) from test_use_plan_cache16"
            sql "select second(now()) from test_use_plan_cache16"
            sleep(1000)
            assertNoCache "select second(now()) from test_use_plan_cache16"
        }),
        extraThread("testUserVariable", {
            // make sure if the table has been dropped, the cache should invalidate,
            // so we should retry twice to check
            for (def i in 0..2) {
                createTestTable "test_use_plan_cache17"

                // after partition changed 10s, the sql cache can be used
                sleep(10000)

                sql "set enable_nereids_planner=true"
                sql "set enable_fallback_to_original_planner=false"
                sql "set enable_sql_cache=true"

                sql "set @custom_variable=10"
                assertNoCache "select @custom_variable from test_use_plan_cache17"
                // create sql cache
                sql "select @custom_variable from test_use_plan_cache17"
                // can use sql cache
                assertHasCache "select @custom_variable from test_use_plan_cache17"

                sql "set @custom_variable=20"
                assertNoCache "select @custom_variable from test_use_plan_cache17"
            }
        }),
        extraThread("test_udf", {
            def jarPath = """${context.config.suitePath}/javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
            try_sql("DROP FUNCTION IF EXISTS java_udf_string_test(string, int, int);")
            try_sql("DROP TABLE IF EXISTS test_javaudf_string")

            sql """ DROP TABLE IF EXISTS test_javaudf_string """
            sql """
                    CREATE TABLE IF NOT EXISTS test_javaudf_string (
                        `user_id`     INT         NOT NULL COMMENT "用户id",
                        `char_col`    CHAR        NOT NULL COMMENT "",
                        `varchar_col` VARCHAR(10) NOT NULL COMMENT "",
                        `string_col`  STRING      NOT NULL COMMENT ""
                        )
                        DISTRIBUTED BY HASH(user_id) PROPERTIES("replication_num" = "1");
                    """

            StringBuilder values = new StringBuilder()
            int i = 1
            for (; i < 9; i ++) {
                values.append(" (${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg'),\n")
            }
            values.append("(${i}, '${i}','abcdefg${i}','poiuytre${i}abcdefg')")

            sql "INSERT INTO test_javaudf_string VALUES ${values}"
            sql "sync"

            File path = new File(jarPath)
            if (!path.exists()) {
                throw new IllegalStateException("""${jarPath} doesn't exist! """)
            }

            sql """ CREATE FUNCTION java_udf_string_test(string, int, int) RETURNS string PROPERTIES (
                        "file"="file://${jarPath}",
                        "symbol"="org.apache.doris.udf.StringTest",
                        "type"="JAVA_UDF"
                    ); """

            assertNoCache "SELECT java_udf_string_test(varchar_col, 2, 3) result FROM test_javaudf_string ORDER BY result;"
            sql "SELECT java_udf_string_test(varchar_col, 2, 3) result FROM test_javaudf_string ORDER BY result;"
            assertNoCache "SELECT java_udf_string_test(varchar_col, 2, 3) result FROM test_javaudf_string ORDER BY result;"
        })
    ).get()
}
