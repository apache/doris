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

import java.util.stream.Collectors

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

            // NOTE: in cloud mode, add empty partition can not use cache, because the table version already update,
            //       but in native mode, add empty partition can use cache
            sql "alter table test_use_plan_cache2 add partition p6 values[('6'),('7'))"
            if (isCloudMode()) {
                assertNoCache "select * from test_use_plan_cache2"
            } else {
                assertHasCache "select * from test_use_plan_cache2"
            }

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
            //cloud-mode
            if (isCloudMode()) {
                def clusters = sql " SHOW CLUSTERS; "
                assertTrue(!clusters.isEmpty())
                def validCluster = clusters[0][0]
                sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO test_cache_user1"""
            }

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
            try_sql """
                DROP ROW POLICY if exists test_cache_row_policy_2
                ON ${dbName}.test_use_plan_cache13
                FOR test_cache_user2"""

            sql "drop user if exists test_cache_user2"
            sql "create user test_cache_user2 identified by 'DORIS@2024'"
            sql """GRANT SELECT_PRIV ON *.* TO test_cache_user2"""
            //cloud-mode
            if (isCloudMode()) {
                def clusters = sql " SHOW CLUSTERS; "
                assertTrue(!clusters.isEmpty())
                def validCluster = clusters[0][0]
                sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO test_cache_user2"""
            }

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

            sql """
                CREATE ROW POLICY test_cache_row_policy_2
                ON ${dbName}.test_use_plan_cache13
                AS RESTRICTIVE TO test_cache_user2
                USING (id = 4)"""

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
            try_sql """
            DROP ROW POLICY if exists test_cache_row_policy_3
            ON ${dbName}.test_use_plan_cache14
            FOR test_cache_user3"""

            sql "drop user if exists test_cache_user3"
            sql "create user test_cache_user3 identified by 'DORIS@2024'"
            sql """GRANT SELECT_PRIV ON *.* TO test_cache_user3"""
            //cloud-mode
            if (isCloudMode()) {
                def clusters = sql " SHOW CLUSTERS; "
                assertTrue(!clusters.isEmpty())
                def validCluster = clusters[0][0]
                sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO test_cache_user3"""
            }

            createTestTable "test_use_plan_cache14"

            sql """
            CREATE ROW POLICY test_cache_row_policy_3
            ON ${dbName}.test_use_plan_cache14
            AS RESTRICTIVE TO test_cache_user3
            USING (id = 4)"""

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

            try_sql """
            DROP ROW POLICY if exists test_cache_row_policy_3
            ON ${dbName}.test_use_plan_cache14
            FOR test_cache_user3"""

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
            //cloud-mode
            if (isCloudMode()) {
                def clusters = sql " SHOW CLUSTERS; "
                assertTrue(!clusters.isEmpty())
                def validCluster = clusters[0][0]
                sql """GRANT USAGE_PRIV ON CLUSTER ${validCluster} TO test_cache_user4"""
            }

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
                assertNoCache "select @custom_variable from test_use_plan_cache17 where id = 1 and value = 1"
                // create sql cache
                sql "select @custom_variable from test_use_plan_cache17 where id = 1 and value = 1"
                // can use sql cache
                assertHasCache "select @custom_variable from test_use_plan_cache17 where id = 1 and value = 1"

                sql "set @custom_variable=20"
                assertNoCache "select @custom_variable from test_use_plan_cache17 where id = 1 and value = 1"

                def result2 = sql "select @custom_variable from test_use_plan_cache17 where id = 1 and value = 1"
                assertHasCache "select @custom_variable from test_use_plan_cache17 where id = 1 and value = 1"
                assertTrue(result2.size() == 1 && result2[0][0].toString().toInteger() == 20)

                // we can switch to origin value and reuse origin cache
                sql "set @custom_variable=10"
                assertHasCache "select @custom_variable from test_use_plan_cache17 where id = 1 and value = 1"
                def result1 = sql "select @custom_variable from test_use_plan_cache17 where id = 1 and value = 1"
                assertTrue(result1.size() == 1 && result1[0][0].toString().toInteger() == 10)


                sql "set @custom_variable2=1"
                assertNoCache "select * from test_use_plan_cache17 where id = @custom_variable2 and value = 1"
                def res = sql "select * from test_use_plan_cache17 where id = @custom_variable2 and value = 1"
                assertTrue(res[0][0] == 1)
                assertHasCache "select * from test_use_plan_cache17 where id = @custom_variable2 and value = 1"

                sql "set @custom_variable2=2"
                assertNoCache "select* from test_use_plan_cache17 where id = @custom_variable2 and value = 1"
                // should not invalidate cache with @custom_variable2=1
                res = sql "select * from test_use_plan_cache17 where id = @custom_variable2 and value = 1"
                assertTrue(res[0][0] == 2)
                assertHasCache "select * from test_use_plan_cache17 where id = @custom_variable2 and value = 1"

                sql "set @custom_variable2=1"
                // should reuse cache
                assertHasCache "select * from test_use_plan_cache17 where id = @custom_variable2 and value = 1"
                res = sql "select * from test_use_plan_cache17 where id = @custom_variable2 and value = 1"
                assertTrue(res[0][0] == 1)
            }
        }),
        extraThread("test_udf", {
            def jarPath = """${context.config.suitePath}/javaudf_p0/jars/java-udf-case-jar-with-dependencies.jar"""
            scp_udf_file_to_all_be(jarPath)
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
        }),
        extraThread("testMultiFrontends", {
            def aliveFrontends = sql_return_maparray("show frontends")
                    .stream()
                    .filter { it["Alive"].toString().equalsIgnoreCase("true") }
                    .collect(Collectors.toList())

            if (aliveFrontends.size() <= 1) {
                return
            }

            def fe1 = aliveFrontends[0]["Host"] + ":" + aliveFrontends[0]["QueryPort"]
            def fe2 = fe1
            if (aliveFrontends.size() > 1) {
                fe2 = aliveFrontends[1]["Host"] + ":" + aliveFrontends[1]["QueryPort"]
            }

            log.info("fe1: ${fe1}")
            log.info("fe2: ${fe2}")

            def dbName = context.config.getDbNameByFile(context.file)

            log.info("connect to fe: ${fe1}")
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = "jdbc:mysql://${fe1}") {
                sql  "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second' = '10')"

                sql "use ${dbName}"

                createTestTable "test_use_plan_cache18"

                sql "sync"

                // after partition changed 10s, the sql cache can be used
                sleep(10000)

                sql "set enable_nereids_planner=true"
                sql "set enable_fallback_to_original_planner=false"
                sql "set enable_sql_cache=true"

                assertNoCache "select * from test_use_plan_cache18"
                sql "select * from test_use_plan_cache18"
                assertHasCache "select * from test_use_plan_cache18"
            }

            log.info("connect to fe: ${fe2}")
            connect(user = context.config.jdbcUser, password = context.config.jdbcPassword, url = "jdbc:mysql://${fe2}") {
                sql  "ADMIN SET FRONTEND CONFIG ('cache_last_version_interval_second' = '10')"

                sql "use ${dbName}"
                sql "set enable_nereids_planner=true"
                sql "set enable_fallback_to_original_planner=false"
                sql "set enable_sql_cache=true"

                assertNoCache "select * from test_use_plan_cache18"
                sql "select * from test_use_plan_cache18"
                assertHasCache "select * from test_use_plan_cache18"
            }
        }),
        extraThread("test_dry_run_query", {
            createTestTable "test_use_plan_cache19"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            sql "set dry_run_query=true"
            assertNoCache "select * from test_use_plan_cache19 order by 1, 2"
            def result1 = sql "select * from test_use_plan_cache19 order by 1, 2"
            assertTrue(result1.size() == 1)
            assertNoCache "select * from test_use_plan_cache19 order by 1, 2"

            sql "set dry_run_query=false"
            assertNoCache "select * from test_use_plan_cache19 order by 1, 2"
            def result2 = sql "select * from test_use_plan_cache19 order by 1, 2"
            assertTrue(result2.size() > 1)
            assertHasCache "select * from test_use_plan_cache19 order by 1, 2"

            sql "set dry_run_query=true"
            assertNoCache "select * from test_use_plan_cache19 order by 1, 2"
            def result3 = sql "select * from test_use_plan_cache19 order by 1, 2"
            assertTrue(result3.size() == 1)
            assertNoCache "select * from test_use_plan_cache19 order by 1, 2"
        }),
        extraThread("test_sql_cache_in_fe", {
            createTestTable "test_use_plan_cache20"

            sql "alter table test_use_plan_cache20 add partition p6 values[('999'), ('1000'))"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            int randomInt = Math.random() * 2000000000

            assertNoCache "select * from (select $randomInt as id)a"
            def result1 = sql "select * from (select $randomInt as id)a"
            assertTrue(result1.size() == 1)

            assertHasCache "select * from (select $randomInt as id)a"
            def result2 = sql "select * from (select $randomInt as id)a"
            assertTrue(result2.size() == 1)

            assertNoCache "select * from test_use_plan_cache20 limit 0"
            def result3 = sql "select * from test_use_plan_cache20 limit 0"
            assertTrue(result3.isEmpty())

            assertHasCache "select * from test_use_plan_cache20 limit 0"
            def result4 = sql "select * from test_use_plan_cache20 limit 0"
            assertTrue(result4.isEmpty())

            assertNoCache "select * from test_use_plan_cache20 where id=999"
            def result5 = sql "select * from test_use_plan_cache20 where id=999"
            assertTrue(result5.isEmpty())
            assertHasCache "select * from test_use_plan_cache20 where id=999"
            def result6 = sql "select * from test_use_plan_cache20 where id=999"
            assertTrue(result6.isEmpty())
        }),
        extraThread("test_truncate_partition", {
            sql "drop table if exists test_use_plan_cache21"
            sql """create table test_use_plan_cache21 (
                        id int,
                        dt int
                       )
                       partition by range(dt)
                       (
                        partition dt1 values [('1'), ('2')),
                        partition dt2 values [('2'), ('3'))
                       )
                       distributed by hash(id)
                       properties('replication_num'='1')"""

            sql "insert into test_use_plan_cache21 values('2', '2')"
            sleep(100)
            sql "insert into test_use_plan_cache21 values('1', '1')"

            // after partition changed 10s, the sql cache can be used
            sleep(10000)

            sql "set enable_nereids_planner=true"
            sql "set enable_fallback_to_original_planner=false"
            sql "set enable_sql_cache=true"

            assertNoCache "select * from test_use_plan_cache21"
            def result1 = sql "select * from test_use_plan_cache21"
            assertTrue(result1.size() == 2)
            assertHasCache "select * from test_use_plan_cache21"

            sql "truncate table test_use_plan_cache21 partition dt2"
            assertNoCache "select * from test_use_plan_cache21"
            def result2 = sql "select * from test_use_plan_cache21"
            assertTrue(result2.size() == 1)
        })
    ).get()
}
