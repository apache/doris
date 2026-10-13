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
// Query Cache scope and execution context isolation tests
//
// Coverage:
//   - dry_run_query
//   - cache sharing across multiple FEs
//   - isolation for same-name tables in different DBs
//   - isolation between temporary and ordinary tables with the same name
//
// Focus on whether Query Cache can reuse results that should be shared across different execution contexts,
// FE nodes, and object namespaces, while avoiding incorrect reuse of caches that should be isolated.
// ===================================================================================

import groovy.json.JsonSlurper

import java.util.stream.Collectors

suite("query_cache_scope_isolation_test") {
    def queryProfileApi = { String feEndpoint, String uriPath, Closure checkFunc ->
        httpTest {
            op "get"
            endpoint feEndpoint
            uri uriPath
            printResponse false
            if (context.config.isCloudMode()) {
                basicAuthorization context.config.feCloudHttpUser, context.config.feCloudHttpPassword
            } else {
                basicAuthorization context.config.feHttpUser, context.config.feHttpPassword
            }
            check checkFunc
        }
    }

    def normalizeProfileText = { String profileText ->
        profileText = profileText.replace("&nbsp;", " ")
        profileText = profileText.replace("</br>", "\n")
        profileText = profileText.replace('\u00A0' as char, ' ' as char)
        return profileText.replaceAll(" {2,}", " ")
    }

    def waitProfileTextByTagOnFe = { String feEndpoint, String tag ->
        for (int retry = 0; retry < 30; retry++) {
            def profileData = []
            queryProfileApi(feEndpoint, "/rest/v1/query_profile") { code, body ->
                assertEquals(200, code)
                profileData = new JsonSlurper().parseText(body).data.rows
            }
            for (final def profileItem in profileData) {
                if (profileItem["Sql Statement"].toString().contains(tag)) {
                    def profileText = [text: ""]
                    String profileId = profileItem["Profile ID"].toString()
                    queryProfileApi(feEndpoint, "/rest/v1/query_profile/${profileId}") { code, body ->
                        assertEquals(200, code)
                        profileText.text = normalizeProfileText(new JsonSlurper().parseText(body).data.toString())
                    }
                    return profileText.text
                }
            }
            sleep(1000)
        }
        throw new IllegalStateException("Missing profile with tag: ${tag} on FE ${feEndpoint}")
    }

    def withProfileOnFe = { String feEndpoint, String sqlStr, Closure checkProfile ->
        String tag = UUID.randomUUID().toString()
        try {
            sql "set enable_profile=true"
            sql "/* ${tag} */ ${sqlStr}"
            sleep(10 * 1000)
            def profileString = waitProfileTextByTagOnFe(feEndpoint, tag)
            logger.info("profileString: " + profileString)
            checkProfile.call(profileString)
        } finally {
            sql "set enable_profile=false"
        }
    }

    def assertHasCacheOnFe = { String feEndpoint, String sqlStr ->
        withProfileOnFe(feEndpoint, sqlStr) { String profileString ->
            assertTrue(profileString.contains("HitCache:  1"))
            assertFalse(profileString.contains("HitCache:  0"))
        }
    }

    def assertNoCacheOnFe = { String feEndpoint, String sqlStr ->
        withProfileOnFe(feEndpoint, sqlStr) { String profileString ->
            assertTrue(profileString.contains("HitCache:  0"))
            assertFalse(profileString.contains("HitCache:  1"))
        }
    }

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

    String dbName = context.config.getDbNameByFile(context.file)
    withGlobalLock("cache_last_version_interval_second") {
        setFeConfigTemporary(["cache_last_version_interval_second": "0"], {

    combineFutures(
            extraThread("testDryRunQuery", {
                def tableName = "query_cache_dry_run_query_table"
                createTestTable tableName
                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                sql "set dry_run_query=true"
                assertNoCache sqlStr
                def result = sql sqlStr
                assertHasCache sqlStr
                assertTrue(result.size() == 1)

                sql "set dry_run_query=false"
                assertHasCache sqlStr
                result = sql sqlStr
                assertTrue(result.size() > 1)

                sql "set dry_run_query=true"
                assertHasCache sqlStr
                result = sql sqlStr
                assertTrue(result.size() == 1)
            }),

            extraThread("testMultiFrontends", {
                def tableName = "query_cache_multi_frontends_table"
                def sqlStr = "select id, count(value) from ${tableName} group by id;"
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
                def fe1Http = aliveFrontends[0]["Host"] + ":" + aliveFrontends[0]["HttpPort"]
                def fe2Http = fe1Http
                if (aliveFrontends.size() > 1) {
                    fe2Http = aliveFrontends[1]["Host"] + ":" + aliveFrontends[1]["HttpPort"]
                }

                log.info("fe1: ${fe1}")
                log.info("fe2: ${fe2}")
                log.info("fe1Http: ${fe1Http}")
                log.info("fe2Http: ${fe2Http}")

                connect(context.config.jdbcUser, context.config.jdbcPassword, "jdbc:mysql://${fe1}") {
                    sql "use ${dbName}"
                    createTestTable tableName
                    sql "sync"
                    setSessionVariables()

                    assertNoCacheOnFe fe1Http, sqlStr
                    assertHasCacheOnFe fe1Http, sqlStr
                }

                connect(context.config.jdbcUser, context.config.jdbcPassword, "jdbc:mysql://${fe2}") {
                    sql "use ${dbName}"
                    setSessionVariables()

                    assertHasCacheOnFe fe2Http, sqlStr
                }
            }),

            extraThread("testSameSqlWithDifferentDb", {
                def dbName1 = "query_cache_same_sql_with_different_db1"
                def dbName2 = "query_cache_same_sql_with_different_db2"
                def tableName = "query_cache_same_sql_with_different_db_table"
                setSessionVariables()

                sql "CREATE DATABASE IF NOT EXISTS ${dbName1}"
                sql "DROP TABLE IF EXISTS ${dbName1}.${tableName}"
                sql """
                    CREATE TABLE IF NOT EXISTS ${dbName1}.${tableName} (
                      `k1` date NOT NULL COMMENT "",
                      `k2` int(11) NOT NULL COMMENT ""
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`k1`, `k2`)
                    COMMENT "OLAP"
                    PARTITION BY RANGE(`k1`)
                    (PARTITION p202411 VALUES [('2024-11-01'), ('2024-12-01')))
                    DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
                    PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "in_memory" = "false",
                    "storage_format" = "V2"
                    )
                    """
                sql "CREATE DATABASE IF NOT EXISTS ${dbName2}"
                sql "DROP TABLE IF EXISTS ${dbName2}.${tableName}"
                sql """
                    CREATE TABLE IF NOT EXISTS ${dbName2}.${tableName} (
                      `k1` date NOT NULL COMMENT "",
                      `k2` int(11) NOT NULL COMMENT ""
                    ) ENGINE=OLAP
                    DUPLICATE KEY(`k1`, `k2`)
                    COMMENT "OLAP"
                    PARTITION BY RANGE(`k1`)
                    (PARTITION p202411 VALUES [('2024-11-01'), ('2024-12-01')))
                    DISTRIBUTED BY HASH(`k1`, `k2`) BUCKETS 1
                    PROPERTIES (
                    "replication_allocation" = "tag.location.default: 1",
                    "in_memory" = "false",
                    "storage_format" = "V2"
                    )
                    """

                sql """
                    INSERT INTO ${dbName1}.${tableName} VALUES
                                ("2024-11-29",1),
                                ("2024-11-30",2)
                    """
                sql """
                    INSERT INTO ${dbName2}.${tableName} VALUES
                                ("2024-11-29",3)
                    """

                def sqlStr = "select k1, count(k2) from ${tableName} group by k1;"
                sql """use ${dbName1}"""
                assertNoCache sqlStr
                assertHasCache sqlStr
                def result1 = sql sqlStr
                assertTrue(result1.size() == 2)

                sql """use ${dbName2}"""
                assertNoCache sqlStr
                assertHasCache sqlStr
                def result2 = sql sqlStr
                assertTrue(result2.size() == 1)
            }),

            extraThread("testTemporaryTable", {
                def tableName = "query_cache_temporary_table_table"
                createTestTable tableName
                setSessionVariables()

                def sqlStr = "select id, count(value) from ${tableName} group by id;"
                assertNoCache sqlStr
                assertHasCache sqlStr
                def result = sql sqlStr
                assertTrue(result.size() == 5)

                connect(context.config.jdbcUser, context.config.jdbcPassword, context.jdbcUrl) {
                    setSessionVariables()
                    sql "create temporary table ${tableName}(id int, value int) properties('replication_num'='1')"
                    sql """insert into ${tableName} values  (1, 1), (1, 2);"""
                    assertNoCache sqlStr
                    assertHasCache sqlStr
                    assertEquals(1, (sql sqlStr).size())
                    assertHasCache sqlStr
                    assertEquals(1, (sql sqlStr).size())
                    assertHasCache sqlStr
                }

                assertHasCache sqlStr
                result = sql sqlStr
                assertTrue(result.size() == 5)
            })
    ).get()
        })
    }
}
