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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_spm_cloud_docker", "docker,multi_cluster") {
    def options = new ClusterOptions(cloudMode: true, feNum: 2, beNum: 1, msNum: 1)
    options.feConfigs += ['enable_debug_points=true', 'cloud_cluster_check_interval_second=1']

    docker(options) {
        def waitUntil = { String description, Closure condition ->
            for (int attempt = 0; attempt < 120; attempt++) {
                if (condition()) {
                    return
                }
                sleep(1000)
            }
            throw new IllegalStateException("Timed out: " + description)
        }
        def tablesReady = {
            try {
                def tables = sql("SHOW TABLES FROM __internal_schema LIKE 'spm_baselines%'")
                if (tables.size() != 3) {
                    return false
                }
                sql "SHOW BASELINE PLANS WHERE scope = 'GLOBAL'"
                return true
            } catch (Exception e) {
                logger.info("Waiting for SPM initialization: " + e.message)
                return false
            }
        }
        waitUntil("SPM tables on a fresh Cloud instance", tablesReady)
        String database = sql("SELECT DATABASE()")[0][0].toString()
        String etl = sql_return_maparray("SHOW CLUSTERS").find { it.is_current == "TRUE" }.cluster
        String bi = "spm_cloud_bi"
        cluster.addBackend(1, bi)
        sql "USE @${etl}"
        sql "SET GLOBAL spm_baseline_refresh_interval_seconds = 1"
        sql "SET GLOBAL enable_audit_plugin = true"

        String createTable = """CREATE TABLE spm_cloud_t(k INT)
            DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES("replication_num"="1")"""
        sql "DROP TABLE IF EXISTS spm_cloud_t"
        sql createTable
        sql "INSERT INTO spm_cloud_t VALUES (1), (2)"
        String bind = "SELECT k FROM spm_cloud_t WHERE k >= 1 ORDER BY k"
        String variant = "SELECT k FROM spm_cloud_t WHERE k >= 2 ORDER BY k"
        def configureSession = { String group ->
            sql "USE ${database}"
            sql "USE @${group}"
            sql "SET enable_spm_rewrite = true"
            sql "SET enable_spm_fallback = false"
            sql "SET enable_sql_cache = false"
            sql "SET force_forward_all_queries = false"
        }
        def hit = { long id, String query ->
            sql("EXPLAIN ${query}").toString().contains("SPM baseline hit: id=${id},")
        }
        def createBaseline = { String scope ->
            sql('CREATE ' + scope + ' BASELINE PLAN "' + bind + '"')[0][0] as Long
        }

        // SESSION baselines stay on their original connection, including queries forwarded
        // to the master, and replay under the caller's new compute group.
        connectWithDockerCluster(cluster, true) {
            configureSession(etl)
            long sessionId = createBaseline("SESSION")
            try {
                assertTrue(hit(sessionId, bind))
                assertEquals(1, sql("SHOW BASELINE PLANS WHERE scope = 'SESSION'").size())
                connectWithDockerCluster(cluster, false) {
                    configureSession(etl)
                    assertTrue(sql("SHOW BASELINE PLANS WHERE id = ${sessionId}").isEmpty())
                    assertTrue(!hit(sessionId, bind))
                }
                sql "USE @${bi}"
                assertTrue(hit(sessionId, variant))
                sql "SET force_forward_all_queries = true"
                assertTrue(hit(sessionId, variant))
                assertEquals([[2]], sql(variant))
                sql "SET force_forward_all_queries = false"
                sql "ALTER BASELINE PLAN ${sessionId} DISABLE"
                assertTrue(!hit(sessionId, bind))
                sql "ALTER BASELINE PLAN ${sessionId} ENABLE"
                assertTrue(hit(sessionId, bind))
            } finally {
                sql "SET force_forward_all_queries = false"
                sql "DROP BASELINE PLAN IF EXISTS ${sessionId}"
            }
            assertTrue(!hit(sessionId, bind))
        }
        // The actual data query ran in BI, not merely on a session carrying the BI name.
        waitUntil("forwarded SESSION query executed in BI", {
            sql "CALL flush_audit_log()"
            (sql("""SELECT COUNT(*) FROM __internal_schema.audit_log
                WHERE compute_group = '${bi}' AND stmt LIKE '${variant}%'""")[0][0] as long) > 0
        })

        long globalId = 0
        connectWithDockerCluster(cluster, false) {
            configureSession(etl)
            globalId = createBaseline("GLOBAL")
            assertTrue(hit(globalId, bind))
        }
        connectWithDockerCluster(cluster, true) {
            configureSession(bi)
            waitUntil("GLOBAL baseline published on follower", { hit(globalId, variant) })
            assertEquals([[2]], sql(variant))
            sql "ALTER BASELINE PLAN ${globalId} DISABLE"
            assertTrue(!hit(globalId, variant))
        }
        connectWithDockerCluster(cluster, false) {
            configureSession(etl)
            waitUntil("disabled GLOBAL baseline no longer matches on creator", { !hit(globalId, bind) })
        }
        connectWithDockerCluster(cluster, true) {
            configureSession(bi)
            sql "ALTER BASELINE PLAN ${globalId} ENABLE"
            assertTrue(hit(globalId, variant))
            sql "INSERT INTO spm_cloud_t VALUES (3)"
            assertTrue(hit(globalId, variant))
            assertEquals([[2], [3]], sql(variant))
            sql "DROP BASELINE PLAN ${globalId}"
            assertTrue(!hit(globalId, variant))
        }
        connectWithDockerCluster(cluster, false) {
            configureSession(etl)
            waitUntil("dropped GLOBAL baseline no longer matches on creator", { !hit(globalId, bind) })
        }

        // The insert is committed, but one confirmation is deliberately hidden. The retry
        // must adopt the same durable id instead of allocating a second baseline.
        String point = "SPM.BaselineManager.confirmInsertVisible.unreadable"
        connectWithDockerCluster(cluster, true) {
            configureSession(bi)
            GetDebugPoint().enableDebugPointForAllFEs(point, [execute: 1])
            try {
                test {
                    sql('CREATE GLOBAL BASELINE PLAN "' + bind + '"')
                    exception "not readable"
                }
            } finally {
                GetDebugPoint().disableDebugPointForAllFEs(point)
            }
            long retriedId = 0
            waitUntil("retry adopts the committed baseline", {
                try {
                    retriedId = createBaseline("GLOBAL")
                    return true
                } catch (Exception e) {
                    logger.info("Waiting for committed SPM baseline: " + e.message)
                    return false
                }
            })
            def matching = sql("SHOW BASELINE PLANS WHERE scope = 'GLOBAL'").findAll {
                it[1].toString().contains("spm_cloud_t")
            }
            assertEquals(1, matching.size())
            assertEquals(retriedId, matching[0][0] as long)
            assertTrue(hit(retriedId, bind))
            sql "DROP TABLE spm_cloud_t"
            sql createTable
            sql "INSERT INTO spm_cloud_t VALUES (8)"
            assertTrue(!hit(retriedId, bind))
            assertEquals([[8]], sql(bind))
            sql "DROP BASELINE PLAN ${retriedId}"
            assertTrue(!hit(retriedId, bind))
        }

        // Only this disposable test cluster is modified: simulate a pre-existing internal
        // schema in which none of the first-release SPM tables has been created.
        for (String name : ["spm_baselines", "spm_baselines_seq", "spm_baselines_hwm"]) {
            sql "DROP TABLE IF EXISTS __internal_schema.${name} FORCE"
        }
        connectWithDockerCluster(cluster, false) {
            configureSession(etl)
            long localId = createBaseline("SESSION")
            try {
                assertTrue(hit(localId, bind))
                assertEquals(1, sql("SHOW BASELINE PLANS WHERE id = ${localId}").size())
                test {
                    sql('CREATE GLOBAL BASELINE PLAN "' + bind + '"')
                    exception "GLOBAL store is not ready"
                }
            } finally {
                sql "DROP BASELINE PLAN IF EXISTS ${localId}"
            }
        }
        cluster.restartFrontends()
        connectWithDockerCluster(cluster, false) {
            configureSession(etl)
            waitUntil("first SPM table creation in existing internal schema", tablesReady)
            long id = createBaseline("GLOBAL")
            try {
                assertTrue(hit(id, bind))
                assertEquals([[8]], sql(bind))
            } finally {
                sql "DROP BASELINE PLAN IF EXISTS ${id}"
            }
        }
    }
}
