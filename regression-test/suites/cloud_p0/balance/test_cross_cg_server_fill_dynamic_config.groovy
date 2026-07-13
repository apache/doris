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

// Dynamic config toggle tests for cross-CG server-side fill.
//
// peer_cache_fill_compute_group_id and enable_peer_server_cache_fill are both
// mutable configs — they can be changed at runtime via the HTTP API without
// restarting BE.  This suite verifies that toggling them takes effect immediately.
//
// Two rounds (each a fresh docker cluster):
//   Round 1 — enable_peer_server_cache_fill: start disabled → no fill → enable dynamically → fill works → disable → no fill
//   Round 2 — peer_cache_fill_compute_group_id: start with wrong id → no fill → correct id dynamically → fill works → wrong id → no fill
//
// Topology per round: 1 FE + CG-A (1 BE, "compute_cluster") + CG-B (1 BE, "cross_cg_peer_cluster")

import org.apache.doris.regression.suite.ClusterOptions

suite('test_cross_cg_server_fill_dynamic_config', 'docker') {
    if (!isCloudMode()) {
        return
    }

    final String CG_A_CLUSTER_ID = "compute_cluster_id"

    def getBrpcMetrics = { ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
            url = url.replace("http://", "https://") +
                " --cert " + context.config.otherConfigs.get("trustCert") +
                " --cacert " + context.config.otherConfigs.get("trustCACert") +
                " --key " + context.config.otherConfigs.get("trustCAKey")
        }
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        }
        return 0L
    }

    // Two rounds with different initial configs.
    def clusterOptionsList = [
        new ClusterOptions(),  // Round 1: enable_peer_server_cache_fill toggle
        new ClusterOptions(),  // Round 2: peer_cache_fill_compute_group_id toggle
    ]

    def commonFeConfigs = [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'auto_check_statistics_in_minutes=60',
    ]
    def commonBeConfigs = [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'enable_cache_read_from_peer=true',
        'enable_peer_s3_race=true',
        'peer_server_cache_fill_timeout_ms=6000',
        'enable_packed_file=false',
        'file_cache_each_block_size=131072',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'file_cache_exit_disk_resource_limit_mode_percent=98',
        'file_cache_enter_need_evict_cache_in_advance_percent=99',
        'file_cache_exit_need_evict_cache_in_advance_percent=98',
    ]

    for (options in clusterOptionsList) {
        options.feConfigs += commonFeConfigs
        options.beConfigs += commonBeConfigs
        options.setFeNum(1)
        options.setBeNum(1)
        options.cloudMode = true
        options.enableDebugPoints()
    }

    // Round 1: fill DISABLED initially, correct CG id.
    clusterOptionsList[0].beConfigs += [
        'enable_peer_server_cache_fill=false',
        "peer_cache_fill_compute_group_id=${CG_A_CLUSTER_ID}",
    ]
    // Round 2: fill ENABLED initially, but WRONG CG id.
    clusterOptionsList[1].beConfigs += [
        'enable_peer_server_cache_fill=true',
        'peer_cache_fill_compute_group_id=nonexistent_cg_id',
    ]

    def roundLabels = [
        "enable_peer_server_cache_fill toggle",
        "peer_cache_fill_compute_group_id toggle",
    ]

    for (int roundIdx = 0; roundIdx < clusterOptionsList.size(); roundIdx++) {
        def options = clusterOptionsList[roundIdx]
        def roundLabel = roundLabels[roundIdx]

        logger.info("===== Round {}: {} =====", roundIdx + 1, roundLabel)

        docker(options) {
            def tbl = "test_dyn_cfg_tbl"
            def nextK1 = 100

            def checkBeHttpReady = { be, key ->
                def cmd = "curl -s --max-time 2 -X GET " +
                    "http://${be.Host}:${be.HttpPort}/api/show_config?conf_item=${key}"
                def process = cmd.execute()
                def out = new StringBuilder()
                def err = new StringBuilder()
                process.waitForProcessOutput(out, err)
                process.exitValue() == 0 && out.toString().contains(key)
            }

            def waitForBeHttpReady = { be, key ->
                awaitUntil(120) {
                    checkBeHttpReady(be, key)
                }
            }

            // ---- Helper: update a single BE config via HTTP API ----
            def updateBeConfig = { be, key, value ->
                waitForBeHttpReady(be, key)
                def (code, out, err) = curl("POST",
                    "http://${be.Host}:${be.HttpPort}/api/update_config?${key}=${value}")
                assertEquals(0, code,
                    "failed to set ${key}=${value} on ${be.Host}:${be.HttpPort}: ${err}")
                assertTrue(out.contains("OK"),
                    "failed to set ${key}=${value} on ${be.Host}:${be.HttpPort}: ${out}")
            }

            // ---- Helper: clear CG-A file cache (requires restart, used for setup only) ----
            def clearCgACacheAndRestart = { beABackendId ->
                cluster.stopBackends(1)
                sleep(2000)
                def node = cluster.getBeByBackendId(beABackendId as long)
                def cacheDir = new File("${node.path}/storage/file_cache")
                if (cacheDir.exists()) {
                    cacheDir.deleteDir()
                    logger.info("Deleted CG-A file cache: {}", cacheDir.absolutePath)
                }
                cluster.startBackends(1)
                sleep(5000)
                waitForBeHttpReady(node, "enable_peer_server_cache_fill")
            }

            // ---- Helper: insert new rows into CG-A and warm its cache ----
            def insertAndWarmCGA = { count ->
                def start = nextK1
                nextK1 += count
                def vals = (start..<nextK1).collect { i -> "(${i}, 'v${i}')" }.join(", ")
                sql "use @compute_cluster"
                sql "INSERT INTO ${tbl} VALUES ${vals}"
                sql "SELECT * FROM ${tbl} ORDER BY k1"
                sleep(1000)
            }

            // ---- Setup: Add CG-B ----
            cluster.addBackend(1, "cross_cg_peer_cluster")
            sleep(5000)

            def backends = sql_return_maparray('show backends')
            assert backends.size() == 2 : "Expected 2 backends"
            def beA = backends.get(0)
            def beB = backends.get(1)
            logger.info("CG-A BE: host={} brpc={} http={}", beA.Host, beA.BrpcPort, beA.HttpPort)
            logger.info("CG-B BE: host={} brpc={} http={}", beB.Host, beB.BrpcPort, beB.HttpPort)

            // ---- Setup: Create table and seed data ----
            sql "use @compute_cluster"
            sql "DROP TABLE IF EXISTS ${tbl}"
            sql """
                CREATE TABLE ${tbl} (
                    k1 INT,
                    v1 VARCHAR(256)
                )
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 2
                PROPERTIES ("replication_num" = "1")
            """
            sql "INSERT INTO ${tbl} VALUES (1, 'a'), (2, 'b')"
            sql "INSERT INTO ${tbl} VALUES (3, 'c'), (4, 'd')"
            sql "SELECT * FROM ${tbl} ORDER BY k1"  // warm CG-A
            sleep(1000)

            // ---- Trigger lazy FE fetch from CG-B so CG-A becomes a candidate ----
            sql "use @cross_cg_peer_cluster"
            sql "SELECT * FROM ${tbl} ORDER BY k1"
            awaitUntil(30) {
                getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > 0
            }
            logger.info("CG-B lazy fetch done — CG-A is now a peer candidate")

            // ==================================================================
            // Phase 1: initial config — fill should NOT work
            // ==================================================================
            logger.info("--- Phase 1: fill should NOT work (initial config) ---")

            insertAndWarmCGA(4)
            clearCgACacheAndRestart(beA.BackendId)
            // Refresh beA reference after restart.
            backends = sql_return_maparray('show backends')
            beA = backends.get(0)

            def p1_fillBefore = getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_requested")

            sql "use @cross_cg_peer_cluster"
            def p1_result = sql "SELECT * FROM ${tbl} ORDER BY k1"
            assertTrue(p1_result.size() >= 8, "Phase 1: query should succeed via S3 fallback")

            assertEquals(p1_fillBefore,
                getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_requested"),
                "Phase 1: peer_server_fill_requested must NOT increase")
            logger.info("Phase 1 PASS")

            // ==================================================================
            // Phase 2: dynamically enable fill — should take effect immediately
            // ==================================================================
            logger.info("--- Phase 2: dynamically enable fill ---")

            if (roundLabel.contains("enable_peer_server_cache_fill")) {
                updateBeConfig(beA, "enable_peer_server_cache_fill", "true")
                logger.info("Dynamically set enable_peer_server_cache_fill=true on CG-A")
            } else {
                updateBeConfig(beA, "peer_cache_fill_compute_group_id", CG_A_CLUSTER_ID)
                updateBeConfig(beB, "peer_cache_fill_compute_group_id", CG_A_CLUSTER_ID)
                logger.info("Dynamically set peer_cache_fill_compute_group_id={}", CG_A_CLUSTER_ID)
            }
            sleep(500)

            // Insert new data and clear CG-A cache so blocks are EMPTY.
            insertAndWarmCGA(4)
            clearCgACacheAndRestart(beA.BackendId)

            // Re-apply the dynamic config after restart (restart resets to be_custom.conf).
            backends = sql_return_maparray('show backends')
            beA = backends.get(0)
            if (roundLabel.contains("enable_peer_server_cache_fill")) {
                updateBeConfig(beA, "enable_peer_server_cache_fill", "true")
            } else {
                updateBeConfig(beA, "peer_cache_fill_compute_group_id", CG_A_CLUSTER_ID)
                updateBeConfig(beB, "peer_cache_fill_compute_group_id", CG_A_CLUSTER_ID)
            }
            sleep(500)

            def p2_fillBefore = getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_requested")

            sql "use @cross_cg_peer_cluster"
            def p2_result = sql "SELECT * FROM ${tbl} ORDER BY k1"
            assertTrue(p2_result.size() >= 12, "Phase 2: query should return all rows")

            assertTrue(
                getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_requested") > p2_fillBefore,
                "Phase 2: peer_server_fill_requested must increase after dynamic enable")
            logger.info("Phase 2 PASS")

            // ==================================================================
            // Phase 3: dynamically disable fill again — verify it stops
            // ==================================================================
            logger.info("--- Phase 3: dynamically disable fill ---")

            if (roundLabel.contains("enable_peer_server_cache_fill")) {
                updateBeConfig(beA, "enable_peer_server_cache_fill", "false")
                logger.info("Dynamically set enable_peer_server_cache_fill=false on CG-A")
            } else {
                updateBeConfig(beA, "peer_cache_fill_compute_group_id", "nonexistent_cg_id")
                updateBeConfig(beB, "peer_cache_fill_compute_group_id", "nonexistent_cg_id")
                logger.info("Dynamically set peer_cache_fill_compute_group_id=nonexistent_cg_id")
            }
            sleep(500)

            insertAndWarmCGA(4)
            clearCgACacheAndRestart(beA.BackendId)

            backends = sql_return_maparray('show backends')
            beA = backends.get(0)

            def p3_fillBefore = getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_requested")

            sql "use @cross_cg_peer_cluster"
            def p3_result = sql "SELECT * FROM ${tbl} ORDER BY k1"
            assertTrue(p3_result.size() >= 16, "Phase 3: query should return all rows via S3 fallback")

            assertEquals(p3_fillBefore,
                getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_requested"),
                "Phase 3: peer_server_fill_requested must NOT increase after re-disabling")
            logger.info("Phase 3 PASS")

            logger.info("===== Round '{}' PASS =====", roundLabel)
        }
    }
}
