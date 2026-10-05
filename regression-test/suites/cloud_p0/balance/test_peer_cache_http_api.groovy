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

// Tests for /api/peer_cache HTTP admin endpoints:
// 1. show_all returns empty initially
// 2. set injects peer candidates for a tablet
// 3. show returns the injected candidates
// 4. reset_cooldown clears cooldown state
// 5. remove deletes peer candidates
// 6. peer read actually uses injected candidates (end-to-end)
//
// Topology: 1 FE + CG-A (1 BE, "compute_cluster") + CG-B (1 BE, "cross_cg_peer_cluster")

import org.apache.doris.regression.suite.ClusterOptions
import groovy.json.JsonSlurper

suite('test_peer_cache_http_api', 'docker') {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'auto_check_statistics_in_minutes=60',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'enable_cache_read_from_peer=true',
        'enable_peer_s3_race=true',
        'file_cache_each_block_size=131072',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'file_cache_exit_disk_resource_limit_mode_percent=98',
        'file_cache_enter_need_evict_cache_in_advance_percent=99',
        'file_cache_exit_need_evict_cache_in_advance_percent=98',
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()

    def jsonSlurper = new JsonSlurper()

    // Helper: call GET /api/peer_cache on a specific BE
    def peerCacheGet = { beHost, beHttpPort, params ->
        def result = null
        httpTest {
            endpoint ""
            uri "${beHost}:${beHttpPort}/api/peer_cache?${params}"
            op "get"
            check { respCode, body ->
                logger.info("GET /api/peer_cache?${params} => ${respCode}: ${body}")
                result = [code: respCode, body: body]
            }
        }
        return result
    }

    // Helper: call POST /api/peer_cache on a specific BE
    def peerCachePost = { beHost, beHttpPort, params, jsonBody ->
        def result = null
        httpTest {
            endpoint ""
            uri "${beHost}:${beHttpPort}/api/peer_cache?${params}"
            body jsonBody
            check { respCode, body ->
                logger.info("POST /api/peer_cache?${params} => ${respCode}: ${body}")
                result = [code: respCode, body: body]
            }
        }
        return result
    }

    // Read a bvar counter from a BE's brpc_metrics endpoint.
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

    docker(options) {
        def tbl = "test_peer_cache_http_api_tbl"

        // ---- Setup: Add CG-B ----
        cluster.addBackend(1, "cross_cg_peer_cluster")
        sleep(5000)

        def backends = sql_return_maparray('show backends')
        assert backends.size() == 2 : "Expected 2 backends after adding CG-B"
        def beA = backends.get(0)
        def beB = backends.get(1)
        logger.info("CG-A BE: host={} httpPort={} brpcPort={}", beA.Host, beA.HttpPort, beA.BrpcPort)
        logger.info("CG-B BE: host={} httpPort={} brpcPort={}", beB.Host, beB.HttpPort, beB.BrpcPort)

        def beBHost = beB.Host
        def beBHttpPort = beB.HttpPort as int
        def beAHost = beA.Host
        def beABrpcPort = beA.BrpcPort as int

        // ==================== T1: show_all returns empty initially ====================
        logger.info("=== T1: show_all returns empty initially ===")
        def t1 = peerCacheGet(beBHost, beBHttpPort, "op=show_all")
        assertEquals(200, t1.code, "show_all should succeed")
        def t1Json = jsonSlurper.parseText(t1.body)
        assertEquals(0, t1Json.total, "no peer candidates should exist initially")
        logger.info("PASS: show_all empty")

        // ==================== T2: set injects peer candidates ====================
        logger.info("=== T2: set injects peer candidates for tablet 99999 ===")
        def setBody = """{
            "candidates": [
                {"host": "${beAHost}", "brpc_port": ${beABrpcPort}, "compute_group_id": "compute_cluster"},
                {"host": "10.0.0.99", "brpc_port": 9999, "compute_group_id": "fake_cg"}
            ],
            "last_successful_compute_group_id": "compute_cluster",
            "consecutive_all_miss": 0,
            "cooldown_until_ms": 0
        }"""
        def t2 = peerCachePost(beBHost, beBHttpPort, "op=set&tablet_id=99999", setBody)
        assertEquals(200, t2.code, "set should succeed")
        logger.info("PASS: set succeeded")

        // ==================== T3: show returns injected candidates ====================
        logger.info("=== T3: show returns injected candidates ===")
        def t3 = peerCacheGet(beBHost, beBHttpPort, "op=show&tablet_id=99999")
        assertEquals(200, t3.code, "show should succeed")
        def t3Json = jsonSlurper.parseText(t3.body)
        assertEquals(99999L, t3Json.tablet_id as long, "tablet_id should match")
        assertEquals(2, t3Json.candidates.size(), "should have 2 candidates")
        assertEquals(beAHost, t3Json.candidates[0].host, "first candidate host should be CG-A")
        assertEquals(beABrpcPort, t3Json.candidates[0].brpc_port as int, "first candidate brpc_port should match")
        assertEquals("compute_cluster", t3Json.candidates[0].compute_group_id)
        assertEquals("10.0.0.99", t3Json.candidates[1].host)
        assertEquals("compute_cluster", t3Json.last_successful_compute_group_id)
        assertEquals(0, t3Json.consecutive_all_miss)
        assertEquals(0L, t3Json.cooldown_until_ms as long)
        logger.info("PASS: show returns correct data")

        // ==================== T4: show_all now has 1 tablet ====================
        logger.info("=== T4: show_all has 1 tablet ===")
        def t4 = peerCacheGet(beBHost, beBHttpPort, "op=show_all&limit=10")
        assertEquals(200, t4.code)
        def t4Json = jsonSlurper.parseText(t4.body)
        assertEquals(1, t4Json.total, "should have 1 tablet after set")
        assertEquals(99999L, t4Json.tablets[0].tablet_id as long)
        logger.info("PASS: show_all has 1 tablet")

        // ==================== T5: set cooldown then reset ====================
        logger.info("=== T5: set cooldown then reset ===")
        // Force-set a tablet with cooldown active
        def cooldownBody = """{
            "candidates": [
                {"host": "${beAHost}", "brpc_port": ${beABrpcPort}, "compute_group_id": "cg1"}
            ],
            "last_successful_compute_group_id": "",
            "consecutive_all_miss": 10,
            "cooldown_until_ms": 9999999999999
        }"""
        def t5set = peerCachePost(beBHost, beBHttpPort, "op=set&tablet_id=88888", cooldownBody)
        assertEquals(200, t5set.code)

        // Verify cooldown is set
        def t5show = peerCacheGet(beBHost, beBHttpPort, "op=show&tablet_id=88888")
        def t5Json = jsonSlurper.parseText(t5show.body)
        assertEquals(10, t5Json.consecutive_all_miss)
        assertTrue(t5Json.cooldown_until_ms as long > 0, "cooldown should be active")

        // Reset cooldown
        def t5reset = peerCachePost(beBHost, beBHttpPort, "op=reset_cooldown&tablet_id=88888", "")
        assertEquals(200, t5reset.code)

        // Verify cooldown is cleared
        def t5after = peerCacheGet(beBHost, beBHttpPort, "op=show&tablet_id=88888")
        def t5afterJson = jsonSlurper.parseText(t5after.body)
        assertEquals(0, t5afterJson.consecutive_all_miss, "consecutive_all_miss should be reset")
        assertEquals(0L, t5afterJson.cooldown_until_ms as long, "cooldown_until_ms should be reset")
        logger.info("PASS: cooldown reset works")

        // ==================== T6: remove deletes peer candidates ====================
        logger.info("=== T6: remove deletes peer candidates ===")
        def t6rm = peerCachePost(beBHost, beBHttpPort, "op=remove&tablet_id=99999", "")
        assertEquals(200, t6rm.code)

        // show should now return error (not found)
        def t6show = peerCacheGet(beBHost, beBHttpPort, "op=show&tablet_id=99999")
        assertEquals(500, t6show.code, "show should fail for removed tablet")

        // Also remove the cooldown test tablet
        peerCachePost(beBHost, beBHttpPort, "op=remove&tablet_id=88888", "")
        logger.info("PASS: remove works")

        // ==================== T7: show for non-existent tablet returns error ====================
        logger.info("=== T7: show non-existent tablet ===")
        def t7 = peerCacheGet(beBHost, beBHttpPort, "op=show&tablet_id=11111")
        assertEquals(500, t7.code, "show non-existent tablet should return error")
        logger.info("PASS: show non-existent returns error")

        // ==================== T8: invalid op returns error ====================
        logger.info("=== T8: invalid op ===")
        def t8 = peerCacheGet(beBHost, beBHttpPort, "op=invalid_op")
        assertEquals(500, t8.code, "invalid op should return error")
        logger.info("PASS: invalid op returns error")

        // ==================== T9: end-to-end — inject candidates, then query uses peer ====================
        logger.info("=== T9: end-to-end peer read with injected candidates ===")

        // Create table and insert data in CG-A
        sql "use @compute_cluster"
        sql "DROP TABLE IF EXISTS ${tbl}"
        sql """
            CREATE TABLE ${tbl} (
                k1 INT,
                v1 VARCHAR(256)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO ${tbl} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')"
        // Warm CG-A file cache
        def warmResult = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertEquals(3, warmResult.size())
        sleep(1000)

        // Get the tablet ID of the table
        def tablets = sql_return_maparray("SHOW TABLETS FROM ${tbl}")
        assertTrue(tablets.size() > 0, "table should have at least one tablet")
        def tabletId = tablets[0].TabletId as long
        logger.info("tablet_id for ${tbl}: ${tabletId}")

        // Inject CG-A as peer candidate for this tablet on CG-B
        def e2eBody = """{
            "candidates": [
                {"host": "${beAHost}", "brpc_port": ${beABrpcPort}, "compute_group_id": "compute_cluster"}
            ],
            "last_successful_compute_group_id": "",
            "consecutive_all_miss": 0,
            "cooldown_until_ms": 0
        }"""
        def t9set = peerCachePost(beBHost, beBHttpPort, "op=set&tablet_id=${tabletId}", e2eBody)
        assertEquals(200, t9set.code, "inject candidate for real tablet should succeed")

        // Verify it shows up
        def t9show = peerCacheGet(beBHost, beBHttpPort, "op=show&tablet_id=${tabletId}")
        assertEquals(200, t9show.code)
        def t9showJson = jsonSlurper.parseText(t9show.body)
        assertEquals(1, t9showJson.candidates.size())
        logger.info("Injected candidate for tablet ${tabletId}: {}", t9showJson)

        // Insert new data in CG-A to create uncached rowset for CG-B
        sql "use @compute_cluster"
        sql "INSERT INTO ${tbl} VALUES (4, 'delta'), (5, 'epsilon')"
        sql "SELECT * FROM ${tbl} ORDER BY k1" // warm CG-A cache
        sleep(1000)

        // Record peer metrics before query from CG-B
        def peerHitBefore = getBrpcMetrics(beBHost, beB.BrpcPort, "peer_candidate_cache_hit")

        // Query from CG-B — new rowset not in CG-B local cache, should try peer from injected candidate
        sql "use @cross_cg_peer_cluster"
        def t9result = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertEquals(5, t9result.size(), "CG-B query should return all 5 rows")

        // Verify peer candidate cache was hit (the injected candidate was used)
        def peerHitAfter = getBrpcMetrics(beBHost, beB.BrpcPort, "peer_candidate_cache_hit")
        assertTrue(peerHitAfter > peerHitBefore,
            "peer_candidate_cache_hit should increase after query with injected candidates")
        logger.info("peer_candidate_cache_hit: before={} after={}", peerHitBefore, peerHitAfter)

        // Verify via show that the candidate was accessed (last_access_time_ms updated)
        def t9showAfter = peerCacheGet(beBHost, beBHttpPort, "op=show&tablet_id=${tabletId}")
        def t9afterJson = jsonSlurper.parseText(t9showAfter.body)
        assertTrue(t9afterJson.candidates[0].last_access_time_ms as long > 0,
            "last_access_time_ms should be updated after access")
        logger.info("PASS: end-to-end peer read with injected candidates works")

        // Cleanup
        sql "use @compute_cluster"
        sql "DROP TABLE IF EXISTS ${tbl}"
        logger.info("=== All peer cache HTTP API tests passed ===")
    }
}
