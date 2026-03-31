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

// Cross compute group peer read core tests:
// lazy FE fetch + peer wins race (P0)
// S3 wins when peer BE is down (P0)
// race disabled falls back to serial (P1)
// query profile records cross-CG metrics (P0)
// cold miss S3 first, lazy fetch, then peer wins (P0)
//
// Topology: 1 FE + CG-A (1 BE, "compute_cluster") + CG-B (1 BE, "cross_cg_peer_cluster")

import org.apache.doris.regression.suite.ClusterOptions

suite('test_cross_cg_peer_read_core', 'docker') {
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
        'max_concurrent_peer_races=64',
        'file_cache_each_block_size=131072',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'file_cache_exit_disk_resource_limit_mode_percent=98',
        'file_cache_enter_need_evict_cache_in_advance_percent=99',
        'file_cache_exit_need_evict_cache_in_advance_percent=98',
    ]
    options.setFeNum(1)
    options.setBeNum(1) // CG-A starts with 1 BE
    options.cloudMode = true
    options.enableDebugPoints()

    // Read a bvar counter from a BE's brpc_metrics endpoint. Returns 0 if the metric is not yet
    // present (common for counters that haven't been incremented).
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

    def getBeConfig = { be, key ->
        def (code, out, err) = curl("GET",
            "http://${be.Host}:${be.HttpPort}/api/show_config?conf_item=${key}")
        assertEquals(0, code, "failed to get ${key} from ${be.Host}:${be.HttpPort}: ${err}")
        assertTrue(out.contains(key), "show_config response must contain ${key}: ${out}")
        def rows = parseJson(out)
        assertEquals(1, rows.size(), "show_config should return one row for ${key}: ${out}")
        assertEquals(4, rows[0].size(), "unexpected show_config row for ${key}: ${out}")
        rows[0][2] as String
    }

    def updateBeConfig = { be, key, value ->
        def (code, out, err) = curl("POST",
            "http://${be.Host}:${be.HttpPort}/api/update_config?${key}=${value}")
        assertEquals(0, code, "failed to set ${key}=${value} on ${be.Host}:${be.HttpPort}: ${err}")
        assertTrue(out.contains("OK"),
            "failed to set ${key}=${value} on ${be.Host}:${be.HttpPort}: ${out}")
    }

    def withBeConfigTemporary = { be, tempConfig, actionSupplier ->
        def originConfig = [:]
        tempConfig.each { key, value ->
            originConfig[key] = getBeConfig(be, key)
        }
        try {
            tempConfig.each { key, value ->
                updateBeConfig(be, key, value)
            }
            actionSupplier()
        } finally {
            originConfig.each { key, value ->
                updateBeConfig(be, key, value)
            }
        }
    }

    docker(options) {
        def tbl = "test_cross_cg_peer_read_core_tbl"

        // Counter for unique k1 ranges across inserts; CG-B will never have local cache for them.
        def nextK1 = 100
        // Insert `count` rows into CG-A, warm CG-A's file cache, then switch to CG-B context.
        def insertAndWarmCGA = { count ->
            def start = nextK1
            nextK1 += count
            def vals = (start..<nextK1).collect { i -> "(${i}, 'row${i}')" }.join(", ")
            sql "use @compute_cluster"
            sql "INSERT INTO ${tbl} VALUES ${vals}"
            sql "SELECT * FROM ${tbl} ORDER BY k1"  // warm CG-A cache
            sleep(1000)
            sql "use @cross_cg_peer_cluster"
        }

        // ---- Setup: Add CG-B ----
        cluster.addBackend(1, "cross_cg_peer_cluster")
        // Allow BE to register and send heartbeat before we read show backends.
        sleep(5000)

        // Backends are listed in registration order: index 0 = CG-A BE, index 1 = CG-B BE.
        def backends = sql_return_maparray('show backends')
        assert backends.size() == 2 : "Expected 2 backends after adding CG-B"
        def beA = backends.get(0)
        def beB = backends.get(1)
        logger.info("CG-A BE: host={} brpcPort={}", beA.Host, beA.BrpcPort)
        logger.info("CG-B BE: host={} brpcPort={}", beB.Host, beB.BrpcPort)

        // ---- Setup: Create table and insert data in CG-A ----
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
        sql "INSERT INTO ${tbl} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')"
        sql "INSERT INTO ${tbl} VALUES (4, 'delta'), (5, 'epsilon'), (6, 'zeta')"

        // ---- Warm CG-A file cache by querying once in CG-A ----
        def warmResult = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertEquals(6, warmResult.size(), "Setup: CG-A warm query should return 6 rows")
        sleep(1000)

        // ==================== Cold miss S3 first, then peer ====================
        logger.info("=== cold miss S3 first, then peer ===")

        def t10_lazyFetchBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_triggered")
        def t10_peerWinBefore   = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")

        // First query from CG-B: no candidates yet → cold miss → S3 wins, async lazy fetch fires.
        sql "use @cross_cg_peer_cluster"
        def t10_r1 = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertEquals(6, t10_r1.size(), "first query (cold miss) should succeed via S3")
        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_triggered") > t10_lazyFetchBefore,
            "lazy fetch should be triggered on cold miss")

        // Wait up to 30 s for lazy FE fetch to complete and populate candidate cache.
        awaitUntil(30) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > 0
        }
        logger.info("lazy fetch success confirmed")

        // Insert a fresh rowset into CG-A so CG-B's local cache doesn't have it yet.
        // Without new data, the second query would be a local cache hit (CG-B already
        // cached everything from the first query via S3) and peer_race_peer_win would never increase.
        insertAndWarmCGA(2)

        // Second query from CG-B: new rowset not in CG-B local cache, candidates available → peer wins race.
        def t10_r2 = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertTrue(t10_r2.size() > 6, "second query (after lazy fetch) should return more than original 6 rows")

        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > t10_peerWinBefore,
            "peer should win race after lazy fetch populates candidates")
        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_candidate_cache_hit") > 0,
            "candidate cache should be hit on second query")
        logger.info("PASS")

        // ==================== + Peer wins, bvar + profile metrics ====================
        logger.info("=== peer wins bvar + profile ===")

        // Insert another fresh rowset so CG-B has no local cache for the new blocks.
        insertAndWarmCGA(2)

        def t1_peerWinBefore     = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")
        def t1_crossCgReadBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_cross_compute_group_read")

        profile("cross_cg_peer_win_profile") {
            sql "set enable_profile = true"
            sql "set profile_level = 2"
            run {
                sql "/* cross_cg_peer_win_profile */ SELECT * FROM ${tbl} ORDER BY k1"
                sleep(500)
            }
            check { profileString, exception ->
                logger.info("profile snippet: {}", profileString.take(2000))
                // CrossCGPeerIOTotal should be > 0 (at least one cross-CG peer IO)
                def crossCgMatcher = (profileString =~ /CrossCGPeerIOTotal:\s+(\d+)/)
                def crossCgTotal = 0
                while (crossCgMatcher.find()) {
                    crossCgTotal += crossCgMatcher.group(1).toInteger()
                }
                assertTrue(crossCgTotal > 0, "CrossCGPeerIOTotal must be > 0 in profile")

                // PeerRaceWin counter should reflect at least one race win
                def raceWinMatcher = (profileString =~ /PeerRaceWin:\s+(\d+)/)
                def raceWin = 0
                while (raceWinMatcher.find()) {
                    raceWin += raceWinMatcher.group(1).toInteger()
                }
                assertTrue(raceWin > 0, "PeerRaceWin must be > 0 in profile")
            }
        }

        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > t1_peerWinBefore,
            "peer_race_peer_win bvar should increase after cross-CG peer read")
        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_cross_compute_group_read") > t1_crossCgReadBefore,
            "peer_cross_compute_group_read bvar should increase")
        logger.info("PASS")

        // ==================== Race disabled → serial peer path ====================
        logger.info("=== race disabled, serial peer path ===")

        // enable_peer_s3_race=false: cross-CG candidates go through serial peer→S3, no race counters.
        withBeConfigTemporary(beB, ['enable_peer_s3_race': 'false']) {
            sql "use @cross_cg_peer_cluster"
            def t5_peerWinBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")
            def t5_s3WinBefore   = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_s3_win")

            def t5_result = sql "SELECT * FROM ${tbl} ORDER BY k1"
            assertTrue(t5_result.size() > 0, "serial path should return data")

            // Race counters must NOT increase while race is disabled.
            assertEquals(t5_peerWinBefore,
                getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win"),
                "peer_race_peer_win must not change when race disabled")
            assertEquals(t5_s3WinBefore,
                getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_s3_win"),
                "peer_race_s3_win must not change when race disabled")
        }
        logger.info("PASS")

        // ==================== S3 wins when CG-A BE is down ====================
        logger.info("=== S3 wins when CG-A BE is unreachable ===")

        // Insert fresh rows into CG-A and warm CG-A's cache BEFORE stopping CG-A.
        // CG-B has no local cache for these new blocks. After CG-A is stopped, the
        // winner race will try peer (fails: CG-A is down) → S3 wins → peer_race_s3_win increases.
        // Without fresh uncached blocks, CG-B would serve all data from its own local cache
        // (populated during T5/T1/T10) and peer_race_s3_win would never move.
        insertAndWarmCGA(2)

        def t2_s3WinBefore   = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_s3_win")
        def t2_peerWinBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")

        // Stop CG-A BE (compose index 1).
        cluster.stopBackends(1)
        sleep(3000)

        def t2_result = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertTrue(t2_result.size() > 0, "S3 fallback should return data when CG-A BE is down")
        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_s3_win") > t2_s3WinBefore,
            "peer_race_s3_win should increase when peer is unreachable")
        assertEquals(t2_peerWinBefore,
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win"),
            "peer_race_peer_win should not increase when CG-A BE is down")

        // Restart CG-A BE for cleanup.
        cluster.startBackends(1)
        sleep(5000)
        logger.info("PASS")

        // ==================== max_concurrent_peer_races=0 → degrades to serial ====================
        logger.info("=== max_concurrent_peer_races=0 disables race ===")

        // When the concurrent-race guard is saturated (limit=0), the reader falls back to the
        // sequential peer→S3 path, so race counters must not increase.
        withBeConfigTemporary(beB, ['max_concurrent_peer_races': '0']) {
            sql "use @cross_cg_peer_cluster"
            def t6_peerWinBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")
            def t6_s3WinBefore   = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_s3_win")

            def t6_result = sql "SELECT * FROM ${tbl} ORDER BY k1"
            assertTrue(t6_result.size() > 0, "degraded path should still return data")

            assertEquals(t6_peerWinBefore,
                getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win"),
                "peer_race_peer_win must not change when race is concurrency-limited to 0")
            assertEquals(t6_s3WinBefore,
                getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_s3_win"),
                "peer_race_s3_win must not change when race is concurrency-limited to 0")
        }
        logger.info("PASS")

        // ==================== Comprehensive bvar sanity check ====================
        // At this point the docker run has exercised all core scenarios, so we can assert
        // the expected non-zero / zero relationship for every new bvar in aggregate.
        logger.info("=== comprehensive bvar sanity ===")

        // Candidate discovery layer (on CG-B BE).
        assertTrue(getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_candidate_cache_miss") > 0,
            "peer_candidate_cache_miss should be > 0 (cold miss in T10)")
        assertTrue(getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_triggered") > 0,
            "peer_lazy_fetch_triggered should be > 0")
        assertTrue(getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > 0,
            "peer_lazy_fetch_success should be > 0")
        assertEquals(0L, getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_failed"),
            "peer_lazy_fetch_failed should be 0 (FE always reachable in docker)")
        assertTrue(getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_candidate_cache_hit") > 0,
            "peer_candidate_cache_hit should be > 0 (T1/T10 second query)")

        // Winner race layer (on CG-B BE).
        assertTrue(getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > 0,
            "peer_race_peer_win should be > 0")
        assertTrue(getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_s3_win") > 0,
            "peer_race_s3_win should be > 0")
        assertTrue(getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_cross_compute_group_read") > 0,
            "peer_cross_compute_group_read should be > 0")

        logger.info("PASS")
    }
}
