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

// Cross compute group peer read bvar metrics smoke tests.
//
// Verifies that 4 new bvar / config items land and work end-to-end:
//
// T_bvar1 – peer_lazy_fetch_total & peer_lazy_fetch_count
//   CGB has no candidates for the tablet (cold miss). A cross-CG query triggers
//   fetch_candidates_from_fe() on CGB's BE. Both counters must increase.
//
// T_bvar2 – peer_same_compute_group_read
//   A second BE (BE_A2) is added to CG-A. Rebalance warmup registers BE_A1 as a
//   same-CG peer candidate on BE_A2 (via record_balanced_tablet). A warmup segment-download
//   debug point skips cache population while still letting warmup bookkeeping complete.
//   Queries on CG-A that scan tablets on BE_A2 take the same-CG winner-race path.
//   Peer should win with the configured hedge delay, so peer_same_compute_group_read
//   increases on CG-A.
//
// T_bvar3 – peer_race_hedge_delay_ms → peer wins reliably
//   peer_race_hedge_delay_ms=50 is set in beConfigs. CGA has warm cache so the peer
//   RPC returns in ~5 ms, well within the hedge window. S3 is never submitted;
//   peer_race_peer_win must increase on CGB's BE.
//
// Topology: 1 FE + CG-A (BE_A1, then BE_A2 added in T_bvar2) + CG-B (BE_B).

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite('test_cross_cg_bvar_metrics', 'docker') {
    if (!isCloudMode()) {
        return
    }

    final String CG_A_CLUSTER_ID = "compute_cluster_id"

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'auto_check_statistics_in_minutes=60',
        'rehash_tablet_after_be_dead_seconds=3600',
        'cloud_warm_up_for_rebalance_type=async_warmup',
        'cloud_pre_heating_time_limit_sec=20',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'enable_cache_read_from_peer=true',
        'enable_peer_s3_race=true',
        'enable_peer_server_cache_fill=true',
        'peer_server_cache_fill_timeout_ms=6000',
        "peer_cache_fill_compute_group_id=${CG_A_CLUSTER_ID}",
        'enable_packed_file=false',
        'file_cache_each_block_size=131072',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'file_cache_exit_disk_resource_limit_mode_percent=98',
        'file_cache_enter_need_evict_cache_in_advance_percent=99',
        'file_cache_exit_need_evict_cache_in_advance_percent=98',
        // Give peer a 50 ms head start before S3 is submitted (T_bvar3).
        // Peer returns in ~5 ms when CGA cache is warm, so it wins every time.
        'peer_race_hedge_delay_ms=50',
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()

    def getBrpcMetric = { ip, port, name ->
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
        def tbl = "test_cross_cg_bvar_metrics_tbl"

        // ---- Setup: add CG-B and create table in CG-A ----
        cluster.addBackend(1, "cross_cg_peer_cluster")
        sleep(5000)

        def backends = sql_return_maparray('show backends')
        assert backends.size() == 2 : "Expected 2 backends (CG-A and CG-B)"
        def beA = backends.get(0)
        def beB = backends.get(1)
        logger.info("BE_A (CG-A): host={} brpcPort={}", beA.Host, beA.BrpcPort)
        logger.info("BE_B (CG-B): host={} brpcPort={}", beB.Host, beB.BrpcPort)

        sql "use @compute_cluster"
        sql "DROP TABLE IF EXISTS ${tbl}"
        sql """
            CREATE TABLE ${tbl} (
                k1 INT,
                v1 VARCHAR(256)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 4
            PROPERTIES ("replication_num" = "1")
        """
        sql "INSERT INTO ${tbl} VALUES (1, 'a'), (2, 'b')"
        sql "INSERT INTO ${tbl} VALUES (3, 'c'), (4, 'd')"
        // Warm CG-A cache for all existing rows.
        sql "SELECT * FROM ${tbl} ORDER BY k1"
        sleep(1000)

    // ==================== T_bvar1: peer_lazy_fetch_total & peer_lazy_fetch_count ====================
        // CGB has no candidates for any tablet (cold start). The first cross-CG query
        // triggers fetch_candidates_from_fe() on CGB's BE:
        //   g_peer_lazy_fetch_total  << 1      (before RPC)
        //   g_peer_lazy_fetch_latency << µs    (after RPC — LatencyRecorder exposes *_count as
        //   peer_lazy_fetch_count in brpc_metrics / vars)
        logger.info("=== T_bvar1: peer_lazy_fetch_total and peer_lazy_fetch_count ===")

        def t1_fetchTotalBefore   = getBrpcMetric(beB.Host, beB.BrpcPort, "peer_lazy_fetch_total")
        def t1_latencyCountBefore = getBrpcMetric(beB.Host, beB.BrpcPort, "peer_lazy_fetch_count")

        sql "use @cross_cg_peer_cluster"
        sql "SELECT * FROM ${tbl} ORDER BY k1"

        // The singleflight RPC may complete slightly after the query returns.
        awaitUntil(15) {
            getBrpcMetric(beB.Host, beB.BrpcPort, "peer_lazy_fetch_total") > t1_fetchTotalBefore
        }

        def t1_fetchTotalAfter   = getBrpcMetric(beB.Host, beB.BrpcPort, "peer_lazy_fetch_total")
        def t1_latencyCountAfter = getBrpcMetric(beB.Host, beB.BrpcPort, "peer_lazy_fetch_count")

        assertTrue(t1_fetchTotalAfter > t1_fetchTotalBefore,
            "T_bvar1: peer_lazy_fetch_total should increase on CGB after cold-miss cross-CG query " +
            "(before=${t1_fetchTotalBefore}, after=${t1_fetchTotalAfter})")
        assertTrue(t1_latencyCountAfter > t1_latencyCountBefore,
            "T_bvar1: peer_lazy_fetch_count should increase (LatencyRecorder recorded the FE RPC) " +
            "(before=${t1_latencyCountBefore}, after=${t1_latencyCountAfter})")
        logger.info("T_bvar1 PASS: lazy_fetch_total={}, latency_count={}",
            t1_fetchTotalAfter, t1_latencyCountAfter)

        // ==================== T_bvar3: peer_race_hedge_delay_ms → peer wins reliably ====================
        // peer_race_hedge_delay_ms=50 (set in beConfigs). CGA cache is warm, so the peer
        // RPC for the new rows returns in ~5 ms — well within the 50 ms hedge window.
        // S3 is never submitted; peer_race_peer_win must increase on CGB.
        logger.info("=== T_bvar3: peer_race_hedge_delay_ms=50 → peer_race_peer_win increases ===")

        // Insert new rows on CG-A and warm its cache; CGB has never read them.
        sql "use @compute_cluster"
        sql "INSERT INTO ${tbl} VALUES (5, 'e'), (6, 'f')"
        sql "SELECT * FROM ${tbl} ORDER BY k1"  // warm CGA for rows (5, 6)
        sleep(500)

        sql "use @cross_cg_peer_cluster"
        def t3_peerWinBefore = getBrpcMetric(beB.Host, beB.BrpcPort, "peer_race_peer_win")

        // CGB has no cache for (5, 6). Peer race: CGA warm → peer returns ~5 ms;
        // hedge delay 50 ms skips S3 entirely → peer wins.
        def t3_result = sql "SELECT * FROM ${tbl} WHERE k1 IN (5, 6) ORDER BY k1"
        assertEquals(2, t3_result.size(), "T_bvar3: query should return 2 rows")

        assertTrue(
            getBrpcMetric(beB.Host, beB.BrpcPort, "peer_race_peer_win") > t3_peerWinBefore,
            "T_bvar3: peer_race_peer_win should increase on CGB when CGA cache is warm and " +
            "hedge delay keeps S3 submission suppressed")
        logger.info("T_bvar3 PASS: peer_race_peer_win={}",
            getBrpcMetric(beB.Host, beB.BrpcPort, "peer_race_peer_win"))

        // ==================== T_bvar2: peer_same_compute_group_read ====================
        // Add BE_A2 to CG-A. Skip warmup segment downloads so BE_A2's file cache stays empty.
        // Rebalance triggers warm_up_cache_async → record_balanced_tablet on BE_A2 with
        // BE_A1 as same-CG candidate. Once BE_A2 has tablets plus candidate mappings, queries
        // on CG-A that hit BE_A2 should run same-CG peer-vs-S3 winner race; peer wins due to
        // the hedge delay, so peer_same_compute_group_read increases.
        logger.info("=== T_bvar2: peer_same_compute_group_read (same-CG winner race) ===")

        // Skip warmup segment downloads to keep BE_A2's file cache empty after registration.
        GetDebugPoint().enableDebugPointForAllBEs("FileCacheBlockDownloader::download_segment_file.skip_warmup")

        setFeConfig('enable_cloud_multi_replica', true)
        cluster.addBackend(1, "compute_cluster")
        setFeConfig('enable_cloud_multi_replica', false)
        sleep(5000)

        def allBackends = sql_return_maparray('show backends')
        assert allBackends.size() == 3 : "Expected 3 backends total"
        // Identify BE_A2 by BackendId — BrpcPort is the same (8060) for all BEs in docker.
        def beA2 = allBackends.find { it.BackendId.toLong() != beA.BackendId.toLong() &&
                                      it.BackendId.toLong() != beB.BackendId.toLong() }
        assert beA2 != null : "Could not identify BE_A2"
        logger.info("T_bvar2: BE_A2 host={} brpcPort={}", beA2.Host, beA2.BrpcPort)
        DebugPoint.enableDebugPoint(
            beA2.Host, beA2.HttpPort.toInteger(), NodeType.BE,
            "FileCacheBlockDownloader::download_segment_file.skip_warmup")

        // Wait until BE_A2 has both accepted tablets and registered same-CG candidates.
        awaitUntil(120) {
            def latest = sql_return_maparray('show backends').find {
                it.BackendId.toLong() == beA2.BackendId.toLong()
            }
            latest != null && latest.TabletNum.toInteger() > 0
        }
        awaitUntil(120) {
            getBrpcMetric(beA2.Host, beA2.BrpcPort, "balance_tablet_be_mapping_size") > 0
        }

        // Give the rebalance warmup workflow a short window to submit and finish the skipped
        // warmup tasks before query traffic starts.
        sleep(2000)

        sql "use @compute_cluster"
        def t2_sameBefore_A1 = getBrpcMetric(beA.Host,  beA.BrpcPort,  "peer_same_compute_group_read")
        def t2_sameBefore_A2 = getBrpcMetric(beA2.Host, beA2.BrpcPort, "peer_same_compute_group_read")

        // Run multiple queries; full-table scans should cover tablets rebalanced to BE_A2.
        // Those scans see: (a) same-CG candidate (BE_A1) registered, (b) empty local cache
        // after warmup timeout → same-CG winner race with peer win → peer_same_compute_group_read++.
        30.times {
            sql "SELECT * FROM ${tbl} ORDER BY k1"
        }
        awaitUntil(30) {
            def sameAfterA1 = getBrpcMetric(beA.Host,  beA.BrpcPort,  "peer_same_compute_group_read")
            def sameAfterA2 = getBrpcMetric(beA2.Host, beA2.BrpcPort, "peer_same_compute_group_read")
            ((sameAfterA1 - t2_sameBefore_A1) + (sameAfterA2 - t2_sameBefore_A2)) > 0
        }

        def t2_sameAfter_A1 = getBrpcMetric(beA.Host,  beA.BrpcPort,  "peer_same_compute_group_read")
        def t2_sameAfter_A2 = getBrpcMetric(beA2.Host, beA2.BrpcPort, "peer_same_compute_group_read")
        def t2_totalDelta = (t2_sameAfter_A1 - t2_sameBefore_A1) + (t2_sameAfter_A2 - t2_sameBefore_A2)

        assertTrue(t2_totalDelta > 0,
            "T_bvar2: peer_same_compute_group_read should increase on CG-A BEs " +
            "(BE_A2 has same-CG candidate and empty local cache after skipped warmup downloads). " +
            "BE_A1 delta=${t2_sameAfter_A1 - t2_sameBefore_A1}, " +
            "BE_A2 delta=${t2_sameAfter_A2 - t2_sameBefore_A2}")
        logger.info("T_bvar2 PASS: peer_same_compute_group_read total delta={}", t2_totalDelta)

        DebugPoint.disableDebugPoint(
            beA2.Host, beA2.HttpPort.toInteger(), NodeType.BE,
            "FileCacheBlockDownloader::download_segment_file.skip_warmup")
        GetDebugPoint().disableDebugPointForAllBEs("FileCacheBlockDownloader::download_segment_file.skip_warmup")
    }
}
