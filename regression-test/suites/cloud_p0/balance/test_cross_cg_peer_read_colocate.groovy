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

// Cross compute group peer read for colocate tables.
//
// Bug: For colocate tables, CloudTabletRebalancer.completeRouteInfo() intentionally
// keeps primaryClusterToBackends empty because query routing uses dynamic hashing
// (getColocatedBeId). This caused getAllPrimaryBes() to return an empty list, so
// peer candidate discovery never found any candidates and cross-CG peer read was
// silently broken for colocate tables.
//
// Fix: getAllPrimaryBes() now dynamically computes backends for colocate tables via
// getColocatedBeId() for each cluster, allowing peer candidates to be discovered.
//
// Topology: 1 FE + CG-A (1 BE, "compute_cluster") + CG-B (1 BE, "cross_cg_peer_cluster")

import org.apache.doris.regression.suite.ClusterOptions

suite('test_cross_cg_peer_read_colocate', 'docker') {
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
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()

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
        def colocateTbl1 = "test_cross_cg_colocate_tbl1"
        def colocateTbl2 = "test_cross_cg_colocate_tbl2"
        def colocateGroup = "test_cross_cg_colocate_group"

        def nextK1 = 100
        def insertAndWarmCGA = { tbl, count ->
            def start = nextK1
            nextK1 += count
            def vals = (start..<nextK1).collect { i -> "(${i}, 'row${i}')" }.join(", ")
            sql "use @compute_cluster"
            sql "INSERT INTO ${tbl} VALUES ${vals}"
            sql "SELECT * FROM ${tbl} ORDER BY k1"
            sleep(1000)
            sql "use @cross_cg_peer_cluster"
        }

        // ---- Setup: Add CG-B ----
        cluster.addBackend(1, "cross_cg_peer_cluster")
        sleep(5000)

        def backends = sql_return_maparray('show backends')
        assert backends.size() == 2 : "Expected 2 backends after adding CG-B"
        def beA = backends.get(0)
        def beB = backends.get(1)
        logger.info("CG-A BE: host={} brpcPort={}", beA.Host, beA.BrpcPort)
        logger.info("CG-B BE: host={} brpcPort={}", beB.Host, beB.BrpcPort)

        // ---- Setup: Create colocate tables in CG-A ----
        sql "use @compute_cluster"
        sql "DROP TABLE IF EXISTS ${colocateTbl1}"
        sql "DROP TABLE IF EXISTS ${colocateTbl2}"
        sql """
            CREATE TABLE ${colocateTbl1} (
                k1 INT,
                v1 VARCHAR(256)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "colocate_with" = "${colocateGroup}"
            )
        """
        sql """
            CREATE TABLE ${colocateTbl2} (
                k1 INT,
                v1 VARCHAR(256)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 2
            PROPERTIES (
                "replication_num" = "1",
                "colocate_with" = "${colocateGroup}"
            )
        """

        // Verify colocate group is established
        def groupResult = sql "SHOW PROC '/colocation_group'"
        logger.info("Colocate groups: {}", groupResult)
        assertTrue(groupResult.size() > 0, "colocate group should exist")

        // Insert and warm cache in CG-A
        sql "INSERT INTO ${colocateTbl1} VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')"
        sql "INSERT INTO ${colocateTbl2} VALUES (10, 'ten'), (20, 'twenty'), (30, 'thirty')"
        def warmResult1 = sql "SELECT * FROM ${colocateTbl1} ORDER BY k1"
        def warmResult2 = sql "SELECT * FROM ${colocateTbl2} ORDER BY k1"
        assertEquals(3, warmResult1.size(), "CG-A warm tbl1 should return 3 rows")
        assertEquals(3, warmResult2.size(), "CG-A warm tbl2 should return 3 rows")
        sleep(1000)

        // ==================== Colocate table: cold miss then peer wins ====================
        logger.info("=== colocate table: cold miss then peer wins ===")

        def lazyFetchBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_triggered")

        // First query from CG-B on colocate table: no candidates yet -> cold miss -> S3
        sql "use @cross_cg_peer_cluster"
        def coldResult1 = sql "SELECT * FROM ${colocateTbl1} ORDER BY k1"
        assertEquals(3, coldResult1.size(), "CG-B cold read of colocate tbl1 should return 3 rows")

        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_triggered") > lazyFetchBefore,
            "lazy fetch should be triggered on cold miss for colocate table")

        // Wait for lazy FE fetch to populate candidate cache.
        // This is the critical part: before the fix, getAllPrimaryBes() returned empty for
        // colocate tables, so the FE would never return any peer candidates, and
        // peer_lazy_fetch_success would never increase.
        awaitUntil(30) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > 0
        }
        logger.info("lazy fetch success confirmed for colocate table - fix is working")

        // Insert fresh rowset in CG-A so CG-B has no local cache for it
        insertAndWarmCGA(colocateTbl1, 3)

        def peerWinBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")
        def crossCgBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_cross_compute_group_read")

        // Second query from CG-B: candidates now available -> peer should win race
        def peerResult1 = sql "SELECT * FROM ${colocateTbl1} ORDER BY k1"
        assertTrue(peerResult1.size() > 3, "CG-B should read fresh rows from colocate tbl1")

        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > peerWinBefore
                || getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_cross_compute_group_read") > crossCgBefore,
            "peer read counters should increase for colocate table " +
            "(peer_race_peer_win ${peerWinBefore}->${getBrpcMetrics(beB.Host, beB.BrpcPort, 'peer_race_peer_win')}, " +
            "peer_cross_compute_group_read ${crossCgBefore}->${getBrpcMetrics(beB.Host, beB.BrpcPort, 'peer_cross_compute_group_read')})")
        logger.info("PASS: colocate table peer read works")

        // ==================== Second colocate table in same group ====================
        logger.info("=== second colocate table in same group ===")

        // The second colocate table shares the same colocate group, so it should also
        // benefit from the fix (same dynamic hashing logic).
        def coldResult2 = sql "SELECT * FROM ${colocateTbl2} ORDER BY k1"
        assertEquals(3, coldResult2.size(), "CG-B cold read of colocate tbl2 should return 3 rows")

        // Wait for lazy fetch for tbl2
        sleep(3000)

        insertAndWarmCGA(colocateTbl2, 3)

        def peerWinBefore2 = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")
        def crossCgBefore2 = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_cross_compute_group_read")

        def peerResult2 = sql "SELECT * FROM ${colocateTbl2} ORDER BY k1"
        assertTrue(peerResult2.size() > 3, "CG-B should read fresh rows from colocate tbl2")

        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > peerWinBefore2
                || getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_cross_compute_group_read") > crossCgBefore2,
            "peer read counters should increase for second colocate table")
        logger.info("PASS: second colocate table in same group also works")

        // ==================== Colocate join across CG ====================
        logger.info("=== colocate join query across CG ===")

        sql "use @compute_cluster"
        // Ensure both tables have overlapping keys for the join
        sql "INSERT INTO ${colocateTbl1} VALUES (10, 'ten_in_tbl1')"
        sql "SELECT * FROM ${colocateTbl1} ORDER BY k1"
        sql "SELECT * FROM ${colocateTbl2} ORDER BY k1"
        sleep(1000)

        sql "use @cross_cg_peer_cluster"
        def joinResult = sql """
            SELECT t1.k1, t1.v1, t2.v1
            FROM ${colocateTbl1} t1
            JOIN ${colocateTbl2} t2 ON t1.k1 = t2.k1
            ORDER BY t1.k1
        """
        logger.info("Colocate join result: {}", joinResult)
        assertTrue(joinResult.size() > 0, "colocate join across CG should return results")
        // k1=10 exists in both tables
        def row10 = joinResult.find { it[0] == 10 }
        assertTrue(row10 != null, "join should find k1=10 in both tables")
        logger.info("PASS: colocate join across CG works")

        // ==================== Verify profile shows cross-CG metrics + timer accuracy ====================
        logger.info("=== profile check for colocate peer read ===")

        insertAndWarmCGA(colocateTbl1, 2)

        profile("cross_cg_colocate_profile") {
            sql "set enable_profile = true"
            sql "set profile_level = 2"
            run {
                sql "/* cross_cg_colocate_profile */ SELECT * FROM ${colocateTbl1} ORDER BY k1"
                sleep(500)
            }
            check { profileString, exception ->
                logger.info("colocate profile snippet: {}", profileString.take(4000))

                // CrossCGPeerIOTotal counter must be > 0
                def crossCgMatcher = (profileString =~ /CrossCGPeerIOTotal:\s+(\d+)/)
                def crossCgTotal = 0
                while (crossCgMatcher.find()) {
                    crossCgTotal += crossCgMatcher.group(1).toInteger()
                }
                assertTrue(crossCgTotal > 0,
                    "CrossCGPeerIOTotal must be > 0 in profile for colocate table")

                def nodesMatcher = (profileString =~ /PeerCacheNodes:\s*(.+)/)
                assertTrue(nodesMatcher.find(), "Profile must contain PeerCacheNodes info")
                logger.info("PeerCacheNodes found: {}", nodesMatcher.group(1))

                // PeerRaceWin counter must be > 0
                def raceWinMatcher = (profileString =~ /PeerRaceWin:\s+(\d+)/)
                def raceWin = 0
                while (raceWinMatcher.find()) {
                    raceWin += raceWinMatcher.group(1).toInteger()
                }
                assertTrue(raceWin > 0,
                    "PeerRaceWin must be > 0 in profile for colocate table")

                // CrossCGPeerIOTime must NOT be 0ns — verifies the peer_elapsed_ns
                // propagation fix (commit b8f5b1ef940). Before the fix this timer was
                // always reported as 0ns because peer_io_timer was not propagated from
                // RaceState to ReadStatistics.
                def crossCgTimeMatcher = (profileString =~ /CrossCGPeerIOTime:\s+(\S+)/)
                def foundNonZeroCrossCgTime = false
                while (crossCgTimeMatcher.find()) {
                    def val = crossCgTimeMatcher.group(1)
                    if (val != "0ns" && val != "0s" && val != "0.000ns") {
                        foundNonZeroCrossCgTime = true
                    }
                }
                assertTrue(foundNonZeroCrossCgTime,
                    "CrossCGPeerIOTime must not be 0ns — timer propagation fix verification")

                // PeerIOUseTimer must NOT be 0ns — same root cause: peer read wall-clock
                // time was lost in the race winner path.
                def peerTimerMatcher = (profileString =~ /PeerIOUseTimer:\s+(\S+)/)
                def foundNonZeroPeerTimer = false
                while (peerTimerMatcher.find()) {
                    def val = peerTimerMatcher.group(1)
                    if (val != "0ns" && val != "0s" && val != "0.000ns") {
                        foundNonZeroPeerTimer = true
                    }
                }
                assertTrue(foundNonZeroPeerTimer,
                    "PeerIOUseTimer must not be 0ns — timer propagation fix verification")
            }
        }
        logger.info("PASS: profile shows cross-CG metrics and non-zero timers for colocate table")
    }
}
