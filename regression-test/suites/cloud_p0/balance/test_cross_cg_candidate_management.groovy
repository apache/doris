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

// Cross compute group peer read candidate management tests:
// consecutive RPC failures → candidate eviction (P1)
// candidate expiry via cleanup daemon (P1)
// cache miss (NOT_FOUND from server) rotates candidate, does NOT evict (Issue 3)
//
// Topology:
//   T3/1 FE + CG-A (1 BE, "compute_cluster") + CG-B (1 BE, "cross_cg_peer_cluster")
//   add CG-C (1 BE, "cross_cg_peer_cluster_alt") so rotate can actually move to
//          another remote candidate after CG-A returns NOT_FOUND.

import org.apache.doris.regression.suite.ClusterOptions

suite('test_cross_cg_candidate_management', 'docker') {
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
        // Eviction threshold = 1 so T3 triggers on the very first RPC failure.
        // Threshold > 1 cannot be reliably tested in docker: after the first peer RPC failure
        // S3 fallback fills CG-B's local file cache, so subsequent queries on the same data
        // are local hits and never issue a second peer RPC to CG-A.
        'peer_rpc_failure_eviction_threshold=1',
        // Short expiry so T4 can complete in reasonable time.
        'peer_candidate_expiry_s=60',
        'peer_candidate_cleanup_interval_s=5',
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
        final String CG_A = "compute_cluster"
        final String CG_B = "cross_cg_peer_cluster"
        final String CG_C = "cross_cg_peer_cluster_alt"
        def tbl = "test_cross_cg_candidate_mgmt_tbl"

        // Counter for unique k1 ranges; CG-B will never have local cache for them.
        def nextK1 = 100
        // Insert `count` rows into CG-A, warm CG-A's file cache, then switch to CG-B context.
        def insertAndWarmCGA = { count ->
            def start = nextK1
            nextK1 += count
            def vals = (start..<nextK1).collect { i -> "(${i}, 'row${i}')" }.join(", ")
            sql "use @${CG_A}"
            sql "INSERT INTO ${tbl} VALUES ${vals}"
            sql "SELECT * FROM ${tbl} ORDER BY k1"  // warm CG-A cache
            sleep(1000)
            sql "use @${CG_B}"
        }

        def findBackendsByClusterName = { clusterName ->
            sql_return_maparray('show backends').findAll { backend ->
                backend.CloudClusterName == clusterName || backend.Tag?.contains(clusterName)
            }
        }

        def getSingleBackendByClusterName = { clusterName ->
            def backendsInCluster = findBackendsByClusterName(clusterName)
            assertEquals(1, backendsInCluster.size(),
                "Expected exactly 1 backend in ${clusterName}, got ${backendsInCluster.size()}")
            backendsInCluster[0]
        }

        def getBackendNodeIndex = { backend ->
            def node = cluster.getBeByBackendId(backend.BackendId.toLong())
            assertTrue(node != null, "Backend node not found for BackendId=${backend.BackendId}")
            node.index
        }

        // ---- Setup: Add CG-B ----
        cluster.addBackend(1, CG_B)
        awaitUntil(30) {
            findBackendsByClusterName(CG_A).size() == 1 &&
                findBackendsByClusterName(CG_B).size() == 1
        }

        def beA = getSingleBackendByClusterName(CG_A)
        def beB = getSingleBackendByClusterName(CG_B)
        def beAIndex = getBackendNodeIndex(beA)
        logger.info("CG-A BE: host={} brpcPort={}", beA.Host, beA.BrpcPort)
        logger.info("CG-B BE: host={} brpcPort={}", beB.Host, beB.BrpcPort)

        // ---- Setup: Create table and insert data in CG-A ----
        sql "use @${CG_A}"
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
        // Warm CG-A cache.
        sql "SELECT * FROM ${tbl} ORDER BY k1"
        sleep(1000)

        // Pre-warm CG-B: do a query from CG-B to trigger lazy FE fetch and populate candidates.
        sql "use @${CG_B}"
        sql "SELECT * FROM ${tbl} ORDER BY k1" // cold miss → lazy fetch
        awaitUntil(30) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > 0
        }
        // Insert a fresh rowset in CG-A so CG-B's local cache won't have it yet.
        // This allows the subsequent query from CG-B to exercise the peer race path.
        sql "use @${CG_A}"
        sql "INSERT INTO ${tbl} VALUES (5, 'e'), (6, 'f')"
        sql "SELECT * FROM ${tbl} ORDER BY k1"  // warm CG-A cache for the new rowset
        sleep(1000)

        // One more query from CG-B on the new data to confirm peer wins (candidates in cache).
        sql "use @${CG_B}"
        sql "SELECT * FROM ${tbl} ORDER BY k1"
        awaitUntil(10) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > 0
        }
        logger.info("Setup: CG-B has candidates for CG-A BE and peer wins confirmed")

        // ==================== RPC failure eviction ====================
        logger.info("=== consecutive RPC failures evict candidate ===")

        def t3_evictionBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_rpc_failure_eviction")
        // Use peer_lazy_fetch_success (monotonically increasing Adder) as the pre-condition signal
        // rather than balance_tablet_be_mapping_size (an Adder<uint64_t> used as a gauge that
        // underflows when decrements exceed increments). The setup's awaitUntil already confirmed
        // lazy fetch succeeded, so this assertion is a documentation-level sanity check.
        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > 0,
            "pre-condition: candidates should exist (lazy fetch succeeded during setup)")

        // Insert fresh data into CG-A (while it is still running) and warm CG-A's cache.
        // CG-B has no local cache for these new blocks, so subsequent queries from CG-B will
        // enter the winner race and attempt peer RPCs — which will fail once CG-A is stopped.
        insertAndWarmCGA(4)

        // Stop CG-A BE so all peer RPCs fail.
        sql "use @${CG_A}"  // switch away so queries below route to CG-B
        cluster.stopBackends(beAIndex)
        sleep(2000)
        sql "use @${CG_B}"

        // With threshold=1, each tablet's first peer RPC failure triggers eviction.
        // Table has BUCKETS 2 → 2 tablets. One query is enough; run 2 to be safe in case
        // tablet distribution causes one tablet to be missed by a single query.
        for (int i = 0; i < 2; i++) {
            sql "SELECT * FROM ${tbl} ORDER BY k1" // peer RPC fails → S3 fallback
            sleep(500)
        }

        // Eviction is incremented by the peer bthread, which runs concurrently with the S3
        // winner path. The query may return (via S3) before the peer bthread has finished
        // its RPC timeout and incremented the counter. Use awaitUntil to avoid a race.
        awaitUntil(15) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_rpc_failure_eviction") > t3_evictionBefore
        }
        logger.info("peer_rpc_failure_eviction confirmed")

        // Also verify the gauge is back to 0 after eviction. This serves as a regression check
        // for the fix that moved g_balance_tablet_be_mapping_size << 1 into the singleflight
        // block (before operator[] creates the placeholder), ensuring increment/decrement are
        // balanced and the gauge no longer underflows to UINT64_MAX - N.
        assertEquals(0L,
            getBrpcMetrics(beB.Host, beB.BrpcPort, "balance_tablet_be_mapping_size"),
            "balance_tablet_be_mapping_size should be 0 after all candidates evicted")

        // Queries should still succeed via S3 fallback.
        def t3_result = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertTrue(t3_result.size() > 0, "S3 fallback should return correct data after eviction")

        // Restart CG-A BE before T4.
        cluster.startBackends(beAIndex)
        sleep(5000)
        logger.info("PASS")

        // ==================== Candidate expiry via cleanup daemon ====================
        logger.info("=== candidate expiry via cleanup daemon ===")

        // CG-A BE is running again (restarted at end of T3).
        // Insert fresh rows into CG-A that CG-B has NEVER cached.
        // After T3, CG-B's local file cache already has all old data; querying the same data
        // would be a local hit and never reach the peer path, so lazy fetch would not fire.
        // Fresh uncached blocks force CG-B into the peer path → candidates empty (T3 evicted
        // them) → lazy fetch fires → candidates re-populated.
        insertAndWarmCGA(4)

        def t4_lazyFetchBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success")
        def t4_expiryBefore    = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_candidate_expiry_eviction")

        sql "SELECT * FROM ${tbl} ORDER BY k1" // new uncached blocks → peer path → lazy fetch
        awaitUntil(30) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > t4_lazyFetchBefore
        }
        logger.info("candidates re-populated via lazy fetch")

        // Wait for peer_candidate_expiry_s=60s + cleanup_interval=5s to fire.
        // Total wait: 70 s to be safe.
        logger.info("sleeping 70s for candidate expiry (expiry_s=60, cleanup_interval=5)...")
        sleep(70 * 1000)

        // peer_candidate_expiry_eviction increasing is sufficient proof that the cleanup daemon
        // ran and removed the expired candidates.
        // Do NOT assert balance_tablet_be_mapping_size == 0 (bvar::Adder<uint64_t> underflows).
        // Do NOT verify via lazy fetch re-trigger after expiry: CG-B's local cache now has the
        // fresh data, so subsequent queries are local hits and never reach the peer path.
        awaitUntil(15) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_candidate_expiry_eviction") > t4_expiryBefore
        }
        logger.info("PASS")

        // ==================== cache miss rotates candidate (Issue 3) ====================
        // After T4, CGB's candidates have all expired (expiry_s=60). Fresh setup needed.
        //
        // When CGA is running but its file cache is EMPTY (blocks not yet cached) and
        // peer_cache_fill_compute_group_id is not set (this suite), CGA returns NOT_FOUND
        // for the missing blocks.  The client (CGB) should call rotate_peer_candidate_on_cache_miss
        // rather than incrementing the RPC-failure eviction counter — the peer_candidate_rotate
        // bvar must increase, and the candidate must NOT be evicted.
        logger.info("=== cache miss (NOT_FOUND) rotates candidate from CG-A to CG-C on CG-B ===")

        // Step 1: Add a second remote compute group so rotate has a real next candidate.
        cluster.addBackend(1, CG_C)
        awaitUntil(30) {
            findBackendsByClusterName(CG_C).size() == 1
        }
        def beC = getSingleBackendByClusterName(CG_C)
        logger.info("CG-C BE: host={} brpcPort={}", beC.Host, beC.BrpcPort)

        // Step 2: Prime CG-C once before CG-B's lazy fetch so FE records a primary backend for
        // CG-C as well. Without this, the first lazy fetch may still return only CG-A, which
        // would reduce rotate to a single-candidate noop.
        sql "use @${CG_C}"
        sql "SELECT * FROM ${tbl} WHERE k1 < 800 ORDER BY k1"
        sleep(1000)

        // Step 3: Trigger a fresh lazy fetch after T4 expiry so CG-B learns both remote
        // compute groups (CG-A + CG-C) as candidates.
        sql "use @${CG_A}"
        sql "INSERT INTO ${tbl} VALUES (800, 'fetch1'), (801, 'fetch2')"
        sql "SELECT * FROM ${tbl} WHERE k1 >= 800 AND k1 < 810 ORDER BY k1"
        sleep(1000)

        def t5_lazyFetchBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success")
        sql "use @${CG_B}"
        sql "SELECT * FROM ${tbl} WHERE k1 >= 800 AND k1 < 810 ORDER BY k1"
        awaitUntil(30) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > t5_lazyFetchBefore
        }
        logger.info("lazy fetch refreshed candidates; CG-B should now know both CG-A and CG-C")

        // Step 4: Seed CG-A as the preferred remote CG using a fresh rowset that CG-B has never
        // cached. This ensures get_peer_candidates() stable-partitions CG-A to the front before
        // the rotate verification below.
        sql "use @${CG_A}"
        sql "INSERT INTO ${tbl} VALUES (820, 'pref1'), (821, 'pref2')"
        sql "SELECT * FROM ${tbl} WHERE k1 >= 820 AND k1 < 900 ORDER BY k1"
        sleep(1000)

        def t5_peerWinSeedBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")
        sql "use @${CG_B}"
        sql "SELECT * FROM ${tbl} WHERE k1 >= 820 AND k1 < 900 ORDER BY k1"
        awaitUntil(10) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > t5_peerWinSeedBefore
        }
        logger.info("CG-A became the preferred remote CG for the rotate verification")

        // Step 5: Insert rotate-test rows, keep CG-A and CG-C warm, but leave CG-B cold.
        sql "use @${CG_A}"
        sql "INSERT INTO ${tbl} VALUES (900, 'rotate1'), (901, 'rotate2')"
        sql "SELECT * FROM ${tbl} WHERE k1 >= 900 ORDER BY k1"  // keep CG-A warm
        sql "use @${CG_C}"
        sql "SELECT * FROM ${tbl} WHERE k1 >= 900 ORDER BY k1"  // warm CG-C only
        sleep(1000)

        // Step 6: Restart CGA with cache deleted → all blocks are EMPTY after restart.
        // trigger_peer_server_fill is NOT invoked here because peer_cache_fill_compute_group_id
        // is empty (not configured in this suite), so request_cache_fill=false → server returns
        // NOT_FOUND for EMPTY blocks → CGB rotate path fires.
        cluster.stopBackends(beAIndex)
        sleep(2000)
        def beA_node5 = cluster.getBeByBackendId(beA.BackendId.toLong())
        def cacheDir5 = new File("${beA_node5.path}/storage/file_cache")
        logger.info("Deleting CGA file cache at: {}", cacheDir5.absolutePath)
        if (cacheDir5.exists()) {
            cacheDir5.deleteDir()
            logger.info("CGA file cache deleted — blocks will be EMPTY after restart")
        }
        cluster.startBackends(beAIndex)
        sleep(5000)
        logger.info("CGA restarted with empty cache while CG-C remains warm")

        // Step 7: CGB queries (900, 901) — CGB has NO local cache for these rows.
        // Peer bthread first tries the preferred CG-A → EMPTY + request_fill=false → NOT_FOUND
        // → rotate_peer_candidate_on_cache_miss. The same peer loop then advances to CG-C,
        // which still has these blocks warm, so this is a real rotate-to-next-candidate path.
        def t5_rotateBefore   = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_candidate_rotate")
        def t5_evictionBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_rpc_failure_eviction")
        def t5_peerWinBefore  = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")
        sql "use @${CG_B}"
        sql "SELECT * FROM ${tbl} WHERE k1 >= 900 ORDER BY k1"

        awaitUntil(15) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_candidate_rotate") > t5_rotateBefore
        }
        awaitUntil(15) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > t5_peerWinBefore
        }
        logger.info("peer_candidate_rotate increased and peer still won via the next candidate")

        // Key assertion: rotate occurred but eviction counter did NOT move.
        assertEquals(t5_evictionBefore,
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_rpc_failure_eviction"),
            "NOT_FOUND (cache miss) must NOT increment peer_rpc_failure_eviction")

        // Data must still be readable after rotate.
        def t5_result = sql "SELECT * FROM ${tbl} WHERE k1 >= 900 ORDER BY k1"
        assertEquals(2, t5_result.size(), "rotate path must return rotate-test rows")

        // Restart CGA before end of suite (it was started in step 4; should still be running).
        logger.info("PASS")
    }
}
