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

// Cross compute group peer read server-side fill test:
// CG-A BE cache miss → server pulls from S3 (pull-through fill) → returns data to CG-B.
//      Second query: CG-A already cached → peer hits directly. (P1)
// max_concurrent_peer_server_fills=0 → all fill requests rejected immediately → (Issue 5)
//      peer_server_fill_rejected counter increases; query still succeeds via S3 fallback.
//
// Topology: 1 FE + CG-A (1 BE, "compute_cluster") + CG-B (1 BE, "cross_cg_peer_cluster")
// CG-A BE does NOT warm its cache before the first cross-CG query, so the server-side fill path
// is exercised.
//
// Note: peer_cache_fill_compute_group_id must be set to CG-A's cluster_id. In the docker
// environment the default cluster id is "compute_cluster_id".

import org.apache.doris.regression.suite.ClusterOptions

suite('test_cross_cg_server_fill', 'docker') {
    if (!isCloudMode()) {
        return
    }

    // CG-A cluster id in the docker environment.
    final String CG_A_CLUSTER_ID = "compute_cluster_id"

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
        'enable_peer_server_cache_fill=true',
        // Give S3 fill enough time in docker (latency can be high).
        'peer_server_cache_fill_timeout_ms=6000',
        // RPC timeout must exceed fill timeout.
        // Designate CG-A as the fill compute group so client sets request_cache_fill=true only for CG-A.
        "peer_cache_fill_compute_group_id=${CG_A_CLUSTER_ID}",
        // Packed files would obscure per-segment file-cache inspection.
        'enable_packed_file=false',
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

    def updateBeConfig = { be, key, value ->
        waitForBeHttpReady(be, key)
        def (code, out, err) = curl("POST",
            "http://${be.Host}:${be.HttpPort}/api/update_config?${key}=${value}")
        assertEquals(0, code, "failed to set ${key}=${value} on ${be.Host}:${be.HttpPort}: ${err}")
        assertTrue(out.contains("OK"),
            "failed to set ${key}=${value} on ${be.Host}:${be.HttpPort}: ${out}")
    }

    docker(options) {
        def tbl = "test_cross_cg_server_fill_tbl"

        // ---- Setup: Add CG-B ----
        cluster.addBackend(1, "cross_cg_peer_cluster")
        sleep(5000)

        def backends = sql_return_maparray('show backends')
        assert backends.size() == 2 : "Expected 2 backends"
        def beA = backends.get(0)
        def beB = backends.get(1)
        logger.info("CG-A BE (fill server): host={} brpcPort={}", beA.Host, beA.BrpcPort)
        logger.info("CG-B BE (requester):   host={} brpcPort={}", beB.Host, beB.BrpcPort)

        // ---- Setup: Create table and insert data in CG-A. Do NOT warm CG-A cache. ----
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
        sql "INSERT INTO ${tbl} VALUES (10, 'foo'), (20, 'bar')"
        sql "INSERT INTO ${tbl} VALUES (30, 'baz'), (40, 'qux')"
        // Intentionally skip the CG-A warming step so CG-A's file cache is empty.
        // CG-B query will trigger server-side fill on CG-A.
        // ---- Step 1: Insert a fill-test rowset into CG-A BEFORE CG-B knows about CG-A ----
        // CG-A's S3FileWriter will cache these blocks during write (DOWNLOADED on CG-A).
        // CG-B has never seen this data → no local cache for it.
        sql "use @compute_cluster"
        sql "INSERT INTO ${tbl} VALUES (50, 'fill1'), (60, 'fill2')"
        // Warm CG-A's READ cache (the write-path cache from S3FileWriter is already populated,
        // so this query is a local hit on CG-A — just ensures CG-A has the data accessible).
        sql "SELECT * FROM ${tbl} ORDER BY k1"
        sleep(1000)

        // ---- Step 2: Trigger lazy FE fetch from CG-B for the INITIAL rows only ----
        // Run a SELECT before CG-A has (50,60) committed to CG-B's view... actually CG-B
        // will read ALL rows. Insert (50,60) AFTER this cold miss to keep CG-B uncached.
        // Restructure: trigger lazy fetch with existing rows, then insert fill-test rows.
        //
        // Actually, we inserted (50,60) first. The cold miss from CG-B will read ALL rows
        // including (50,60) → CG-B caches them → can't test fill for (50,60).
        //
        // Correct order:
        //   1. CG-B cold miss on initial rows (10,20,30,40) → lazy fetch, CGB caches initial
        //   2. Insert fill-test rows (50,60) on CG-A → CGA write-caches them, CGB never reads them
        //   3. Restart CGA → CGA file cache cleared (including write-cached (50,60))
        //   4. CGB queries ALL: (10,20,30,40) = local hit; (50,60) = remote needed; CGA EMPTY → fill!

        // Undo: we already inserted (50,60) above. Drop and recreate to get the right order.
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
        sql "INSERT INTO ${tbl} VALUES (10, 'foo'), (20, 'bar')"
        sql "INSERT INTO ${tbl} VALUES (30, 'baz'), (40, 'qux')"
        // Warm CG-A cache for initial rows.
        sql "SELECT * FROM ${tbl} ORDER BY k1"
        sleep(1000)

        // CG-B cold miss on initial rows (10,20,30,40) → triggers lazy fetch, CGB caches them.
        sql "use @cross_cg_peer_cluster"
        sql "SELECT * FROM ${tbl} ORDER BY k1"
        awaitUntil(30) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > 0
        }
        logger.info("CG-B now has CG-A BE as candidate; CG-B has (10,20,30,40) cached")

        // ---- Step 3: Insert fill-test rows (50,60) into CG-A ----
        // CG-A's S3FileWriter caches them (DOWNLOADED on CG-A). CG-B has NEVER read them.
        sql "use @compute_cluster"
        sql "INSERT INTO ${tbl} VALUES (50, 'fill1'), (60, 'fill2')"
        sleep(1000)

        // ---- Step 4: Restart CG-A and EXPLICITLY delete its disk file cache ----
        // S3FileWriter populates CGA's local file cache during INSERT (write-time caching).
        // The file cache is disk-based and survives a BE process restart — blocks come back as
        // DOWNLOADED, not EMPTY. trigger_peer_server_fill is only called when the block state
        // is EMPTY. We must delete the cache directory after stopping the process so CGA truly
        // starts with empty blocks after restart.
        cluster.stopBackends(1)
        sleep(2000)

        def beA_node = cluster.getBeByBackendId(beA.BackendId.toLong())
        def cacheDir = new File("${beA_node.path}/storage/file_cache")
        logger.info("Deleting CGA file cache at: {}", cacheDir.absolutePath)
        if (cacheDir.exists()) {
            cacheDir.deleteDir()
            logger.info("CGA file cache deleted — blocks will be EMPTY after restart")
        } else {
            logger.warn("CGA file cache directory not found at {}", cacheDir.absolutePath)
        }

        cluster.startBackends(1)
        sleep(5000)
        waitForBeHttpReady(beA, "enable_peer_server_cache_fill")
        logger.info("CG-A restarted — file cache cleared, (50,60) blocks now EMPTY on CG-A")

        // ==================== Server-side pull-through fill ====================
        logger.info("=== server-side fill (CG-A cache miss → S3 pull-through) ===")

        def t9_fillReqBefore  = getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_requested")
        def t9_fillSuccBefore = getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_success")

        // CG-B queries ALL rows:
        //   (10,20,30,40): CG-B local cache hit → no remote access
        //   (50,60): CG-B has NO local cache; CG-A has EMPTY blocks (cleared by restart)
        //            → winner race → peer bthread → RPC to CG-A with request_cache_fill=true
        //            → CG-A EMPTY → enable_peer_server_cache_fill=true → trigger_peer_server_fill
        //            → CG-A pulls from S3, returns data → peer_server_fill_requested++
        sql "use @cross_cg_peer_cluster"
        def t9_r1 = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertEquals(6, t9_r1.size(), "first query should return all 6 rows")

        // Verify that CG-A's server-side fill was triggered.
        assertTrue(
            getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_requested") > t9_fillReqBefore,
            "peer_server_fill_requested should increase on CG-A after first cross-CG query")

        // ---- Second query: verify peer wins now that CG-A has cached the data ----
        // Insert another fresh rowset so CG-B has no cache for the new blocks.
        sql "use @compute_cluster"
        sql "INSERT INTO ${tbl} VALUES (70, 'new3'), (80, 'new4')"
        sql "SELECT * FROM ${tbl} ORDER BY k1"  // warm CG-A cache
        sleep(1000)
        sql "use @cross_cg_peer_cluster"
        def t9_fillSuccBeforeSecondQuery = getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_success")

        def t9_peerWinBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")
        def t9_r2 = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertEquals(8, t9_r2.size(), "second query should return 8 rows")
        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > t9_peerWinBefore,
            "peer should win race on second query (CG-A has data cached)")
        // (70,80) blocks are already DOWNLOADED on CG-A — no server fill should be triggered.
        assertEquals(t9_fillSuccBeforeSecondQuery,
            getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_success"),
            "peer_server_fill_success must NOT increase for (70,80) — CG-A already has them cached")

        // CG-A fill success counter should be positive.
        assertTrue(
            getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_success") > t9_fillSuccBefore,
            "peer_server_fill_success should increase on CG-A")

        // ==================== fill concurrency limit (Issue 5) ====================
        // Set max_concurrent_peer_server_fills=0 on CG-A → every fill request is rejected
        // immediately (fill slot limit exceeded on the first fetch_add).
        // CGB issues a query for blocks that CGA has NOT cached, with request_cache_fill=true.
        // Expected: peer_server_fill_rejected increases; query succeeds via S3 winner fallback.
        logger.info("=== max_concurrent_peer_server_fills=0 → fill rejected (Issue 5) ===")

        // Insert T10-exclusive rows into CGA.  CGA's write-time caching populates them (DOWNLOADED).
        // Then delete CGA's file cache so those blocks become EMPTY after restart.
        sql "use @compute_cluster"
        sql "INSERT INTO ${tbl} VALUES (200, 'limit_test1'), (201, 'limit_test2')"
        sql "SELECT * FROM ${tbl} ORDER BY k1"  // warm CGA
        sleep(1000)

        // Restart CGA with cache deleted so (200, 201) blocks are EMPTY.
        cluster.stopBackends(1)
        sleep(2000)
        def beA_node10 = cluster.getBeByBackendId(beA.BackendId.toLong())
        def cacheDir10 = new File("${beA_node10.path}/storage/file_cache")
        if (cacheDir10.exists()) {
            cacheDir10.deleteDir()
            logger.info("CGA file cache deleted — blocks will be EMPTY after restart")
        }
        cluster.startBackends(1)
        sleep(5000)
        waitForBeHttpReady(beA, "max_concurrent_peer_server_fills")
        logger.info("CGA restarted with empty cache")

        // Dynamically set max_concurrent_peer_server_fills=0 on CGA so all fills are rejected.
        updateBeConfig(beA, "max_concurrent_peer_server_fills", "0")
        sleep(500)

        def t10_rejectedBefore = getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_rejected")

        // CGB queries (200, 201) — CGB has NO local cache; CGA EMPTY, fill rejected (limit=0).
        // Winner race: peer RPC → CGA EMPTY + fill rejected → NOT_FOUND / error; S3 fallback wins.
        sql "use @cross_cg_peer_cluster"
        def t10_result = sql "SELECT * FROM ${tbl} WHERE k1 IN (200, 201) ORDER BY k1"
        assertEquals(2, t10_result.size(), "S3 fallback must return limit-test rows")

        // Verify fill was rejected on CGA.
        assertTrue(
            getBrpcMetrics(beA.Host, beA.BrpcPort, "peer_server_fill_rejected") > t10_rejectedBefore,
            "peer_server_fill_rejected must increase when max_concurrent_peer_server_fills=0")

        // Restore the limit to the default.
        updateBeConfig(beA, "max_concurrent_peer_server_fills", "32")
        logger.info("PASS")
    }
}
