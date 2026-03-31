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

// cross-CG peer read works correctly for tables with inverted index (.dat + .idx files).
// CG-A warms cache for both .dat and .idx segments. CG-B lazy-fetch then reads via peer.
// Priority: P2
//
// Topology: 1 FE + CG-A (1 BE, "compute_cluster") + CG-B (1 BE, "cross_cg_peer_cluster")

import org.apache.doris.regression.suite.ClusterOptions

suite('test_cross_cg_inverted_index', 'docker') {
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
        def tbl = "test_cross_cg_inverted_index_tbl"

        // ---- Setup: Add CG-B ----
        cluster.addBackend(1, "cross_cg_peer_cluster")
        sleep(5000)

        def backends = sql_return_maparray('show backends')
        assert backends.size() == 2 : "Expected 2 backends"
        def beA = backends.get(0)
        def beB = backends.get(1)
        logger.info("CG-A BE: host={} brpcPort={}", beA.Host, beA.BrpcPort)
        logger.info("CG-B BE: host={} brpcPort={}", beB.Host, beB.BrpcPort)

        // ---- Create table with inverted index in CG-A ----
        sql "use @compute_cluster"
        sql "DROP TABLE IF EXISTS ${tbl}"
        sql """
            CREATE TABLE ${tbl} (
                id     BIGINT,
                title  VARCHAR(256),
                body   STRING,
                INDEX idx_body (body) USING INVERTED PROPERTIES("parser" = "english")
            )
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 2
            PROPERTIES ("replication_num" = "1")
        """
        sql """INSERT INTO ${tbl} VALUES
            (1, 'hello world',  'the quick brown fox jumps over the lazy dog'),
            (2, 'foo bar',      'pack my box with five dozen liquor jugs'),
            (3, 'test case',    'how vexingly quick daft zebras jump')
        """
        sql """INSERT INTO ${tbl} VALUES
            (4, 'more data',    'the five boxing wizards jump quickly'),
            (5, 'last row',     'sphinx of black quartz judge my vow')
        """

        // Warm CG-A cache (both .dat segment files and .idx inverted-index files).
        def warmResult = sql "SELECT * FROM ${tbl} ORDER BY id"
        assertEquals(5, warmResult.size(), "Setup: CG-A warm query should return 5 rows")
        sleep(1000)

        // ==================== Cross-CG peer read with inverted index ====================
        logger.info("=== cross-CG peer read with inverted index ===")

        // Switch to CG-B, trigger cold miss → lazy fetch.
        sql "use @cross_cg_peer_cluster"
        def t11_peerWinBefore = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win")

        // First query: cold miss → lazy fetch fires. This populates CG-B's local cache for the
        // existing rows and triggers async candidate registration for CG-A's BE.
        sql "SELECT * FROM ${tbl} ORDER BY id"
        awaitUntil(30) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_lazy_fetch_success") > 0
        }

        // Insert fresh rows into CG-A and warm CG-A cache (both .dat and .idx files).
        // CG-B has no local cache for these new blocks, so the MATCH_ALL query below will
        // enter the winner race and peer-read .dat/.idx blocks from CG-A.
        sql "use @compute_cluster"
        sql """INSERT INTO ${tbl} VALUES
            (6, 'extra row', 'the quick fox is back again'),
            (7, 'another',  'no matching keyword here')
        """
        sql "SELECT * FROM ${tbl} ORDER BY id"  // warm CG-A cache: .dat + .idx for new rows
        sleep(1000)
        sql "use @cross_cg_peer_cluster"

        // Second query: new blocks not in CG-B local cache; MATCH_ALL exercises .idx files via peer.
        def t11_result = sql """
            SELECT id, title FROM ${tbl}
            WHERE body MATCH_ALL 'quick'
            ORDER BY id
        """
        assertTrue(t11_result.size() > 0, "MATCH_ALL query should return rows")
        // Rows containing exact word 'quick': id=1 ("quick brown fox"), id=3 ("vexingly quick"),
        // id=6 ("the quick fox is back again"). Row 4 has "quickly" which is a different token.
        def ids = t11_result.collect { it[0] as long }
        assertTrue(ids.containsAll([1L, 3L, 6L]),
            "MATCH_ALL 'quick' should match rows 1, 3, 6; got ${ids}")

        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_race_peer_win") > t11_peerWinBefore,
            "peer_race_peer_win should increase (peer served .dat and/or .idx blocks)")
        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_cross_compute_group_read") > 0,
            "peer_cross_compute_group_read should be > 0")

        // Insert yet another fresh row with 'quick' into CG-A and warm CG-A, so the profile
        // query below has uncached blocks on CG-B and will trigger a cross-CG peer read.
        sql "use @compute_cluster"
        sql """INSERT INTO ${tbl} VALUES (8, 'profile row', 'quick brown profile test')"""
        sql "SELECT * FROM ${tbl} ORDER BY id"  // warm CG-A cache including new .idx blocks
        sleep(1000)
        sql "use @cross_cg_peer_cluster"

        // Profile check: CrossCGPeerIOTotal should reflect .dat + .idx reads.
        profile("cross_cg_inverted_index_profile") {
            sql "set enable_profile = true"
            sql "set profile_level = 2"
            run {
                sql """/* cross_cg_inverted_index */ SELECT id FROM ${tbl}
                       WHERE body MATCH_ALL 'quick' ORDER BY id"""
                sleep(500)
            }
            check { profileString, exception ->
                logger.info("profile snippet: {}", profileString.take(2000))
                def matcher = (profileString =~ /CrossCGPeerIOTotal:\s+(\d+)/)
                def total = 0
                while (matcher.find()) {
                    total += matcher.group(1).toInteger()
                }
                assertTrue(total > 0,
                    "CrossCGPeerIOTotal must be > 0 in profile for inverted-index query")
            }
        }
        logger.info("PASS")
    }
}
