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

// End-to-end peer reads on the async file cache write path.
//
// Topology: 1 FE + compute_cluster (CG-A, 1 BE) + cross_cg_peer_cluster (CG-B, 1 BE). Both BEs
// use async cache writes, peer read with the peer/S3 race, and the same cache block size. Peer
// server-side fill is not enabled, so a block missing from CG-A's cache fails the peer request
// instead of being filled from S3.
//
// T0 warm CG-A until a query reads nothing from S3 (CG-A serves peers only from persisted blocks).
// T1 first CG-B query: no candidate yet, so it reads S3 and starts the candidate fetch from FE.
// T2 CG-B with an empty cache reads the misses from CG-A through peer and submits async writes.
// T3 after CG-B's async writes finish, the same query is served from CG-B's cache only.
// T4 partially warmed CG-B cache, then a full scan: result must stay correct.
// T5 peer read disabled at runtime on CG-B: no peer read, storage only, result unchanged.

import org.apache.doris.regression.suite.ClusterOptions

suite('test_async_file_cache_write_peer', 'docker') {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'heartbeat_interval_second=1',
        'auto_check_statistics_in_minutes=60',
        'rehash_tablet_after_be_dead_seconds=3600',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'enable_file_cache=true',
        'enable_file_cache_write_from_s3_file_writer=false',
        'enable_async_file_cache_write=true',
        'enable_async_file_cache_write_inflight_write_buffer_index=true',
        'enable_read_cache_file_directly=false',
        'disable_storage_page_cache=true',
        'disable_segment_cache=true',
        'enable_cache_read_from_peer=true',
        'enable_peer_s3_race=true',
        // A 50 ms head start lets a warm peer answer before the S3 leg is submitted.
        'peer_race_hedge_delay_ms=50',
        'enable_packed_file=false',
        // Peer client and server must agree on the cache block size.
        'file_cache_each_block_size=131072',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'file_cache_exit_disk_resource_limit_mode_percent=98',
        'file_cache_enter_need_evict_cache_in_advance_percent=99',
        'file_cache_exit_need_evict_cache_in_advance_percent=98',
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    docker(options) {
        final String CG_A = 'compute_cluster'
        final String CG_B = 'cross_cg_peer_cluster'

        cluster.addBackend(1, CG_B)
        awaitUntil(60) {
            def backends = sql_return_maparray('show backends')
            backends.size() == 2 && backends.every { it.Alive.toString() == 'true' }
        }
        def clusterBe = { String clusterName ->
            def be = sql_return_maparray('show backends').find { it.Tag.contains(clusterName) }
            assertTrue(be != null, "backend for ${clusterName} should exist")
            be
        }
        def beA = clusterBe(CG_A)
        def beB = clusterBe(CG_B)

        // Sum a bvar across lines whose name is `name`, optionally behind a prefix such as the
        // per-disk prefix of the async cache write metrics.
        def metric = { be, String name ->
            def text = new URL("http://${be.Host}:${be.BrpcPort}/brpc_metrics").text
            long total = 0
            text.eachLine { String line ->
                def matcher = line =~ ~"^(?:\\S*_)?${name}\\s+([0-9]+)\\s*\$"
                if (matcher.matches()) {
                    total += matcher[0][1] as long
                }
            }
            total
        }
        def snapshot = { be, List<String> names -> names.collectEntries { [(it): metric(be, it)] } }
        def delta = { be, Map before, String name -> metric(be, name) - before[name] }

        def clearFileCache = { be ->
            def (code, out, err) = curl('GET',
                    "http://${be.Host}:${be.HttpPort}/api/file_cache?op=clear&sync=true")
            assertTrue(code == 0 && out.contains('OK'), "clear file cache failed, out=${out}, err=${err}")
        }
        def setBeConfig = { be, String name, String value ->
            def (code, out, err) = curl('POST',
                    "http://${be.Host}:${be.HttpPort}/api/update_config?${name}=${value}")
            assertTrue(code == 0 && out.contains('OK'),
                    "update_config ${name}=${value} failed, out=${out}, err=${err}")
        }
        // Every queued, running or inflight async write task holds a buffer charged to this
        // per-disk gauge, so it returns to zero only after all of them finished.
        def waitAsyncWritesDone = { be ->
            awaitUntil(60, 0.1) { metric(be, 'async_cache_write_buffer_memory_bytes') == 0L }
        }

        final String S3_READ = 'cached_remote_reader_s3_read'
        final String PEER_READ = 'cached_remote_reader_peer_read'
        final String PEER_RPC = 'peer_cache_reader_read_at'
        final String PEER_SUCC = 'peer_cache_reader_succ_counter'
        final String PEER_BYTES = 'peer_cache_reader_bytes_read'
        final String PEER_WIN = 'peer_race_peer_win'
        final String SUBMITTED = 'async_cache_write_submitted_task_count'
        final String PROBE_HIT = 'cached_remote_file_reader_probe_hit_downloaded_count'
        final String PROBE_MISS = 'cached_remote_file_reader_probe_miss_count'
        final String ELIGIBLE = 'cached_remote_file_reader_async_peer_eligible_span_count'
        final String NO_CANDIDATE = 'cached_remote_file_reader_async_peer_no_candidate_span_count'
        final String LAZY_FETCH_SUCCESS = 'peer_lazy_fetch_success'
        def names = [S3_READ, PEER_READ, PEER_RPC, PEER_SUCC, PEER_BYTES, PEER_WIN, SUBMITTED,
                     PROBE_HIT, PROBE_MISS, ELIGIBLE, NO_CANDIDATE, LAZY_FETCH_SUCCESS]

        final String QUERY = """
            SELECT count(*), sum(k), sum(length(v)), max(v)
            FROM test_async_file_cache_write_peer_tbl
        """

        sql 'SET enable_sql_cache = false'
        sql 'SET enable_query_cache = false'
        sql 'SET enable_push_down_no_group_agg = false'

        sql "use @${CG_A}"
        sql 'DROP TABLE IF EXISTS test_async_file_cache_write_peer_tbl'
        sql """
            CREATE TABLE test_async_file_cache_write_peer_tbl (
                k BIGINT NOT NULL,
                v STRING NOT NULL
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """
        sql """
            INSERT INTO test_async_file_cache_write_peer_tbl
            SELECT number, repeat(md5(cast(number AS STRING)), 4)
            FROM numbers("number" = "200000")
        """
        sql 'SYNC'

        // ---- T0: CG-A cache holds every block the query reads ----
        order_qt_t0_cga QUERY
        def resultA = sql QUERY
        boolean cgaWarm = false
        Map t0Before = null
        for (int round = 0; round < 5 && !cgaWarm; ++round) {
            waitAsyncWritesDone(beA)
            t0Before = snapshot(beA, names)
            assertEquals(resultA, sql(QUERY))
            cgaWarm = delta(beA, t0Before, S3_READ) == 0 && delta(beA, t0Before, PROBE_MISS) == 0
            logger.info("T0 round ${round}: s3 delta=${delta(beA, t0Before, S3_READ)}, " +
                    "probe miss delta=${delta(beA, t0Before, PROBE_MISS)}")
        }
        assertTrue(cgaWarm, 'T0: CG-A should serve the query from its own cache')
        assertTrue(delta(beA, t0Before, PROBE_HIT) > 0, 'T0: CG-A should hit downloaded blocks')

        // ---- T1: the first CG-B query has no candidate and fetches candidates from FE ----
        sql "use @${CG_B}"
        def t1Before = snapshot(beB, names)
        assertEquals(resultA, sql(QUERY))
        awaitUntil(30) { delta(beB, t1Before, LAZY_FETCH_SUCCESS) > 0 }
        waitAsyncWritesDone(beB)

        // ---- T2: empty CG-B cache, misses are read from CG-A through peer ----
        clearFileCache(beB)
        def t2Before = snapshot(beB, names)
        order_qt_t2_cgb_peer QUERY
        def t2Deltas = names.collectEntries { [(it): delta(beB, t2Before, it)] }
        logger.info("T2 CG-B deltas: ${t2Deltas}")
        assertTrue(t2Deltas[PEER_SUCC] > 0, "T2: peer reads should succeed, ${t2Deltas}")
        assertTrue(t2Deltas[PEER_BYTES] > 0, "T2: peer should return bytes, ${t2Deltas}")
        assertTrue(t2Deltas[PEER_WIN] > 0, "T2: peer should win the race, ${t2Deltas}")
        assertTrue(t2Deltas[SUBMITTED] > 0, "T2: peer data should be submitted as async writes, ${t2Deltas}")
        assertTrue(t2Deltas[ELIGIBLE] > 0, "T2: async spans should be peer eligible, ${t2Deltas}")
        assertEquals(0L, t2Deltas[NO_CANDIDATE], "T2: CG-B should have candidates, ${t2Deltas}")
        assertEquals(resultA, sql(QUERY))

        // ---- T3: the peer payload was persisted into CG-B's cache ----
        waitAsyncWritesDone(beB)
        def t3Before = snapshot(beB, names)
        order_qt_t3_cgb_cached QUERY
        def t3Deltas = names.collectEntries { [(it): delta(beB, t3Before, it)] }
        logger.info("T3 CG-B deltas: ${t3Deltas}")
        assertTrue(t3Deltas[PROBE_HIT] > 0, "T3: CG-B should hit downloaded blocks, ${t3Deltas}")
        assertEquals(0L, t3Deltas[PEER_READ], "T3: no peer read expected, ${t3Deltas}")
        assertEquals(0L, t3Deltas[S3_READ], "T3: no storage read expected, ${t3Deltas}")

        // ---- T4: partially warmed CG-B cache, then a full scan ----
        // A sparse peer read needs one read_at covering a cached block between two misses. Segment
        // reads are page sized, and a page's middle blocks hold only that page's bytes, so SQL
        // cannot cache such a middle block alone; the sparse read is covered by BE unit tests.
        clearFileCache(beB)
        sql '''
            SELECT count(*), sum(length(v)) FROM test_async_file_cache_write_peer_tbl
            WHERE k >= 80000 AND k < 120000
        '''
        waitAsyncWritesDone(beB)
        def t4Before = snapshot(beB, names)
        order_qt_t4_cgb_partial QUERY
        logger.info("T4 CG-B deltas: ${names.collectEntries { [(it): delta(beB, t4Before, it)] }}")
        assertEquals(resultA, sql(QUERY))
        waitAsyncWritesDone(beB)

        // ---- T5: peer read disabled at runtime on CG-B ----
        setBeConfig(beB, 'enable_cache_read_from_peer', 'false')
        try {
            clearFileCache(beB)
            def t5Before = snapshot(beB, names)
            order_qt_t5_cgb_no_peer QUERY
            def t5Deltas = names.collectEntries { [(it): delta(beB, t5Before, it)] }
            logger.info("T5 CG-B deltas: ${t5Deltas}")
            assertEquals(0L, t5Deltas[PEER_READ], "T5: no peer read expected, ${t5Deltas}")
            assertEquals(0L, t5Deltas[PEER_RPC], "T5: no peer RPC expected, ${t5Deltas}")
            assertEquals(0L, t5Deltas[ELIGIBLE], "T5: no span should be peer eligible, ${t5Deltas}")
            assertTrue(t5Deltas[S3_READ] > 0, "T5: storage should serve the misses, ${t5Deltas}")
            assertTrue(t5Deltas[SUBMITTED] > 0, "T5: async writes should still be submitted, ${t5Deltas}")
            assertEquals(resultA, sql(QUERY))
            waitAsyncWritesDone(beB)
        } finally {
            setBeConfig(beB, 'enable_cache_read_from_peer', 'true')
        }
    }
}
