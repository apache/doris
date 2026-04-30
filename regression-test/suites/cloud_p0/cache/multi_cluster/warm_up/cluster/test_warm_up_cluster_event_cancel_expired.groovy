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
import groovy.json.JsonSlurper

// Covers a two-part bug on the event-driven warm-up path:
//   1. BE side (cloud_warm_up_manager.cpp): `st.is<CANCELED>()` used the
//      proto enum value PCacheStatus::CANCELED=9 instead of
//      ErrorCode::CANCELLED=1, so BE never cleared a cancelled job from
//      `_tablet_replica_cache`.
//   2. FE side (FrontendServiceImpl.getTabletReplicaInfos): when the job
//      had been removed from `cloudWarmUpJobs` (after
//      history_cloud_warm_up_job_keep_max_second), the branch still
//      called `job.getJobId()` on a null reference, throwing NPE to BE.
//
// After the fix, once a warm-up job is cancelled BE must drop it from
// its cache on the next RPC (via the CANCELLED TStatus), so that later
// expiry removal on FE never receives follow-up requests with the dead
// job_id and no NPE path is exercised.
suite('test_warm_up_cluster_event_cancel_expired', 'docker') {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        // Keep expiry small so FE removes the cancelled job quickly.
        'history_cloud_warm_up_job_keep_max_second=30',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
    ]
    options.cloudMode = true

    def clearFileCache = { ip, port ->
        def url = "http://${ip}:${port}/api/file_cache?op=clear&sync=true"
        def response = new URL(url).text
        def json = new JsonSlurper().parseText(response)
        if (json.status != "OK") {
            throw new RuntimeException("Clear cache on ${ip}:${port} failed: ${json.status}")
        }
    }

    def clearFileCacheOnAllBackends = {
        def backends = sql """SHOW BACKENDS"""
        for (be in backends) {
            clearFileCache(be[1], be[4])
        }
        sleep(10000)
    }

    def getBrpcMetrics = { ip, port, name ->
        def url = "http://${ip}:${port}/brpc_metrics"
        if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
            url = url.replace("http://", "https://") + " --cert " + context.config.otherConfigs.get("trustCert") + " --cacert " + context.config.otherConfigs.get("trustCACert") + " --key " + context.config.otherConfigs.get("trustCAKey")
        }
        def metrics = new URL(url).text
        def matcher = metrics =~ ~"${name}\\s+(\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        } else {
            throw new RuntimeException("${name} not found for ${ip}:${port}")
        }
    }

    def getSkippedRowsetSum = { cluster ->
        def backends = sql """SHOW BACKENDS"""
        def cluster_bes = backends.findAll {
            it[19].contains("""\"compute_group_name\" : \"${cluster}\"""")
        }
        long sum = 0
        for (be in cluster_bes) {
            sum += getBrpcMetrics(be[1], be[5], "file_cache_event_driven_warm_up_skipped_rowset_num")
        }
        return sum
    }

    docker(options) {
        def clusterSrc = "warmup_source"
        def clusterDst = "warmup_target"

        cluster.addBackend(1, clusterSrc)
        cluster.addBackend(1, clusterDst)

        sql """use @${clusterSrc}"""
        sql """CREATE TABLE IF NOT EXISTS t_exp (
                   id INT, v STRING
               ) DUPLICATE KEY(id)
                 DISTRIBUTED BY HASH(id) BUCKETS 3
                 PROPERTIES ("file_cache_ttl_seconds" = "3600")"""

        // 1. Start event-driven LOAD warm-up job; this is the only mode
        //    that populates `_tablet_replica_cache[jobId]` on BE.
        def jobRows = sql """
            WARM UP CLUSTER ${clusterDst} WITH CLUSTER ${clusterSrc}
            PROPERTIES (
                "sync_mode"  = "event_driven",
                "sync_event" = "load"
            )
        """
        def jobId = jobRows[0][0]
        logger.info("event-driven warm-up jobId=${jobId}")
        clearFileCacheOnAllBackends()
        sleep(15000)

        // 2. Drive some loads so BE caches the job_id in _tablet_replica_cache
        //    (we don't care about the warmed data itself for this test).
        for (int i = 0; i < 20; i++) {
            sql """INSERT INTO t_exp VALUES (${i}, 'x')"""
        }
        sleep(5000)

        // 3. Cancel the job. After the fix, BE sees TStatus.CANCELLED on
        //    the next get_replica_info and drops the job from
        //    _tablet_replica_cache; before the fix (CANCELED typo) it
        //    would keep the entry.
        sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
        def st = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
        assertEquals("CANCELLED", st[0][3])

        // 4. One more batch so BE actually sees the CANCELLED status
        //    and (with the fix) purges its cache entry.
        for (int i = 0; i < 20; i++) {
            sql """INSERT INTO t_exp VALUES (${100 + i}, 'y')"""
        }
        sleep(5000)

        // 5. Baseline for the skipped-rowset counter. After BE has
        //    cleaned its cache, subsequent warm_up_rowset calls return
        //    early (empty replicas -> "skipping rowset") and bump this
        //    counter on every commit. If the typo is unfixed the counter
        //    stays flat because BE keeps calling FE.
        def skippedBaseline = getSkippedRowsetSum(clusterDst)
        logger.info("skipped_rowset baseline=${skippedBaseline}")

        // 6. Wait past history_cloud_warm_up_job_keep_max_second plus one
        //    JobDaemon cycle (~20s, CYCLE_COUNT_TO_CHECK_EXPIRE=20 ticks
        //    at 1s each) so FE removes the cancelled job from its map.
        logger.info("waiting for FE to expire+remove cancelled job")
        def removed = false
        for (int i = 0; i < 60; i++) {
            def rows = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            if (rows.isEmpty()) {
                removed = true
                logger.info("job ${jobId} removed from FE after ${i}s")
                break
            }
            sleep(1000)
        }
        assertTrue(removed, "FE should have removed expired warm-up job")

        // 7. After FE removal, run more loads. Buggy code path:
        //    BE still has job_id in _tablet_replica_cache -> RPC to FE ->
        //    FE's LOG.info("...", job.getJobId(), ...) NPE's on
        //    `job == null` -> BE falls into the 2s thrift exception
        //    sleep *inside* CloudWarmUpManager::_mtx, serialising every
        //    commit_rowset and blowing up heavy_work_pool.
        //
        //    Fixed code path: BE cache already cleaned in step 4, so
        //    warm_up_rowset takes the empty-replicas fast path, loads
        //    stay fast, and FE never gets called with the dead job_id.
        def t0 = System.currentTimeMillis()
        for (int i = 0; i < 20; i++) {
            sql """INSERT INTO t_exp VALUES (${200 + i}, 'z')"""
        }
        def elapsedMs = System.currentTimeMillis() - t0
        logger.info("20 INSERTs after FE removal took ${elapsedMs}ms")

        // 20 INSERTs, each commit would cost ~2s of serialised sleep in
        // the buggy case (>= 40s). Threshold of 30s is a generous
        // upper bound that the fixed path comfortably meets but the
        // buggy path cannot.
        assertTrue(elapsedMs < 30_000,
                "post-removal inserts should not be blocked by NPE sleeps, " +
                "took ${elapsedMs}ms")

        // 8. On the fixed path every commit short-circuits through
        //    g_file_cache_event_driven_warm_up_skipped_rowset_num.
        //    We expect it to grow; on the buggy path it would be flat
        //    since BE never stopped pursuing FE replicas.
        def skippedAfter = getSkippedRowsetSum(clusterDst)
        logger.info("skipped_rowset after=${skippedAfter}")
        assertTrue(skippedAfter > skippedBaseline,
                "BE should skip warm_up_rowset for tablets after cancel+expire, " +
                "baseline=${skippedBaseline} after=${skippedAfter}")

        sql """DROP TABLE IF EXISTS t_exp"""
    }
}
