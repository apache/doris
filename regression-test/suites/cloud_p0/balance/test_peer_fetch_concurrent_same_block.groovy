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

// Peer fetch concurrent same-block regression.
//
// Topology: 1 FE + CG-A (source BE) + CG-B (reader 1) + CG-C (reader 2). Both CG-B
// and CG-C have CG-A as a peer candidate. Fan-in from different reader BEs can
// produce concurrent peer RPCs on CG-A for the same block. The server should serve
// both requests instead of rejecting a duplicate in-flight block.

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

suite('test_peer_fetch_concurrent_same_block', 'docker') {
    if (!isCloudMode()) {
        return
    }

    final String CG_A_CLUSTER_ID = "compute_cluster_id"
    final String CG_A_CLUSTER = "compute_cluster"
    final String CG_B_CLUSTER = "cross_cg_peer_cluster_b"
    final String CG_C_CLUSTER = "cross_cg_peer_cluster_c"

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
        'peer_server_cache_fill_timeout_ms=6000',
        "peer_cache_fill_compute_group_id=${CG_A_CLUSTER_ID}",
        'enable_packed_file=false',
        'file_cache_each_block_size=131072',
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'file_cache_exit_disk_resource_limit_mode_percent=98',
        'file_cache_enter_need_evict_cache_in_advance_percent=99',
        'file_cache_exit_need_evict_cache_in_advance_percent=98',
        'peer_race_hedge_delay_ms=5000',
        // Raise the queue timeout well above the DBUG hold so the second RPC is
        // served concurrently rather than getting rejected by the queue guard.
        'peer_fetch_queue_timeout_ms=5000',
        'brpc_peer_fetch_pool_threads=4',
        'brpc_peer_fetch_pool_max_queue_size=64',
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true
    options.enableDebugPoints()

    def getHttpUrl = { host, port, path ->
        def url = "http://${host}:${port}${path}"
        if ((context.config.otherConfigs.get("enableTLS")?.toString()?.equalsIgnoreCase("true")) ?: false) {
            url = url.replace("http://", "https://") +
                " --cert " + context.config.otherConfigs.get("trustCert") +
                " --cacert " + context.config.otherConfigs.get("trustCACert") +
                " --key " + context.config.otherConfigs.get("trustCAKey")
        }
        return url
    }

    def getBrpcMetric = { host, port, name ->
        def metrics = new URL(getHttpUrl(host, port, "/brpc_metrics")).text
        def matcher = metrics =~ ~"${name}\\s+(-?\\d+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        }
        return null
    }

    def getRequiredBrpcMetric = { host, port, name ->
        def value = getBrpcMetric(host, port, name)
        assertTrue(value != null, "missing brpc metric ${name} on ${host}:${port}")
        return value
    }

    docker(options) {
        def tbl = "test_peer_fetch_concurrent_same_block_tbl"
        def nextK1 = 100

        def insertAndWarmCGA = { count ->
            def start = nextK1
            nextK1 += count
            def values = (start..<nextK1).collect { k -> "(${k}, 'row${k}')" }.join(', ')
            sql "use @compute_cluster"
            sql "INSERT INTO ${tbl} VALUES ${values}"
            def rows = sql "SELECT * FROM ${tbl} WHERE k1 >= ${start} AND k1 < ${nextK1} ORDER BY k1"
            assertEquals(count, rows.size(), "CG-A warm read should return inserted rows")
            sleep(1000)
            return [start, nextK1]
        }

        def queryRangeOnCluster = { String clusterName, range ->
            sql "use @${clusterName}"
            sql "SELECT * FROM ${tbl} WHERE k1 >= ${range[0]} AND k1 < ${range[1]} ORDER BY k1"
        }

        def clearFileCacheOnBE = { backend ->
            def (code, out, err) = curl("GET", "http://${backend.Host}:${backend.HttpPort}/api/file_cache?op=clear&sync=false")
            assertTrue(code == 0 && (out.contains("OK") || out.contains("success") || err == null),
                    "failed to clear file cache on ${backend.Host}:${backend.HttpPort}: code=${code}, out=${out}, err=${err}")
        }

        def getBackendByCluster = { String clusterName ->
            def backends = sql """SHOW BACKENDS"""
            def clusterBackends = backends.findAll {
                it[19].contains("""\"compute_group_name\" : \"${clusterName}\"""")
            }
            assertEquals(1, clusterBackends.size(), "expected exactly one BE for cluster ${clusterName}")
            def backend = clusterBackends[0]
            return [BackendId: backend[0], Host: backend[1], HttpPort: backend[4], BrpcPort: backend[5]]
        }

        cluster.addBackend(1, CG_B_CLUSTER)
        cluster.addBackend(1, CG_C_CLUSTER)
        awaitUntil(120) {
            def backends = sql_return_maparray('show backends')
            backends.size() == 3 && backends.every { backend -> backend.Alive.toString() == 'true' }
        }

        def beA = getBackendByCluster(CG_A_CLUSTER)
        def beB = getBackendByCluster(CG_B_CLUSTER)
        def beC = getBackendByCluster(CG_C_CLUSTER)
        logger.info("CG-A BE: host={} brpc={} http={}", beA.Host, beA.BrpcPort, beA.HttpPort)
        logger.info("CG-B BE: host={} brpc={} http={}", beB.Host, beB.BrpcPort, beB.HttpPort)
        logger.info("CG-C BE: host={} brpc={} http={}", beC.Host, beC.BrpcPort, beC.HttpPort)

        def capturePeerMetrics = { String stage ->
            def metrics = [
                cgb_peer_lazy_fetch_triggered: getRequiredBrpcMetric(beB.Host, beB.BrpcPort, 'peer_lazy_fetch_triggered'),
                cgb_peer_race_peer_win: getRequiredBrpcMetric(beB.Host, beB.BrpcPort, 'peer_race_peer_win'),
                cgc_peer_lazy_fetch_triggered: getRequiredBrpcMetric(beC.Host, beC.BrpcPort, 'peer_lazy_fetch_triggered'),
                cgc_peer_race_peer_win: getRequiredBrpcMetric(beC.Host, beC.BrpcPort, 'peer_race_peer_win'),
                cga_file_cache_get_by_peer_num: getRequiredBrpcMetric(beA.Host, beA.BrpcPort, 'file_cache_get_by_peer_num'),
            ]
            logger.info("peer metrics {}: CGB({}:{}) lazy={} peer_win={}, CGC({}:{}) lazy={} peer_win={}, " +
                    "CGA({}:{}) get_by_peer={}",
                    stage,
                    beB.Host, beB.BrpcPort, metrics.cgb_peer_lazy_fetch_triggered, metrics.cgb_peer_race_peer_win,
                    beC.Host, beC.BrpcPort, metrics.cgc_peer_lazy_fetch_triggered, metrics.cgc_peer_race_peer_win,
                    beA.Host, beA.BrpcPort, metrics.cga_file_cache_get_by_peer_num)
            return metrics
        }

        sql "use @compute_cluster"
        sql "DROP TABLE IF EXISTS ${tbl}"
        sql """
            CREATE TABLE ${tbl} (
                k1 INT,
                v1 VARCHAR(256)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES ("replication_num" = "1", "disable_auto_compaction" = "true")
        """
        sql "INSERT INTO ${tbl} VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')"
        sql "SELECT * FROM ${tbl} ORDER BY k1"
        sleep(1000)

        logger.info("=== prepare peer candidates on CG-B and CG-C ===")
        def baselineB = queryRangeOnCluster(CG_B_CLUSTER, [1, 5])
        assertEquals(4, baselineB.size(), "baseline CGB read should succeed")
        def baselineC = queryRangeOnCluster(CG_C_CLUSTER, [1, 5])
        assertEquals(4, baselineC.size(), "baseline CGC read should succeed")
        awaitUntil(30) {
            getRequiredBrpcMetric(beB.Host, beB.BrpcPort, 'peer_lazy_fetch_triggered') > 0L &&
                    getRequiredBrpcMetric(beC.Host, beC.BrpcPort, 'peer_lazy_fetch_triggered') > 0L
        }

        logger.info("=== concurrent same-block peer fetch ===")
        def concurrentRange = insertAndWarmCGA(8)
        clearFileCacheOnBE(beB)
        clearFileCacheOnBE(beC)
        def metricsBeforeTrigger = capturePeerMetrics('before-trigger')
        GetDebugPoint().enableDebugPoint(beA.Host, beA.HttpPort as int, NodeType.BE,
                "CloudInternalServiceImpl::handle_peer_file_cache_block_request_hold_before_get_or_set", [sleep_ms: 3000])
        try {
            // Each reader BE emits exactly one outbound peer RPC for this block
            // (client-side downloader CAS). Firing concurrently from CG-B and CG-C
            // therefore puts two same-key RPCs on CG-A's pool at the same time.
            def threadB = Thread.start {
                def rows = queryRangeOnCluster(CG_B_CLUSTER, concurrentRange)
                assertEquals(8, rows.size(), "CGB query should succeed via peer or S3 race")
            }
            def threadC = Thread.start {
                def rows = queryRangeOnCluster(CG_C_CLUSTER, concurrentRange)
                assertEquals(8, rows.size(), "CGC query should succeed via peer or S3 race")
            }
            threadB.join()
            threadC.join()
        } finally {
            GetDebugPoint().disableDebugPoint(beA.Host, beA.HttpPort as int, NodeType.BE,
                    "CloudInternalServiceImpl::handle_peer_file_cache_block_request_hold_before_get_or_set")
        }
        def metricsAfterQueries = capturePeerMetrics('after-queries')
        awaitUntil(60) {
            getRequiredBrpcMetric(beA.Host, beA.BrpcPort, 'file_cache_get_by_peer_num') >=
                    metricsBeforeTrigger.cga_file_cache_get_by_peer_num + 2L
        }
        def metricsAfterAwait = capturePeerMetrics('after-await')
        assertTrue(metricsAfterAwait.cgb_peer_race_peer_win > metricsBeforeTrigger.cgb_peer_race_peer_win,
                "CGB should win the peer race for the concurrent same-block read")
        assertTrue(metricsAfterAwait.cgc_peer_race_peer_win > metricsBeforeTrigger.cgc_peer_race_peer_win,
                "CGC should win the peer race for the concurrent same-block read")
        logger.info("peer metric deltas: before={}, afterQueries={}, afterAwait={}",
                metricsBeforeTrigger, metricsAfterQueries, metricsAfterAwait)
    }
}
