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

// Peer fetch pool isolation regression.
//
// Topology: 1 FE + CG-A (source BE) + CG-B (reader BE). CG-A cache is warmed, CG-B
// discovers CG-A as a peer candidate, then CG-B cold reads should hit peer fetch RPCs.

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

suite('test_peer_fetch_pool_isolation', 'docker') {
    if (!isCloudMode()) {
        return
    }

    final String CG_A_CLUSTER_ID = "compute_cluster_id"
    final String CG_A_CLUSTER = "compute_cluster"
    final String CG_B_CLUSTER = "cross_cg_peer_cluster"

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
        'peer_race_hedge_delay_ms=50',
        'peer_fetch_queue_timeout_ms=500',
        'brpc_peer_fetch_pool_threads=1',
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

    def getVarzConfig = { host, httpPort, name ->
        def vars = new URL(getHttpUrl(host, httpPort, "/varz")).text
        def matcher = vars =~ ~"(?m)^${name}[=:]\\s*(\\S+)"
        if (matcher.find()) {
            return matcher[0][1]
        }
        return null
    }

    docker(options) {
        def tbl = "test_peer_fetch_pool_isolation_tbl"
        def nextK1 = 100

        def updateBeConfig = { host, httpPort, key, value ->
            def (code, out, err) = curl("POST", "http://${host}:${httpPort}/api/update_config?${key}=${value}")
            assertTrue(out.contains("OK"),
                    "failed to set ${key}=${value} on ${host}:${httpPort}: code=${code}, out=${out}, err=${err}")
        }

        def setPeerFetchTimeoutForAllBEs = { value ->
            def configValue = value.toString()
            sql_return_maparray('show backends').each { backend ->
                updateBeConfig(backend.Host, backend.HttpPort as int, 'peer_fetch_queue_timeout_ms', configValue)
                awaitUntil(10) {
                    getVarzConfig(backend.Host, backend.HttpPort as int, 'peer_fetch_queue_timeout_ms') == configValue
                }
            }
        }

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

        def queryRangeOnCGB = { range ->
            sql "use @cross_cg_peer_cluster"
            sql "SELECT * FROM ${tbl} WHERE k1 >= ${range[0]} AND k1 < ${range[1]} ORDER BY k1"
        }

        def clearFileCacheOnBE = { backend ->
            def (code, out, err) = curl("GET", "http://${backend.Host}:${backend.HttpPort}/api/file_cache?op=clear&sync=true")
            assertTrue(code == 0 && (out.contains("OK") || out.contains("success") || err == null),
                    "failed to clear file cache on ${backend.Host}:${backend.HttpPort}: code=${code}, out=${out}, err=${err}")
        }

        def setPeerFetchTimeoutOnBE = { backend, value ->
            def configValue = value.toString()
            updateBeConfig(backend.Host, backend.HttpPort as int, 'peer_fetch_queue_timeout_ms', configValue)
            awaitUntil(10) {
                getVarzConfig(backend.Host, backend.HttpPort as int, 'peer_fetch_queue_timeout_ms') == configValue
            }
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
        awaitUntil(120) {
            def backends = sql_return_maparray('show backends')
            backends.size() == 2 && backends.every { backend -> backend.Alive.toString() == 'true' }
        }

        def beA = getBackendByCluster(CG_A_CLUSTER)
        def beB = getBackendByCluster(CG_B_CLUSTER)
        logger.info("CG-A BE: host={} brpc={} http={}", beA.Host, beA.BrpcPort, beA.HttpPort)
        logger.info("CG-B BE: host={} brpc={} http={}", beB.Host, beB.BrpcPort, beB.HttpPort)

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

        logger.info("=== metrics existence smoke ===")
        // Pool capacity metrics are REGISTER_HOOK_METRIC and live on /metrics with the doris_be_ prefix,
        // unlike the bvars below on /brpc_metrics; smoke only new bvars here and let the queue timeout
        // trigger below prove the peer fetch pool exists and runs.
        assertEquals(0L, getRequiredBrpcMetric(beA.Host, beA.BrpcPort, 'file_cache_get_by_peer_queue_timeout_num'))

        logger.info("=== dynamic config ===")
        updateBeConfig(beA.Host, beA.HttpPort as int, 'peer_fetch_queue_timeout_ms', '1')
        awaitUntil(10) { getVarzConfig(beA.Host, beA.HttpPort as int, 'peer_fetch_queue_timeout_ms') == '1' }
        updateBeConfig(beA.Host, beA.HttpPort as int, 'peer_fetch_queue_timeout_ms', '1000')
        awaitUntil(10) { getVarzConfig(beA.Host, beA.HttpPort as int, 'peer_fetch_queue_timeout_ms') == '1000' }
        updateBeConfig(beA.Host, beA.HttpPort as int, 'peer_fetch_queue_timeout_ms', '500')
        awaitUntil(10) { getVarzConfig(beA.Host, beA.HttpPort as int, 'peer_fetch_queue_timeout_ms') == '500' }

        logger.info("=== prepare peer candidate on CG-B ===")
        sql "use @cross_cg_peer_cluster"
        def baseline = sql "SELECT * FROM ${tbl} ORDER BY k1"
        assertEquals(4, baseline.size(), "baseline CGB read should succeed")
        awaitUntil(30) {
            getRequiredBrpcMetric(beB.Host, beB.BrpcPort, 'peer_lazy_fetch_triggered') > 0L
        }

        logger.info("=== queue timeout trigger ===")
        // Two separate rowsets so the two queries touch different files (different
        // cache hashes). Query 1 populates CG-B's cache for its own block via the
        // S3 race, so if both queries used the same range Query 2 would cache-hit
        // locally and never emit a peer RPC. Distinct rowsets force Query 2 to be
        // a real cold read, so its peer RPC actually queues behind the held worker
        // on CG-A.
        def timeoutRangeHold = insertAndWarmCGA(8)
        def timeoutRangeQueued = insertAndWarmCGA(8)
        clearFileCacheOnBE(beB)
        def queueTimeoutBefore = getRequiredBrpcMetric(beA.Host, beA.BrpcPort, 'file_cache_get_by_peer_queue_timeout_num')
        setPeerFetchTimeoutOnBE(beA, '100')
        GetDebugPoint().enableDebugPoint(beA.Host, beA.HttpPort as int, NodeType.BE,
                "CloudInternalServiceImpl::handle_peer_file_cache_block_request_hold_before_get_or_set", [sleep_ms: 3000])
        def firstTimeoutThread = Thread.start {
            def rows = queryRangeOnCGB(timeoutRangeHold)
            assertEquals(8, rows.size(), "first CGB query should succeed while holding peer fetch pool")
        }
        sleep(100)
        try {
            def timeoutRows = queryRangeOnCGB(timeoutRangeQueued)
            assertEquals(8, timeoutRows.size(), "CGB query should succeed via S3 fallback after peer queue timeout")
            firstTimeoutThread.join()
        } finally {
            GetDebugPoint().disableDebugPoint(beA.Host, beA.HttpPort as int, NodeType.BE,
                    "CloudInternalServiceImpl::handle_peer_file_cache_block_request_hold_before_get_or_set")
        }
        awaitUntil(30) {
            getRequiredBrpcMetric(beA.Host, beA.BrpcPort, 'file_cache_get_by_peer_queue_timeout_num') > queueTimeoutBefore
        }
        setPeerFetchTimeoutOnBE(beA, '500')
    }
}
