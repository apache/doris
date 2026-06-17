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
import java.net.ServerSocket

suite('test_file_cache_warmup_read_metrics_docker', 'docker') {
    if (!isCloudMode()) {
        return
    }

    def allocatePort = {
        def socket = new ServerSocket(0)
        def port = socket.localPort
        socket.close()
        return port
    }
    def minioPort = allocatePort()
    def minioBucket = "test-bucket"
    def minioContainer = "doris-file-cache-warmup-metrics-minio-${System.currentTimeMillis()}"

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
        'fetch_cluster_cache_hotspot_interval_ms=1000',
        'heartbeat_interval_second=1',
        'cloud_warm_up_for_rebalance_type=async_warmup',
        'cloud_pre_heating_time_limit_sec=180',
        'auto_check_statistics_in_minutes=60',
    ]
    options.beConfigs += [
        'file_cache_enter_disk_resource_limit_mode_percent=99',
        'enable_evict_file_cache_in_advance=false',
        'file_cache_background_monitor_interval_ms=1000',
        'report_tablet_interval_seconds=1',
        'schedule_sync_tablets_interval_s=18000',
        'disable_auto_compaction=true',
        'disable_segment_cache=true',
        'disable_storage_page_cache=true',
        'enable_cache_read_from_peer=false',
    ]
    options.cloudStoreConfigs += [
        'DORIS_CLOUD_USER=minio',
        'DORIS_CLOUD_AK=minioadmin',
        'DORIS_CLOUD_SK=minioadmin',
        "DORIS_CLOUD_BUCKET=${minioBucket}",
        "DORIS_CLOUD_ENDPOINT=host.docker.internal:${minioPort}",
        "DORIS_CLOUD_EXTERNAL_ENDPOINT=host.docker.internal:${minioPort}",
        'DORIS_CLOUD_REGION=us-east-1',
        'DORIS_CLOUD_PROVIDER=S3',
    ]
    options.extraHosts += [
        'host.docker.internal:host-gateway',
        "${minioBucket}.host.docker.internal:host-gateway",
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    def computeCluster = "compute_cluster"
    def sourceCluster = "metrics_warmup_source"
    def targetCluster = "metrics_warmup_target"
    def payload = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

    def waitForCondition = { condition, timeoutMs, message ->
        long start = System.currentTimeMillis()
        while (System.currentTimeMillis() - start < timeoutMs) {
            if (condition()) {
                return
            }
            sleep(1000)
        }
        if (!condition()) {
            throw new RuntimeException(message)
        }
    }

    def startMinio = {
        cmd """docker rm -f ${minioContainer} >/dev/null 2>&1 || true"""
        cmd """
            docker run -d --name ${minioContainer} \
                -p ${minioPort}:9000 \
                -e MINIO_ROOT_USER=minioadmin \
                -e MINIO_ROOT_PASSWORD=minioadmin \
                -e MINIO_DOMAIN=host.docker.internal \
                minio/minio:RELEASE.2024-11-07T00-52-20Z \
                server /data --console-address ':9001'
        """
        waitForCondition({
            try {
                new URL("http://127.0.0.1:${minioPort}/minio/health/ready").text
                return true
            } catch (Throwable ignored) {
                return false
            }
        }, 60000, "MinIO did not become ready")
        cmd """
            docker exec ${minioContainer} sh -c \
                'mc alias set local http://localhost:9000 minioadmin minioadmin && mc mb -p local/${minioBucket}'
        """
    }

    def stopMinio = {
        cmd """docker rm -f ${minioContainer} >/dev/null 2>&1 || true"""
    }

    def getClusterBackends = { clusterName ->
        def backends = sql """SHOW BACKENDS"""
        def clusterBackends = backends.findAll {
            it[19].contains("""\"compute_group_name\" : \"${clusterName}\"""")
        }
        assertTrue(clusterBackends.size() > 0, "No backend found for cluster ${clusterName}")
        return clusterBackends
    }

    def getPromMetric = { ip, httpPort, name ->
        def metrics = new URL("http://${ip}:${httpPort}/metrics").text
        def matcher = metrics =~ ~"(?m)^${name}\\s+([0-9]+(?:\\.[0-9]+)?)"
        if (matcher.find()) {
            return new BigDecimal(matcher[0][1]).longValue()
        }
        logger.info("${name} not found in /metrics for ${ip}:${httpPort}, treat it as 0")
        return 0L
    }

    def getBrpcMetric = { ip, brpcPort, name ->
        def metrics = new URL("http://${ip}:${brpcPort}/brpc_metrics").text
        def matcher = metrics =~ ~"(?m)^.*${name}\\s+([0-9]+)"
        if (matcher.find()) {
            return matcher[0][1] as long
        }
        logger.info("${name} not found in /brpc_metrics for ${ip}:${brpcPort}, treat it as 0")
        return 0L
    }

    def getBackendReadMetrics = { be ->
        def ip = be[1]
        def httpPort = be[4]
        return [
            total : getPromMetric(ip, httpPort, "doris_be_num_io_bytes_read_total"),
            local : getPromMetric(ip, httpPort, "doris_be_num_io_bytes_read_from_cache"),
            remote: getPromMetric(ip, httpPort, "doris_be_num_io_bytes_read_from_remote"),
            peer  : getPromMetric(ip, httpPort, "doris_be_num_io_bytes_read_from_peer"),
        ]
    }

    def getClusterReadMetrics = { clusterName ->
        def result = [total: 0L, local: 0L, remote: 0L, peer: 0L]
        getClusterBackends(clusterName).each { be ->
            def metrics = getBackendReadMetrics(be)
            result.total += metrics.total
            result.local += metrics.local
            result.remote += metrics.remote
            result.peer += metrics.peer
        }
        return result
    }

    def getMetricDelta = { before, after ->
        return [
            total : after.total - before.total,
            local : after.local - before.local,
            remote: after.remote - before.remote,
            peer  : after.peer - before.peer,
        ]
    }

    def assertMetricInvariant = { metrics ->
        assertEquals(metrics.total, metrics.local + metrics.remote + metrics.peer)
    }

    def assertNoReadMetricDelta = { before, after, label ->
        def delta = getMetricDelta(before, after)
        logger.info("${label} read metric delta: ${delta}")
        assertEquals(0L, delta.total)
        assertEquals(0L, delta.local)
        assertEquals(0L, delta.remote)
        assertEquals(0L, delta.peer)
    }

    def clearFileCache = { ip, httpPort ->
        def response = new URL("http://${ip}:${httpPort}/api/file_cache?op=clear&sync=true").text
        def json = new JsonSlurper().parseText(response)
        if (json.status != "OK") {
            throw new RuntimeException("Clear cache on ${ip}:${httpPort} failed: ${json.status}")
        }
    }

    def clearFileCacheOnCluster = { clusterName ->
        getClusterBackends(clusterName).each { be ->
            clearFileCache(be[1], be[4])
        }
        sleep(5000)
    }

    def getBackendCacheSize = { be ->
        return getBrpcMetric(be[1], be[5], "cache_cache_size")
    }

    def getClusterCacheSizeSum = { clusterName ->
        long sum = 0
        getClusterBackends(clusterName).each { be ->
            sum += getBackendCacheSize(be)
        }
        return sum
    }

    def getClusterBrpcMetricSum = { clusterName, metricName ->
        long sum = 0
        getClusterBackends(clusterName).each { be ->
            sum += getBrpcMetric(be[1], be[5], metricName)
        }
        return sum
    }

    def createTable = { table, buckets ->
        sql """DROP TABLE IF EXISTS ${table}"""
        sql """
            CREATE TABLE ${table} (
                k1 INT,
                v1 VARCHAR(4096)
            )
            DUPLICATE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS ${buckets}
            PROPERTIES (
                "replication_num" = "1",
                "file_cache_ttl_seconds" = "3600"
            )
        """
    }

    def loadRows = { table, rowStart, rowCount ->
        int batchSize = 100
        for (int begin = 0; begin < rowCount; begin += batchSize) {
            int end = Math.min(rowCount, begin + batchSize)
            def values = (begin..<end).collect { idx ->
                int key = rowStart + idx
                return "(${key}, '${payload}_${key}_${payload}')"
            }.join(",")
            sql """INSERT INTO ${table} VALUES ${values}"""
        }
        sql """sync"""
    }

    def queryTable = { table ->
        def result = sql """SELECT SUM(LENGTH(v1)) FROM ${table} WHERE k1 >= 0"""
        assertTrue((result[0][0] as String).toLong() > 0)
    }

    def waitWarmupFinished = { jobId ->
        waitForCondition({
            def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
            if (jobInfo.size() == 0) {
                return false
            }
            def state = jobInfo[0][3].toString()
            if (state == "CANCELLED") {
                throw new RuntimeException("Warm up job ${jobId} was cancelled")
            }
            return state == "FINISHED"
        }, 120000, "Warm up job ${jobId} did not finish")
    }

    def cancelWarmupJob = { jobId ->
        sql """CANCEL WARM UP JOB WHERE ID = ${jobId}"""
        def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${jobId}"""
        assertEquals("CANCELLED", jobInfo[0][3])
    }

    try {
        startMinio()

        docker(options) {
            cluster.addBackend(1, sourceCluster)
            cluster.addBackend(1, targetCluster)

            sql """SET enable_file_cache = true"""
            sql """SET enable_sql_cache = false"""
            sql """SET enable_query_cache = false"""

            sql """use @${computeCluster}"""
            def manualTable = "test_file_cache_metrics_manual"
            createTable(manualTable, 1)
            loadRows(manualTable, 1, 300)
            clearFileCacheOnCluster(computeCluster)

            def beforeQueryMiss = getClusterReadMetrics(computeCluster)
            queryTable(manualTable)
            waitForCondition({
                def delta = getMetricDelta(beforeQueryMiss, getClusterReadMetrics(computeCluster))
                return delta.total > 0 && delta.remote > 0
            }, 30000, "Normal query miss did not increase global remote read metrics")
            def queryMissDelta = getMetricDelta(beforeQueryMiss, getClusterReadMetrics(computeCluster))
            logger.info("query miss read metric delta: ${queryMissDelta}")
            assertMetricInvariant(queryMissDelta)
            assertTrue(queryMissDelta.remote > 0)

            clearFileCacheOnCluster(computeCluster)
            def beforeManualWarmup = getClusterReadMetrics(computeCluster)
            def manualJob = sql """WARM UP CLUSTER ${computeCluster} WITH TABLE ${manualTable}"""
            waitWarmupFinished(manualJob[0][0])
            waitForCondition({
                return getClusterCacheSizeSum(computeCluster) > 0
            }, 30000, "Manual table warm up did not populate file cache")
            def afterManualWarmup = getClusterReadMetrics(computeCluster)
            assertNoReadMetricDelta(beforeManualWarmup, afterManualWarmup, "manual table warm up")

            def beforeQueryHit = getClusterReadMetrics(computeCluster)
            queryTable(manualTable)
            waitForCondition({
                def delta = getMetricDelta(beforeQueryHit, getClusterReadMetrics(computeCluster))
                return delta.total > 0 && delta.local > 0
            }, 30000, "Query after warm up did not increase global local read metrics")
            def queryHitDelta = getMetricDelta(beforeQueryHit, getClusterReadMetrics(computeCluster))
            logger.info("query hit read metric delta: ${queryHitDelta}")
            assertMetricInvariant(queryHitDelta)
            assertTrue(queryHitDelta.local > 0)

            sql """use @${sourceCluster}"""
            sql """TRUNCATE TABLE __internal_schema.cloud_cache_hotspot"""
            def periodicTable = "test_file_cache_metrics_periodic"
            createTable(periodicTable, 1)
            loadRows(periodicTable, 1000, 300)
            clearFileCacheOnCluster(sourceCluster)
            clearFileCacheOnCluster(targetCluster)

            def beforePeriodicWarmup = getClusterReadMetrics(targetCluster)
            def periodicJob = sql """
                WARM UP CLUSTER ${targetCluster} WITH CLUSTER ${sourceCluster}
                PROPERTIES (
                    "sync_mode" = "periodic",
                    "sync_interval_sec" = "1"
                )
            """
            for (int i = 0; i < 120; i++) {
                queryTable(periodicTable)
            }
            waitForCondition({
                return getClusterCacheSizeSum(sourceCluster) > 0 &&
                        getClusterCacheSizeSum(targetCluster) > 0
            }, 90000, "Periodic warm up did not populate target file cache")
            def afterPeriodicWarmup = getClusterReadMetrics(targetCluster)
            assertNoReadMetricDelta(beforePeriodicWarmup, afterPeriodicWarmup,
                    "periodic cluster warm up")
            cancelWarmupJob(periodicJob[0][0])

            def eventTable = "test_file_cache_metrics_event"
            createTable(eventTable, 1)
            clearFileCacheOnCluster(sourceCluster)
            clearFileCacheOnCluster(targetCluster)
            def beforeEventWarmup = getClusterReadMetrics(targetCluster)
            def eventJob = sql """
                WARM UP CLUSTER ${targetCluster} WITH CLUSTER ${sourceCluster}
                PROPERTIES (
                    "sync_mode" = "event_driven",
                    "sync_event" = "load"
                )
            """
            waitForCondition({
                def jobInfo = sql """SHOW WARM UP JOB WHERE ID = ${eventJob[0][0]}"""
                return jobInfo.size() > 0 && jobInfo[0][3].toString() in ["RUNNING", "PENDING"]
            }, 30000, "Event-driven warm up job did not enter running state")
            sleep(15000)
            def beforeEventSubmitted = getClusterBrpcMetricSum(targetCluster,
                    "file_cache_event_driven_warm_up_submitted_segment_num")
            def beforeEventFinished = getClusterBrpcMetricSum(targetCluster,
                    "file_cache_event_driven_warm_up_finished_segment_num")
            def beforeEventFailed = getClusterBrpcMetricSum(targetCluster,
                    "file_cache_event_driven_warm_up_failed_segment_num")
            loadRows(eventTable, 2000, 300)
            waitForCondition({
                def submitted = getClusterBrpcMetricSum(targetCluster,
                        "file_cache_event_driven_warm_up_submitted_segment_num")
                def finished = getClusterBrpcMetricSum(targetCluster,
                        "file_cache_event_driven_warm_up_finished_segment_num")
                return submitted > beforeEventSubmitted && finished >= submitted
            }, 90000, "Event-driven warm up did not finish target segment downloads")
            def afterEventSubmitted = getClusterBrpcMetricSum(targetCluster,
                    "file_cache_event_driven_warm_up_submitted_segment_num")
            def afterEventFinished = getClusterBrpcMetricSum(targetCluster,
                    "file_cache_event_driven_warm_up_finished_segment_num")
            def afterEventFailed = getClusterBrpcMetricSum(targetCluster,
                    "file_cache_event_driven_warm_up_failed_segment_num")
            logger.info("event-driven warm up segment metrics: submitted ${beforeEventSubmitted}"
                    + " -> ${afterEventSubmitted}, finished ${beforeEventFinished}"
                    + " -> ${afterEventFinished}, failed ${beforeEventFailed}"
                    + " -> ${afterEventFailed}")
            assertEquals(beforeEventFailed, afterEventFailed)
            def afterEventWarmup = getClusterReadMetrics(targetCluster)
            assertNoReadMetricDelta(beforeEventWarmup, afterEventWarmup,
                    "event-driven cluster warm up")
            cancelWarmupJob(eventJob[0][0])

            sql """use @${computeCluster}"""
            def rebalanceTable = "test_file_cache_metrics_rebalance"
            createTable(rebalanceTable, 8)
            loadRows(rebalanceTable, 3000, 500)
            clearFileCacheOnCluster(computeCluster)
            def beforeRebalanceSourceQuery = getClusterReadMetrics(computeCluster)
            queryTable(rebalanceTable)
            waitForCondition({
                def delta = getMetricDelta(beforeRebalanceSourceQuery,
                        getClusterReadMetrics(computeCluster))
                return delta.total > 0 && delta.remote > 0
            }, 30000, "Rebalance source query did not populate source file cache")

            def beforeBackendIds = getClusterBackends(computeCluster).collect { it[0].toString() } as Set
            def beforeRebalanceWarmup = getClusterReadMetrics(computeCluster)
            cluster.addBackend(1, computeCluster)
            def newBackendHolder = [:]
            waitForCondition({
                def added = getClusterBackends(computeCluster).find {
                    !beforeBackendIds.contains(it[0].toString())
                }
                if (added != null) {
                    newBackendHolder.be = added
                    return true
                }
                return false
            }, 30000, "New backend was not added to ${computeCluster}")

            waitForCondition({
                def distribution = sql_return_maparray """ADMIN SHOW REPLICA DISTRIBUTION FROM ${rebalanceTable}"""
                return distribution.any {
                    it.BackendId.toString() == newBackendHolder.be[0].toString() &&
                            Integer.valueOf(it.ReplicaNum.toString()) > 0
                }
            }, 180000, "Rebalance did not move any replica to the new backend")

            waitForCondition({
                return getBackendCacheSize(newBackendHolder.be) > 0
            }, 180000, "Rebalance warm up did not populate file cache on the new backend")

            def afterRebalanceWarmup = getClusterReadMetrics(computeCluster)
            assertNoReadMetricDelta(beforeRebalanceWarmup, afterRebalanceWarmup,
                    "rebalance warm up")
        }
    } finally {
        stopMinio()
    }
}
