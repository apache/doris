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

suite('test_cross_cg_server_fill_three_cg', 'docker') {
    if (!isCloudMode()) {
        return
    }

    final String CG_A = "compute_cluster"
    final String CG_B = "cross_cg_fill_allow_cluster"
    final String CG_C = "cross_cg_fill_reject_cluster"
    final String CG_B_ID = "${CG_B}_id"
    final String CG_C_ID = "${CG_C}_id"

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

    docker(options) {
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

        def rewriteBeCustomConfigAndRestart = { backend, updates ->
            def backendIndex = getBackendNodeIndex(backend)
            cluster.stopBackends(backendIndex)
            sleep(2000)

            def node = cluster.getBeByBackendId(backend.BackendId.toLong())
            assertTrue(node != null, "Backend node not found for BackendId=${backend.BackendId}")
            def customConf = new File("${node.path}/conf/be_custom.conf")
            assertTrue(customConf.exists(), "be_custom.conf not found for BackendId=${backend.BackendId}")

            def lines = customConf.readLines().findAll { line ->
                def trimmed = line.trim()
                !updates.keySet().any { key ->
                    trimmed.startsWith("${key}=") || trimmed.startsWith("${key} =")
                }
            }
            if (!lines.isEmpty() && lines[-1] != "") {
                lines << ""
            }
            updates.each { key, value ->
                lines << "${key} = ${value}"
            }
            customConf.text = lines.join(System.lineSeparator()) + System.lineSeparator()
            logger.info("Updated {} with {}", customConf.absolutePath, updates)

            cluster.startBackends(backendIndex)
            sleep(5000)
        }

        def clearFileCacheAndRestart = { backend, reason ->
            def backendIndex = getBackendNodeIndex(backend)
            cluster.stopBackends(backendIndex)
            sleep(2000)
            def node = cluster.getBeByBackendId(backend.BackendId.toLong())
            def cacheDir = new File("${node.path}/storage/file_cache")
            logger.info("Deleting {} file cache at {}", reason, cacheDir.absolutePath)
            if (cacheDir.exists()) {
                cacheDir.deleteDir()
            }
            cluster.startBackends(backendIndex)
            sleep(5000)
        }

        def createTable = { clusterName, tableName ->
            sql "use @${clusterName}"
            sql "DROP TABLE IF EXISTS ${tableName}"
            sql """
                CREATE TABLE ${tableName} (
                    k1 INT,
                    v1 VARCHAR(256)
                )
                DUPLICATE KEY(k1)
                DISTRIBUTED BY HASH(k1) BUCKETS 2
                PROPERTIES ("replication_num" = "1")
            """
        }

        def warmRows = { clusterName, tableName, predicate ->
            sql "use @${clusterName}"
            sql "SELECT * FROM ${tableName} WHERE ${predicate} ORDER BY k1"
            sleep(1000)
        }

        cluster.addBackend(1, CG_B)
        cluster.addBackend(1, CG_C)
        awaitUntil(30) {
            findBackendsByClusterName(CG_A).size() == 1 &&
                findBackendsByClusterName(CG_B).size() == 1 &&
                findBackendsByClusterName(CG_C).size() == 1
        }

        def beA = getSingleBackendByClusterName(CG_A)
        def beB = getSingleBackendByClusterName(CG_B)
        def beC = getSingleBackendByClusterName(CG_C)
        logger.info("CG-A requester: host={} brpcPort={}", beA.Host, beA.BrpcPort)
        logger.info("CG-B fill-allow: host={} brpcPort={}", beB.Host, beB.BrpcPort)
        logger.info("CG-C fill-reject: host={} brpcPort={}", beC.Host, beC.BrpcPort)

        // Case 1: A targets B, B allows server fill.
        def tblAllow = "test_cross_cg_server_fill_three_cg_allow_tbl"
        // These cloud configs are startup-only in the current docker image,
        // so switch them through be_custom.conf and a BE restart.
        rewriteBeCustomConfigAndRestart(beA,
            ["peer_cache_fill_compute_group_id": CG_B_ID])

        createTable(CG_B, tblAllow)
        sql "use @${CG_B}"
        sql "INSERT INTO ${tblAllow} VALUES (10, 'b10'), (20, 'b20'), (30, 'b30'), (40, 'b40')"
        warmRows(CG_B, tblAllow, "k1 < 50")

        def bFillReqBeforeAllow = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_server_fill_requested")
        def bFillSuccBeforeAllow = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_server_fill_success")
        def cFillReqBeforeAllow = getBrpcMetrics(beC.Host, beC.BrpcPort, "peer_server_fill_requested")

        sql "use @${CG_A}"
        def allowInitial = sql "SELECT * FROM ${tblAllow} WHERE k1 < 50 ORDER BY k1"
        assertEquals(4, allowInitial.size(), "CG-A should read initial rows from CG-B")
        // Candidate refresh is asynchronous and does not reliably surface as a lazy-fetch
        // counter bump after the requester BE has been restarted. Give it a short settle window
        // and validate the real peer/server-fill behavior on the next query.
        sleep(1000)

        sql "use @${CG_B}"
        sql "INSERT INTO ${tblAllow} VALUES (50, 'b50'), (60, 'b60')"
        warmRows(CG_B, tblAllow, "k1 >= 50")
        clearFileCacheAndRestart(beB, "CG-B")

        sql "use @${CG_A}"
        def allowAll = sql "SELECT * FROM ${tblAllow} ORDER BY k1"
        assertEquals(6, allowAll.size(), "CG-A should read all rows when CG-B server fill is enabled")
        awaitUntil(30) {
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_server_fill_requested") > bFillReqBeforeAllow &&
                getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_server_fill_success") > bFillSuccBeforeAllow
        }
        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_server_fill_requested") > bFillReqBeforeAllow,
            "CG-B should receive server fill requests from CG-A")
        assertTrue(
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_server_fill_success") > bFillSuccBeforeAllow,
            "CG-B should complete server fill successfully")
        assertEquals(
            cFillReqBeforeAllow,
            getBrpcMetrics(beC.Host, beC.BrpcPort, "peer_server_fill_requested"),
            "CG-C should not receive server fill requests when CG-A targets CG-B")

        // Case 2: A targets C, C rejects server fill.
        def tblReject = "test_cross_cg_server_fill_three_cg_reject_tbl"
        rewriteBeCustomConfigAndRestart(beA,
            ["peer_cache_fill_compute_group_id": CG_C_ID])
        rewriteBeCustomConfigAndRestart(beC,
            ["enable_peer_server_cache_fill": "false"])

        createTable(CG_C, tblReject)
        sql "use @${CG_C}"
        sql "INSERT INTO ${tblReject} VALUES (110, 'c110'), (120, 'c120'), (130, 'c130'), (140, 'c140')"
        warmRows(CG_C, tblReject, "k1 < 150")

        sql "use @${CG_A}"
        def rejectInitial = sql "SELECT * FROM ${tblReject} WHERE k1 < 150 ORDER BY k1"
        assertEquals(4, rejectInitial.size(), "CG-A should read initial rows from CG-C")
        sleep(1000)

        sql "use @${CG_C}"
        sql "INSERT INTO ${tblReject} VALUES (150, 'c150'), (160, 'c160')"
        warmRows(CG_C, tblReject, "k1 >= 150")
        clearFileCacheAndRestart(beC, "CG-C")

        def cFillReqBefore = getBrpcMetrics(beC.Host, beC.BrpcPort, "peer_server_fill_requested")
        def cFillSuccBefore = getBrpcMetrics(beC.Host, beC.BrpcPort, "peer_server_fill_success")
        def bFillReqBeforeReject = getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_server_fill_requested")
        def rejectPeerBefore = getBrpcMetrics(beA.Host, beA.BrpcPort, "cached_remote_reader_peer_read")
        def rejectS3Before = getBrpcMetrics(beA.Host, beA.BrpcPort, "cached_remote_reader_s3_read")

        sql "use @${CG_A}"
        def rejectAll = sql "SELECT * FROM ${tblReject} ORDER BY k1"
        assertEquals(6, rejectAll.size(), "CG-A should fall back and still read all rows when CG-C rejects fill")
        assertTrue(
            getBrpcMetrics(beA.Host, beA.BrpcPort, "cached_remote_reader_peer_read") > rejectPeerBefore,
            "CG-A should attempt peer read against CG-C before fallback")
        assertTrue(
            getBrpcMetrics(beA.Host, beA.BrpcPort, "cached_remote_reader_s3_read") > rejectS3Before,
            "CG-A should fall back to S3 when CG-C refuses server fill")
        assertEquals(
            cFillReqBefore,
            getBrpcMetrics(beC.Host, beC.BrpcPort, "peer_server_fill_requested"),
            "CG-C should not trigger server fill when enable_peer_server_cache_fill=false")
        assertEquals(
            cFillSuccBefore,
            getBrpcMetrics(beC.Host, beC.BrpcPort, "peer_server_fill_success"),
            "CG-C should not report server fill success when fill is disabled")
        assertEquals(
            bFillReqBeforeReject,
            getBrpcMetrics(beB.Host, beB.BrpcPort, "peer_server_fill_requested"),
            "CG-B should stay uninvolved when CG-A targets CG-C")

        logger.info("PASS")
    }
}
