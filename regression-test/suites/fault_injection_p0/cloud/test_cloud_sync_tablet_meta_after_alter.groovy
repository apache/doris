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

suite("test_cloud_sync_tablet_meta_after_alter", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }
    if (!getFeConfig("enable_debug_points").equalsIgnoreCase("true")) {
        logger.info("enable_debug_points=false, skip")
        return
    }
    if (context.config.multiClusterBes == null || context.config.multiClusterBes.trim().isEmpty()) {
        logger.info("multiClusterBes is empty, skip")
        return
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def cluster0 = "sync_meta_cluster0"
    def cluster1 = "sync_meta_cluster1"
    def clusterId0 = "sync_meta_cluster_id0"
    def clusterId1 = "sync_meta_cluster_id1"
    def dbName = "reg_sync_tablet_meta_db"
    def tableName = "test_cloud_sync_tablet_meta_after_alter"
    def versionLimitTableName = "test_cloud_sync_tablet_meta_version_limit_after_alter"

    List<String> ipList = []
    List<String> hbPortList = []
    List<String> beUniqueIdList = []

    String[] bes = context.config.multiClusterBes.split(',')
    for (String values : bes) {
        if (ipList.size() == 2) {
            break
        }
        String[] beInfo = values.split(':')
        ipList.add(beInfo[0])
        hbPortList.add(beInfo[1])
        beUniqueIdList.add(beInfo[3])
    }
    assertTrue(ipList.size() >= 2, "need at least two backends in multiClusterBes")

    def getBackendForCluster = { String clusterName ->
        def backend = sql_return_maparray("show backends").find { it.Tag.contains(clusterName) }
        assert backend != null: "backend for cluster ${clusterName} not found"
        return backend
    }

    def getBvar = { backend, String name ->
        def url = "http://${backend.Host}:${backend.BrpcPort}/vars/${name}"
        def (code, out, err) = curl("GET", url)
        assertEquals(0, code)
        def matcher = (out =~ /(?m)^${java.util.regex.Pattern.quote(name)}\\s*:\\s*(\\d+)$/)
        assertTrue(matcher.find(), "failed to parse bvar ${name} from ${url}, out=${out}, err=${err}")
        return matcher.group(1).toLong()
    }

    def waitForBvarIncrease = { backend, String name, long baseline, long delta ->
        for (int retry = 0; retry < 20; retry++) {
            def current = getBvar(backend, name)
            logger.info("wait bvar ${name} on ${backend.Host}:${backend.BrpcPort}, baseline=${baseline}, current=${current}")
            if (current >= baseline + delta) {
                return current
            }
            sleep(500)
        }
        return getBvar(backend, name)
    }

    def getCompactionPolicy = { String currentTableName ->
        def tablets = sql_return_maparray("""show tablets from ${currentTableName};""")
        assertEquals(1, tablets.size())
        def (code, out, err) = curl("GET", tablets[0].CompactionStatus)
        assertEquals(0, code)
        def tabletJson = parseJson(out.trim())
        return tabletJson["compaction policy"]
    }

    def useCluster = { String clusterName ->
        sql """use @${clusterName}"""
        sql """use ${dbName}"""
    }

    def cleanupClusters = {
        for (uniqueId in beUniqueIdList) {
            def resp = get_cluster.call(uniqueId)
            for (cluster in resp) {
                if (cluster.type == "COMPUTE") {
                    drop_cluster.call(cluster.cluster_name, cluster.cluster_id)
                }
            }
        }
        wait_cluster_change()
    }

    setBeConfigTemporary([
            tablet_sync_interval_s: 18000,
            schedule_sync_tablets_interval_s: 18000
    ]) {
        try {
            cleanupClusters.call()
            add_cluster.call(beUniqueIdList[0], ipList[0], hbPortList[0], cluster0, clusterId0)
            add_cluster.call(beUniqueIdList[1], ipList[1], hbPortList[1], cluster1, clusterId1)
            wait_cluster_change()

            sql """use @${cluster0}"""
            sql """create database if not exists ${dbName}"""
            sql """use ${dbName}"""
            sql """drop table if exists ${tableName} force"""
            sql """drop table if exists ${versionLimitTableName} force"""
            sql """
                CREATE TABLE ${tableName} (
                    id INT NOT NULL,
                    value INT
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                )
            """
            sql """insert into ${tableName} values (1, 10), (2, 20), (3, 30)"""

            def be0 = getBackendForCluster(cluster0)
            def be1 = getBackendForCluster(cluster1)

            // Case 1: warm only cluster0, alter property, and verify cluster1 is notified but skipped.
            useCluster.call(cluster0)
            qt_case1_cluster0_warmup """select count(*) from ${tableName}"""
            assertEquals("size_based", getCompactionPolicy(tableName))

            def case1Synced0 = getBvar(be0, "cloud_sync_tablet_meta_synced_total")
            def case1Skipped1 = getBvar(be1, "cloud_sync_tablet_meta_skipped_total")

            sql """alter table ${tableName} set ("compaction_policy" = "time_series")"""

            assertTrue(waitForBvarIncrease(be0, "cloud_sync_tablet_meta_synced_total", case1Synced0, 1) >= case1Synced0 + 1)
            assertTrue(waitForBvarIncrease(be1, "cloud_sync_tablet_meta_skipped_total", case1Skipped1, 1) >= case1Skipped1 + 1)

            useCluster.call(cluster0)
            qt_case1_cluster0_after_alter """select count(*) from ${tableName}"""
            assertEquals("time_series", getCompactionPolicy(tableName))

            useCluster.call(cluster1)
            qt_case1_cluster1_first_access """select count(*) from ${tableName}"""
            assertEquals("time_series", getCompactionPolicy(tableName))

            // Case 2: warm both clusters, alter property again, and verify both clusters sync immediately.
            useCluster.call(cluster0)
            qt_case2_cluster0_warmup """select count(*) from ${tableName}"""
            useCluster.call(cluster1)
            qt_case2_cluster1_warmup """select count(*) from ${tableName}"""

            def case2Synced0 = getBvar(be0, "cloud_sync_tablet_meta_synced_total")
            def case2Synced1 = getBvar(be1, "cloud_sync_tablet_meta_synced_total")

            useCluster.call(cluster0)
            sql """alter table ${tableName} set ("compaction_policy" = "size_based")"""

            assertTrue(waitForBvarIncrease(be0, "cloud_sync_tablet_meta_synced_total", case2Synced0, 1) >= case2Synced0 + 1)
            assertTrue(waitForBvarIncrease(be1, "cloud_sync_tablet_meta_synced_total", case2Synced1, 1) >= case2Synced1 + 1)

            useCluster.call(cluster0)
            assertEquals("size_based", getCompactionPolicy(tableName))
            useCluster.call(cluster1)
            assertEquals("size_based", getCompactionPolicy(tableName))

            // Case 3: hit the size_based version limit first, then alter to time_series and verify writes succeed immediately.
            useCluster.call(cluster0)
            sql """drop table if exists ${versionLimitTableName} force"""
            sql """
                CREATE TABLE ${versionLimitTableName} (
                    id BIGINT NOT NULL,
                    value BIGINT
                ) ENGINE=OLAP
                DUPLICATE KEY(id)
                DISTRIBUTED BY HASH(id) BUCKETS 1
                PROPERTIES (
                    "replication_num" = "1",
                    "disable_auto_compaction" = "true"
                )
            """
            qt_case3_version_limit_warmup """select count(*) from ${versionLimitTableName}"""
            assertEquals("size_based", getCompactionPolicy(versionLimitTableName))

            int successfulBeforeAlter = 0
            String versionLimitFailure = null
            for (int i = 0; i < 2400; i++) {
                try {
                    sql """insert into ${versionLimitTableName} values (${i}, ${i})"""
                    successfulBeforeAlter++
                    if ((i + 1) % 200 == 0) {
                        logger.info("inserted {} versions into {}", i + 1, versionLimitTableName)
                    }
                } catch (Exception e) {
                    versionLimitFailure = e.getMessage()
                    logger.info("hit version limit after {} successful inserts, msg={}",
                            successfulBeforeAlter, versionLimitFailure)
                    break
                }
            }
            assertTrue(versionLimitFailure != null, "expected version limit failure before alter")
            assertTrue(versionLimitFailure.contains("-235")
                    || versionLimitFailure.toLowerCase().contains("too many versions")
                    || versionLimitFailure.toLowerCase().contains("version count")
                    || versionLimitFailure.toLowerCase().contains("exceed limit"),
                    "unexpected failure message: ${versionLimitFailure}")

            def versionLimitTablets = sql_return_maparray("""show tablets from ${versionLimitTableName};""")
            assertEquals(1, versionLimitTablets.size())
            long versionCountBeforeAlter = versionLimitTablets[0].VersionCount.toLong()
            assertTrue(versionCountBeforeAlter >= 2000,
                    "expected version count near max limit, actual=${versionCountBeforeAlter}")

            sql """alter table ${versionLimitTableName} set ("compaction_policy" = "time_series")"""
            assertEquals("time_series", getCompactionPolicy(versionLimitTableName))

            int successfulAfterAlter = 0
            for (int i = 0; i < 50; i++) {
                long rowId = 100000L + i
                sql """insert into ${versionLimitTableName} values (${rowId}, ${rowId})"""
                successfulAfterAlter++
            }
            assertEquals(50, successfulAfterAlter)
            def versionLimitTabletsAfterAlter = sql_return_maparray("""show tablets from ${versionLimitTableName};""")
            assertEquals(1, versionLimitTabletsAfterAlter.size())
            assertTrue(versionLimitTabletsAfterAlter[0].VersionCount.toLong() > versionCountBeforeAlter,
                    "expected version count to keep increasing immediately after alter")
            qt_case3_version_limit_after_alter """select count(*) from ${versionLimitTableName}"""

            // Case 4: disable FE proactive notification and verify cached tablets remain stale.
            GetDebugPoint().enableDebugPointForAllFEs("CloudSchemaChangeHandler.notifyBackendsToSyncTabletMeta.skip")
            def case4Request0 = getBvar(be0, "cloud_sync_tablet_meta_requests_total")
            def case4Request1 = getBvar(be1, "cloud_sync_tablet_meta_requests_total")
            def case4Synced0 = getBvar(be0, "cloud_sync_tablet_meta_synced_total")
            def case4Synced1 = getBvar(be1, "cloud_sync_tablet_meta_synced_total")
            def case4Skipped0 = getBvar(be0, "cloud_sync_tablet_meta_skipped_total")
            def case4Skipped1 = getBvar(be1, "cloud_sync_tablet_meta_skipped_total")

            useCluster.call(cluster0)
            sql """alter table ${tableName} set ("compaction_policy" = "time_series")"""
            sleep(2000)

            assertEquals(case4Request0, getBvar(be0, "cloud_sync_tablet_meta_requests_total"))
            assertEquals(case4Request1, getBvar(be1, "cloud_sync_tablet_meta_requests_total"))
            assertEquals(case4Synced0, getBvar(be0, "cloud_sync_tablet_meta_synced_total"))
            assertEquals(case4Synced1, getBvar(be1, "cloud_sync_tablet_meta_synced_total"))
            assertEquals(case4Skipped0, getBvar(be0, "cloud_sync_tablet_meta_skipped_total"))
            assertEquals(case4Skipped1, getBvar(be1, "cloud_sync_tablet_meta_skipped_total"))

            useCluster.call(cluster0)
            assertEquals("size_based", getCompactionPolicy(tableName))
            useCluster.call(cluster1)
            assertEquals("size_based", getCompactionPolicy(tableName))
        } finally {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()
            try {
                sql """use @${cluster0}"""
                sql """drop table if exists ${dbName}.${tableName} force"""
                sql """drop table if exists ${dbName}.${versionLimitTableName} force"""
            } catch (Exception e) {
                logger.info("drop table in finally failed: ${e.getMessage()}")
            }
            cleanupClusters.call()
        }
    }
}
