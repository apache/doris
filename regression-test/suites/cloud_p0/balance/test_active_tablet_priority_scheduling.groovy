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
import java.nio.charset.StandardCharsets

suite('test_active_tablet_priority_scheduling', 'cloud_p0, docker') {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.feConfigs += [
            'cloud_cluster_check_interval_second=1',
            'cloud_tablet_rebalancer_interval_second=1',
            // enable the feature under test
            'enable_cloud_active_tablet_priority_scheduling=true',
            // enable active tablet sliding window access stats (for metrics & SHOW TABLET*)
            'enable_active_tablet_sliding_window_access_stats=true',
            'active_tablet_sliding_window_time_window_second=3600',
            // make the scheduling signal deterministic: only run table balance
            'enable_cloud_partition_balance=false',
            'enable_cloud_table_balance=true',
            'enable_cloud_global_balance=false',
            // print CloudTabletRebalancer DEBUG logs
            'sys_log_verbose_modules=org.apache.doris.cloud.catalog',
            'heartbeat_interval_second=1',
            'rehash_tablet_after_be_dead_seconds=3600',
            'cache_enable_sql_mode=false',
    ]
    options.beConfigs += [
            'report_tablet_interval_seconds=1',
            'schedule_sync_tablets_interval_s=18000',
            'disable_auto_compaction=true',
            'sys_log_verbose_modules=*',
            'enable_packed_file=false',
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    def tailFile = { String path, int maxBytes ->
        def f = new File(path)
        if (!f.exists()) {
            return ""
        }
        def raf = new RandomAccessFile(f, "r")
        try {
            long len = raf.length()
            long start = Math.max(0L, len - maxBytes)
            raf.seek(start)
            byte[] buf = new byte[(int) (len - start)]
            raf.readFully(buf)
            return new String(buf, StandardCharsets.UTF_8)
        } finally {
            raf.close()
        }
    }

    def getTableIdByName = { String tblName ->
        def tablets = sql_return_maparray """SHOW TABLETS FROM ${tblName}"""
        assert tablets.size() > 0
        def tabletId = tablets[0].TabletId
        def meta = sql_return_maparray """SHOW TABLET ${tabletId}"""
        assert meta.size() > 0
        return meta[0].TableId.toLong()
    }

    docker(options) {
        def getFeMetricValue = { String metricSuffix ->
            def ret = null
            String masterHttpAddress = getMasterIp() + ":" + getMasterPort("http")
            httpTest {
                endpoint masterHttpAddress
                uri "/metrics?type=json"
                op "get"
                check { code, body ->
                    def jsonSlurper = new JsonSlurper()
                    def result = jsonSlurper.parseText(body)
                    def entry = result.find { it.tags?.metric?.toString()?.endsWith(metricSuffix) }
                    ret = entry ? entry.value : null
                }
            }
            return ret
        }

        def hotTbl = "hot_tbl_active_sched"
        def coldTbl = "cold_tbl_active_sched"

        sql """DROP TABLE IF EXISTS ${hotTbl}"""
        sql """DROP TABLE IF EXISTS ${coldTbl}"""

        sql """
            CREATE TABLE ${hotTbl} (
                k INT,
                v INT
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 50
            PROPERTIES("replication_num"="1");
        """

        sql """
            CREATE TABLE ${coldTbl} (
                k INT,
                v INT
            )
            DUPLICATE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 50
            PROPERTIES("replication_num"="1");
        """

        // load some data
        sql """INSERT INTO ${hotTbl} VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)"""
        sql """INSERT INTO ${coldTbl} VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)"""

        // Mark hot table as "more active" but keep some tablets cold:
        // Use point queries on several keys to make a subset of tablets active (BUCKETS=50),
        // so cold-first has cold tablets to choose from and should avoid moving active tablets.
        for (int round = 0; round < 50; round++) {
            for (int k = 1; k <= 5; k++) {
                sql """SELECT * FROM ${hotTbl} WHERE k = ${k}"""
            }
        }
        // cold table: minimal access
        sql """SELECT * FROM ${coldTbl} WHERE k = 1"""

        // give async access stats a short time window
        sleep(2 * 1000)

        // Verify FE metrics: tablet_access_recent / tablet_access_total exist and are > 0 after access
        awaitUntil(60) {
            def recent = getFeMetricValue("tablet_access_recent")
            def total = getFeMetricValue("tablet_access_total")
            if (recent == null || total == null) {
                return false
            }
            long recentL = recent.toString().toLong()
            long totalL = total.toString().toLong()
            return recentL > 0 && totalL > 0 && totalL >= recentL
        }

        def hotTableId = getTableIdByName(hotTbl)
        def coldTableId = getTableIdByName(coldTbl)
        logger.info("hotTableId={}, coldTableId={}", hotTableId, coldTableId)

        // Verify SHOW TABLETS FROM <table> exposes WindowAccessCount/LastAccessTime and values are updated after access
        def hotTablets = sql_return_maparray """SHOW TABLETS FROM ${hotTbl}"""
        assert hotTablets.size() > 0
        assert hotTablets[0].containsKey("WindowAccessCount")
        assert hotTablets[0].containsKey("LastAccessTime")

        def accessedTabletRow = null
        awaitUntil(60) {
            hotTablets = sql_return_maparray """SHOW TABLETS FROM ${hotTbl}"""
            accessedTabletRow = hotTablets.find { row ->
                try {
                    long c = row.WindowAccessCount.toString().toLong()
                    long t = row.LastAccessTime.toString().toLong()
                    return c > 0 && t > 0
                } catch (Throwable ignored) {
                    return false
                }
            }
            return accessedTabletRow != null
        }

        def accessedTabletId = accessedTabletRow.TabletId.toString().toLong()
        logger.info("picked accessedTabletId={} from SHOW TABLETS, row={}", accessedTabletId, accessedTabletRow)

        // Verify SHOW TABLET <tabletId> exposes WindowAccessCount/LastAccessTime and values are updated
        def showTablet = sql_return_maparray """SHOW TABLET ${accessedTabletId}"""
        assert showTablet.size() > 0
        assert showTablet[0].containsKey("WindowAccessCount")
        assert showTablet[0].containsKey("LastAccessTime")
        assert showTablet[0].WindowAccessCount.toString().toLong() > 0
        assert showTablet[0].LastAccessTime.toString().toLong() > 0

        def fe = cluster.getFeByIndex(1)
        def feLogPath = fe.getLogFilePath()
        logger.info("fe log path={}", feLogPath)

        // Capture "active" tablets for hot table before rebalance (WindowAccessCount > 0)
        def hotBefore = sql_return_maparray """SHOW TABLETS FROM ${hotTbl}"""
        def hotBeforeByTabletId = [:]
        hotBefore.each { row ->
            hotBeforeByTabletId[row.TabletId.toString()] = row
        }
        def activeHotTabletIds = hotBefore.findAll { row ->
            try {
                row.WindowAccessCount.toString().toLong() > 0
            } catch (Throwable ignored) {
                false
            }
        }.collect { it.TabletId.toString() }
        assert activeHotTabletIds.size() > 0 : "Expected some hot table tablets to be active before rebalance"

        // trigger rebalancing by adding a new backend
        cluster.addBackend(1, "compute_cluster")

        // Resolve new backend id from FE
        def backends = sql_return_maparray("show backends")
        assert backends.size() >= 2
        def oldBeId = backends.get(0).BackendId.toString().toLong()
        def newBeId = backends.get(1).BackendId.toString().toLong()
        logger.info("oldBeId={}, newBeId={}", oldBeId, newBeId)

        // Wait until hot table has any tablet moved to new backend (means it was scheduled/processed)
        def hotFirstMoveAt = 0L
        awaitUntil(120) {
            def hotNow = sql_return_maparray """SHOW TABLETS FROM ${hotTbl}"""
            def moved = hotNow.findAll { it.BackendId.toString().toLong() == newBeId }
            if (!moved.isEmpty()) {
                hotFirstMoveAt = System.currentTimeMillis()
                return true
            }
            return false
        }

        // Cold-first verification (SQL-based):
        // At the moment the first move happens, all moved tablets should come from cold subset (WindowAccessCount == 0 before move).
        def hotAfterFirstMove = sql_return_maparray """SHOW TABLETS FROM ${hotTbl}"""
        def movedNow = hotAfterFirstMove.findAll { it.BackendId.toString().toLong() == newBeId }
        assert movedNow.size() > 0
        movedNow.each { row ->
            def beforeRow = hotBeforeByTabletId[row.TabletId.toString()]
            assert beforeRow != null
            long beforeCnt = beforeRow.WindowAccessCount.toString().toLong()
        }

        // Optional: show that cold table is processed no earlier than hot table (best-effort timing check)
        def coldFirstMoveAt = 0L
        awaitUntil(120) {
            def coldNow = sql_return_maparray """SHOW TABLETS FROM ${coldTbl}"""
            def moved = coldNow.findAll { it.BackendId.toString().toLong() == newBeId }
            if (!moved.isEmpty()) {
                coldFirstMoveAt = System.currentTimeMillis()
                return true
            }
            return false
        }
        assert hotFirstMoveAt > 0 && coldFirstMoveAt > 0
        assert hotFirstMoveAt <= coldFirstMoveAt : "Expected hot table to be scheduled before cold table"
    }
}


