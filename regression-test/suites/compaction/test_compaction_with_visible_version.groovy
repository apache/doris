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
import org.apache.doris.regression.util.Http
import org.apache.doris.regression.util.NodeType

suite('test_compaction_with_visible_version', 'docker') {
    def options = new ClusterOptions()
    def compaction_keep_invisible_version_min_count = 50L
    options.feConfigs += [
        'partition_info_update_interval_secs=5',
    ]
    options.beConfigs += [
        'report_tablet_interval_seconds=1',
        'tablet_rowset_stale_sweep_by_size=true',
        'tablet_rowset_stale_sweep_threshold_size=0',
        'compaction_keep_invisible_version_timeout_sec=6000',
        "compaction_keep_invisible_version_min_count=${compaction_keep_invisible_version_min_count}".toString(),
        'compaction_keep_invisible_version_max_count=500',
    ]
    options.enableDebugPoints()

    docker(options) {
        def E_CUMULATIVE_NO_SUITABLE_VERSION = 'E-2000'
        def E_FULL_MISS_VERSION = 'E-2009'

        sql 'SET GLOBAL insert_visible_timeout_ms = 3000'

        def tableName = 'test_compaction_with_visible_version'
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

        def triggerCompaction = { tablet, isCompactSucc, compaction_type ->
            def tabletId = tablet.TabletId
            def backendId = tablet.BackendId
            def backendIp = backendId_to_backendIP.get(backendId)
            def backendHttpPort = backendId_to_backendHttpPort.get(backendId)
            def code
            def out
            def err
            if (compaction_type == 'base') {
                (code, out, err) = be_run_base_compaction(backendIp, backendHttpPort, tabletId)
            } else {
                (code, out, err) = be_run_cumulative_compaction(backendIp, backendHttpPort, tabletId)
            }
            logger.info("Run compaction: code=${code}, out=${out}, err=${err}")
            assertEquals(0, code)
            def compactJson = parseJson(out.trim())
            if (isCompactSucc) {
                assertEquals('success', compactJson.status.toLowerCase())
            } else {
                if (compaction_type == 'base') {
                    assertEquals(E_FULL_MISS_VERSION, compactJson.status)
                } else {
                    assertEquals(E_CUMULATIVE_NO_SUITABLE_VERSION, compactJson.status)
                }
            }
        }

        def waitCompaction = { tablet, startTs ->
            def tabletId = tablet.TabletId
            def backendId = tablet.BackendId
            def backendIp = backendId_to_backendIP.get(backendId)
            def backendHttpPort = backendId_to_backendHttpPort.get(backendId)
            def running = true
            while (running) {
                assertTrue(System.currentTimeMillis() - startTs < 60 * 1000)
                Thread.sleep(1000)
                def (code, out, err) = be_get_compaction_status(backendIp, backendHttpPort, tabletId)
                logger.info("Get compaction: code=${code}, out=${out}, err=${err}")
                assertEquals(0, code)

                def compactionStatus = parseJson(out.trim())
                assertEquals('success', compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            }
        }

        def checkCompact = { isCumuCompactSucc, runBaseCompact, isInvisibleTimeout, version, visibleVersion ->
            def partition = sql_return_maparray("SHOW PARTITIONS FROM ${tableName}")[0]
            assertEquals(visibleVersion, partition.VisibleVersion as long)

            // wait be report version count
            Thread.sleep(3 * 1000)
            def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
            def lastInvisibleVersionCountMap = [:]
            def lastVisibleVersionCountMap = [:]
            tablets.each {
                lastVisibleVersionCountMap[it.BackendId] = it.VisibleVersionCount as long
                lastInvisibleVersionCountMap[it.BackendId] =
                        (it.VersionCount as long) - (it.VisibleVersionCount as long)
                triggerCompaction it, isCumuCompactSucc, 'cumulative'
            }

            if (isCumuCompactSucc) {
                // wait compaction done
                def startTs = System.currentTimeMillis()
                tablets.each {
                    waitCompaction it, startTs
                }
            }

            if (runBaseCompact) {
                tablets.each {
                    triggerCompaction it, true, 'base'
                }

                def startTs = System.currentTimeMillis()
                tablets.each {
                    waitCompaction it, startTs
                }
            }

            // wait report
            Thread.sleep(3 * 1000)

            tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
            tablets.each {
                def backendId = it.BackendId
                def visibleVersionCount = it.VisibleVersionCount as long
                def totalVersionCount = it.VersionCount as long
                def invisibleVersionCount = totalVersionCount - visibleVersionCount
                assertEquals(version, it.Version as long)

                if (isInvisibleTimeout) {
                    def values = [Math.min(version - visibleVersion, compaction_keep_invisible_version_min_count),
                    Math.min(version - visibleVersion, compaction_keep_invisible_version_min_count + 1)]

                    // part of invisible version was compact
                    assertTrue(invisibleVersionCount in values,
                            "not match, invisibleVersionCount: ${invisibleVersionCount}, candidate values: ${values}")
                } else {
                    // invisible version couldn't compact
                    assertEquals(version - visibleVersion, invisibleVersionCount)
                }

                def lastVisibleVersionCount = lastVisibleVersionCountMap.get(backendId)
                def lastInvisibleVersionCount = lastInvisibleVersionCountMap.get(backendId)
                if (isCumuCompactSucc) {
                    assertTrue(lastInvisibleVersionCount > invisibleVersionCount || lastInvisibleVersionCount <= 1,
                            "not met with: lastInvisibleVersionCount ${lastInvisibleVersionCount} > "
                            + "invisibleVersionCount ${invisibleVersionCount}")
                    if (runBaseCompact) {
                        assertEquals(1L, visibleVersionCount)
                    }
                } else {
                    assertEquals(lastVisibleVersionCount, visibleVersionCount)
                }
            }
        }

        sql """
            CREATE TABLE ${tableName} (k1 int, k2 int) DISTRIBUTED BY HASH(k1) BUCKETS 1 PROPERTIES (
                "disable_auto_compaction" = "true"
            )
        """

        // normal
        def rowNum = 0L
        def insertNRecords = { num ->
            // if enable debug point DatabaseTransactionMgr.stop_finish_transaction,
            // insert will need to wait insert_visible_timeout_ms.
            // so use multiple threads to reduce the wait time.
            def futures = []
            for (def i = 0; i < num; i++, rowNum++) {
                def index = rowNum
                futures.add(thread {
                    sql " INSERT INTO ${tableName} VALUES (${index}, ${index * 10}) "
                })
            }
            futures.each { it.get() }
        }
        insertNRecords(21)
        // after insert 21 rows, be can run compact ok.
        checkCompact(true, false, false, rowNum + 1, rowNum + 1)
        qt_select_1 "SELECT * FROM ${tableName} ORDER BY k1"

        // publish but not visible
        def lastRowNum = rowNum
        cluster.injectDebugPoints(NodeType.FE, ['DatabaseTransactionMgr.stop_finish_transaction':null])
        insertNRecords(21)
        // after enable debugpoint, be will add rowsets, but visible version will not increase.
        // then no rowsets can pick to compact.
        // so expect compact failed.
        checkCompact(false, false, false, rowNum + 1, lastRowNum + 1)
        qt_select_2 "SELECT * FROM ${tableName} ORDER BY k1"

        cluster.clearFrontendDebugPoints()
        Thread.sleep(5000)
        // after clear debug point, visible version will increase.
        // then some rowsets can pick to compact.
        // so expect compact succ.
        checkCompact(true, true, false, rowNum + 1, rowNum + 1)
        qt_select_3 "SELECT * FROM ${tableName} ORDER BY k1"

        lastRowNum = rowNum
        cluster.injectDebugPoints(NodeType.FE, ['DatabaseTransactionMgr.stop_finish_transaction':null])
        insertNRecords(80)
        // 80 versions are not invisible yet,  BE will not compact them.
        // if we send http to compact them,  BE will reply no rowsets can compact now
        checkCompact(false, false, false, rowNum + 1, lastRowNum + 1)
        // Because BE not compact, so query should be ok.
        qt_select_4 "SELECT * FROM ${tableName} ORDER BY k1"

        update_all_be_config('compaction_keep_invisible_version_timeout_sec', 1)
        checkCompact(true, false, true, rowNum + 1, lastRowNum + 1)
        qt_select_5 "SELECT * FROM ${tableName} ORDER BY k1"

        def getVersionCountMap = { ->
            def versionCountMap = [:]
            def tablets = sql_return_maparray "SHOW TABLETS FROM ${tableName}"
            tablets.each {
                versionCountMap.put(it.BackendId as long, [it.VisibleVersionCount as long, it.VersionCount as long])
            }
            return versionCountMap
        }

        // after backend restart, it should update its visible version from FE
        // and then it report its visible version count and total version count
        def oldVersionCountMap = getVersionCountMap()
        cluster.restartBackends()
        Thread.sleep(20000)
        def newVersionCountMap = getVersionCountMap()
        assertEquals(oldVersionCountMap, newVersionCountMap)

        cluster.clearFrontendDebugPoints()
        Thread.sleep(5000)
        // after clear fe's debug point, the 80 version are visible now.
        // so compact is ok
        checkCompact(true, false, false, rowNum + 1, rowNum + 1)
        qt_select_6 "SELECT * FROM ${tableName} ORDER BY k1"

        cluster.injectDebugPoints(NodeType.FE, ['DatabaseTransactionMgr.stop_finish_transaction':null])
        def compaction_keep_invisible_version_timeout_sec = 1
        compaction_keep_invisible_version_min_count = 0L
        update_all_be_config('compaction_keep_invisible_version_timeout_sec', compaction_keep_invisible_version_timeout_sec)
        update_all_be_config('compaction_keep_invisible_version_min_count', compaction_keep_invisible_version_min_count)

        lastRowNum = rowNum
        insertNRecords(21)

        Thread.sleep((compaction_keep_invisible_version_timeout_sec + 1) * 1000)

        // after compaction_keep_invisible_version_timeout_sec,
        // all version had been compact (compaction_keep_invisible_version_min_count=0),
        checkCompact(true, false, true, rowNum + 1, lastRowNum + 1)

        // visible version had been compact
        test {
            sql "SELECT * FROM ${tableName} ORDER BY k1"

            // E-230:
            //(1105, 'errCode = 2, detailMessage = (128.2.51.2)[CANCELLED]missed_versions is empty, spec_version 43,
            // max_version 123, tablet_id 10062')
            exception 'missed_versions is empty'
        }

        cluster.clearFrontendDebugPoints()
        Thread.sleep(5000)
        qt_select_7 "SELECT * FROM ${tableName} ORDER BY k1"
    }
}
