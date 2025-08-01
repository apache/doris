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
import org.apache.doris.regression.util.NodeType

suite("test_cloud_full_compaction_multi_segments","multi_cluster,docker") {
    if (!isCloudMode()) {
        return
    }

    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'calculate_delete_bitmap_task_timeout_seconds=10',
        'mow_calculate_delete_bitmap_retry_times=10',
        'enable_workload_group=false',
    ]
    options.beConfigs += [
        'doris_scanner_row_bytes=1' // to cause multi segments
    ]
    docker(options) {
        def tableName = "test_cloud_full_compaction_multi_segments"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` int ,
                `v` int ,
                `c` int
            ) engine=olap
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            properties(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "function_column.sequence_col" = 'c');"""

        sql """ INSERT INTO ${tableName} VALUES (-2,-2,100)"""
        sql """ INSERT INTO ${tableName} VALUES (-1,-1,100)"""
        qt_sql """ SELECT * FROM ${tableName} ORDER BY k; """

        def tabletStats = sql_return_maparray("show tablets from ${tableName};")
        def tabletId = tabletStats[0].TabletId
        def tabletBackendId = tabletStats[0].BackendId
        def tabletBackend
        def backends = sql_return_maparray('show backends')
        for (def be : backends) {
            if (be.BackendId == tabletBackendId) {
                tabletBackend = be
                break;
            }
        }
        logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");

        def checkSegmentNum = { rowsetNum, lastRowsetSegmentNum ->
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """
            logger.info("tablets: ${tablets}")
            String compactionUrl = tablets[0]["CompactionStatus"]
            def (code, out, err) = curl("GET", compactionUrl)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            assert tabletJson.rowsets.size() == rowsetNum + 1
            def rowset = tabletJson.rowsets.get(tabletJson.rowsets.size() - 1)
            logger.info("rowset: ${rowset}")
            int start_index = rowset.indexOf("]")
            int end_index = rowset.indexOf("DATA")
            def segmentNumStr = rowset.substring(start_index + 1, end_index).trim()
            logger.info("segmentNumStr: ${segmentNumStr}")
            assert lastRowsetSegmentNum == Integer.parseInt(segmentNumStr)
        }

        def loadMultiSegmentData = { rows->
            // load data that will have multi segments and there are duplicate keys between segments
            String content = ""
            (1..rows).each {
                content += "${it},${it},${it}\n"
            }
            content += content

            // key=-1, in segment 0, sequence column is smaller than existing row in table
            // full compaction will generate delete bitmap mark on this segment when calculating for incremental rowset
            content = "-1,-1,1\n${content}"
            streamLoad {
                table "${tableName}"
                set 'column_separator', ','
                inputStream new ByteArrayInputStream(content.getBytes())
                time 30000
            }
        }

        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()

        try {
            // batch_size is 4164 in csv_reader.cpp
            // _batch_size is 8192 in vtablet_writer.cpp
            // to cause multi segments
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")

            // block load and compaction
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
            GetDebugPoint().enableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")

            def newThreadInDocker = { Closure actionSupplier ->
                def connInfo = context.threadLocalConn.get()
                return Thread.start {
                    connect(connInfo.username, connInfo.password, connInfo.conn.getMetaData().getURL(), actionSupplier)
                }
            }

            def t1 = newThreadInDocker {
                loadMultiSegmentData(4096)
            }

            // trigger full compaction
            logger.info("trigger full compaction on BE ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
            def (code, out, err) = be_run_full_compaction(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assert  code == 0
            def compactJson = parseJson(out.trim())
            assert "success" == compactJson.status.toLowerCase()


            Thread.sleep(1000)

            // let the load publish
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
            t1.join()
            Thread.sleep(1000)


            // let full compaction continue and wait for compaction to finish
            GetDebugPoint().disableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")
            def running = true
            do {
                Thread.sleep(1000)
                (code, out, err) = be_get_compaction_status(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)


            // use new cluster to force it read delete bitmaps from MS rather than BE's cache
            cluster.addBackend(1, "cluster1")
            sql """ use @cluster1 """
            qt_dup_key_count "select count() from (select k, count(*) from ${tableName} group by k having count(*) > 1) t"
            qt_sql "select count() from ${tableName};"
            checkSegmentNum(2, 3)
        } catch (Exception e) {
            logger.info(e.getMessage())
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
