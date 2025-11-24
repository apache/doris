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

import java.util.concurrent.atomic.AtomicBoolean
import org.apache.doris.regression.suite.ClusterOptions
import org.codehaus.groovy.runtime.IOGroovyMethods
import org.apache.doris.regression.util.Http

suite("test_filecache_compaction_multisegments_and_read_stale_cloud_docker", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)
    options.enableDebugPoints()
    options.feConfigs.add("enable_workload_group=false")
    options.beConfigs.add('compaction_promotion_version_count=5')
    options.beConfigs.add('tablet_rowset_stale_sweep_time_sec=0')
    options.beConfigs.add('vacuum_stale_rowsets_interval_s=10')
    options.beConfigs.add('enable_java_support=false')
    options.beConfigs.add('doris_scanner_row_bytes=1')

    def dbName = ""
    def testTable = "test_filecache_multisegments_and_read_stale"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_backendBrpcPort = [:]

    def triggerCompaction = { tablet ->
        def compact_type = "cumulative"
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        if (compact_type == "cumulative") {
            def (code_1, out_1, err_1) = be_run_cumulative_compaction(be_host, be_http_port, tablet_id)
            logger.info("Run compaction: code=" + code_1 + ", out=" + out_1 + ", err=" + err_1)
            assertEquals(code_1, 0)
            return out_1
        } else if (compact_type == "full") {
            def (code_2, out_2, err_2) = be_run_full_compaction(be_host, be_http_port, tablet_id)
            logger.info("Run compaction: code=" + code_2 + ", out=" + out_2 + ", err=" + err_2)
            assertEquals(code_2, 0)
            return out_2
        } else {
            assertFalse(True)
        }
    }

    def getTabletStatus = { tablet, rowsetIndex, lastRowsetSegmentNum, enableAssert = false, outputRowsets = null ->
        String compactionUrl = tablet["CompactionStatus"]
        def (code, out, err) = curl("GET", compactionUrl)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        if (outputRowsets != null) {
            outputRowsets.addAll(tabletJson.rowsets)
        }

        assertTrue(tabletJson.rowsets.size() >= rowsetIndex)
        def rowset = tabletJson.rowsets.get(rowsetIndex - 1)
        logger.info("rowset: ${rowset}")
        int start_index = rowset.indexOf("]")
        int end_index = rowset.indexOf("DATA")
        def segmentNumStr = rowset.substring(start_index + 1, end_index).trim()
        logger.info("segmentNumStr: ${segmentNumStr}")
        if (enableAssert) {
            assertEquals(lastRowsetSegmentNum, Integer.parseInt(segmentNumStr))
        } else {
            return lastRowsetSegmentNum == Integer.parseInt(segmentNumStr);
        }
    }

    def waitForCompaction = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        def running = true
        do {
            Thread.sleep(1000)
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X GET http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run_status?tablet_id=")
            sb.append(tablet_id)

            String command = sb.toString()
            logger.info(command)
            def process = command.execute()
            def code = process.waitFor()
            def out = process.getText()
            logger.info("Get compaction status: code=" + code + ", out=" + out)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())
            running = compactionStatus.run_status
        } while (running)
    }

    def getLocalDeleteBitmapStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_local?verbose=true&tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get local delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    def getMsDeleteBitmapStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_ms?verbose=true&tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get ms delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    docker(options) {
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")
        def url = "jdbc:mysql://${fes[0].Host}:${fes[0].QueryPort}/"
        logger.info("url: " + url)
        AtomicBoolean query_result = new AtomicBoolean(true)
        def query = {
            connect( context.config.jdbcUser,  context.config.jdbcPassword,  url) {
                logger.info("query start")
                def results = sql_return_maparray """ select * from ${dbName}.${testTable}; """
                logger.info("query result: " + results)
                Set<String> keys = new HashSet<>()
                for (final def result in results) {
                    if (keys.contains(result.k)) {
                        logger.info("find duplicate key: " + result.k)
                        query_result.set(false)
                        break
                    }
                    keys.add(result.k)
                }
                logger.info("query finish. query_result: " + query_result.get())
            }
        }

        def result = sql 'SELECT DATABASE()'
        dbName = result[0][0]

        sql """ DROP TABLE IF EXISTS ${testTable} """
        sql """ CREATE TABLE IF NOT EXISTS ${testTable} (
            `k1` int(11) NULL, 
            `k2` int(11) NULL, 
            `v3` int(11) NULL,
            `v4` int(11) NULL
        ) unique KEY(`k1`, `k2`) 
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
            );
        """

        // getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
        getBackendIpHttpAndBrpcPort(backendId_to_backendIP, backendId_to_backendHttpPort, backendId_to_backendBrpcPort);

        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId
        def backend_id = tablet.BackendId

        GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
        GetDebugPoint().enableDebugPointForAllBEs("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset")
        GetDebugPoint().enableDebugPointForAllBEs("Tablet.delete_expired_stale_rowset.start_delete_unused_rowset")

        Set<String> all_history_stale_rowsets = new HashSet<>();
        try {
            // load 1
            streamLoad {
                table "${testTable}"
                set 'column_separator', ','
                set 'compress_type', 'GZ'
                file 'test_schema_change_add_key_column.csv.gz'
                time 10000 // limit inflight 10s

                check { res, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(res)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(8192, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"
            def rowCount1 = sql """ select count() from ${testTable}; """
            logger.info("rowCount1: ${rowCount1}")
            // check generate 3 segments
            getTabletStatus(tablet, 2, 3, true, all_history_stale_rowsets)

            // trigger compaction
            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                    [tablet_id: "${tablet.TabletId}", start_version: 2, end_version: 2])
            def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            logger.info("compact json: " + compactJson)
            // check generate 1 segments
            for (int i = 0; i < 20; i++) {
                if (getTabletStatus(tablet, 2, 1, false, all_history_stale_rowsets)) {
                    break
                }
                sleep(100)
            }
            getTabletStatus(tablet, 2, 1, false, all_history_stale_rowsets)
            sql """ select * from ${testTable} limit 1; """

            // load 2
            streamLoad {
                table "${testTable}"
                set 'column_separator', ','
                set 'compress_type', 'GZ'
                file 'test_schema_change_add_key_column1.csv.gz'
                time 10000 // limit inflight 10s

                check { res, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    def json = parseJson(res)
                    assertEquals("success", json.Status.toLowerCase())
                    assertEquals(20480, json.NumberTotalRows)
                    assertEquals(0, json.NumberFilteredRows)
                }
            }
            sql "sync"
            def rowCount2 = sql """ select count() from ${testTable}; """
            logger.info("rowCount2: ${rowCount2}")
            // check generate 3 segments
            getTabletStatus(tablet, 3, 6, false, all_history_stale_rowsets)
            def local_dm = getLocalDeleteBitmapStatus(tablet)
            logger.info("local delete bitmap 1: " + local_dm)

            // trigger compaction for load 2
            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                    [tablet_id: "${tablet.TabletId}", start_version: 3, end_version: 3])
            (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            compactJson = parseJson(out.trim())
            logger.info("compact json: " + compactJson)
            waitForCompaction(tablet)
            // check generate 1 segments
            for (int i = 0; i < 20; i++) {
                if (getTabletStatus(tablet, 3, 1, false, all_history_stale_rowsets)) {
                    break
                }
                sleep(100)
            }
            getTabletStatus(tablet, 3, 1, false, all_history_stale_rowsets)

            GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.vacuum_stale_rowsets") // cloud
            GetDebugPoint().enableDebugPointForAllBEs("DeleteBitmapAction._handle_show_local_delete_bitmap_count.start_delete_unused_rowset") // local
            local_dm = getLocalDeleteBitmapStatus(tablet)
            logger.info("local delete bitmap 2: " + local_dm)
            assertEquals(1, local_dm["delete_bitmap_count"])


            // sleep for vacuum_stale_rowsets_interval_s=10 seconds to wait for unused rowsets are deleted
            sleep(21000)

            def be_host = backendId_to_backendIP[tablet.BackendId]
            def be_http_port = backendId_to_backendHttpPort[tablet.BackendId]
            logger.info("be_host: ${be_host}, be_http_port: ${be_http_port}, BrpcPort: ${backendId_to_backendBrpcPort[tablet.BackendId]}")

            for (int i = 0; i < all_history_stale_rowsets.size(); i++) {
                def rowsetStr = all_history_stale_rowsets[i]
                // [12-12] 1 DATA NONOVERLAPPING 02000000000000124843c92c13625daa8296c20957119893 1011.00 B
                def start_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[0].toInteger()
                def end_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[1].toInteger()
                def rowset_id = rowsetStr.split(" ")[4]
                if (start_version == 0) {
                    continue
                }

                int start_index = rowsetStr.indexOf("]")
                int end_index = rowsetStr.indexOf("DATA")
                def segmentNum = rowsetStr.substring(start_index + 1, end_index).trim().toInteger()

                logger.info("rowset ${i}, start: ${start_version}, end: ${end_version}, id: ${rowset_id}, segment: ${segmentNum}")
                def data = Http.GET("http://${be_host}:${be_http_port}/api/file_cache?op=list_cache&value=${rowset_id}_0.dat", true)
                logger.info("file cache data: ${data}")
                if (segmentNum <= 1) {
                    assertTrue(data.size() > 0)
                } else {
                    assertTrue(data.size() == 0)
                }
            }

            def (code_0, out_0, err_0) = curl("GET", "http://${be_host}:${backendId_to_backendBrpcPort[tablet.BackendId]}/vars/unused_rowsets_count")
            logger.info("out_0: ${out_0}")
            def unusedRowsetsCount = out_0.trim().split(":")[1].trim().toInteger()
            assertEquals(0, unusedRowsetsCount)
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
