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

suite("test_filecache_with_base_compaction", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    options.beConfigs.add('enable_flush_file_cache_async=false')
    options.beConfigs.add('file_cache_enter_disk_resource_limit_mode_percent=99')
    options.beConfigs.add('enable_evict_file_cache_in_advance=false')
    options.beConfigs.add('')

    def testTable = "test_filecache_with_base_compaction"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_backendBrpcPort = [:]

    def triggerCumulativeCompaction = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        def (code_1, out_1, err_1) = be_run_cumulative_compaction(be_host, be_http_port, tablet_id)
        logger.info("Run compaction: code=" + code_1 + ", out=" + out_1 + ", err=" + err_1)
        assertEquals(code_1, 0)
        return out_1
    }

    def triggerBaseCompaction = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        def (code_1, out_1, err_1) = be_run_base_compaction(be_host, be_http_port, tablet_id)
        logger.info("Run compaction: code=" + code_1 + ", out=" + out_1 + ", err=" + err_1)
        assertEquals(code_1, 0)
        return out_1
    }

    def getTabletStatus = { tablet ->
        String tablet_id = tablet.TabletId
        String trigger_backend_id = tablet.BackendId
        def be_host = backendId_to_backendIP[trigger_backend_id]
        def be_http_port = backendId_to_backendHttpPort[trigger_backend_id]
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/compaction/show?tablet_id=")
        sb.append(tablet_id)

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get tablet status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def tabletStatus = parseJson(out.trim())
        return tabletStatus
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

    docker(options) {
        def fes = sql_return_maparray "show frontends"
        logger.info("frontends: ${fes}")
        def url = "jdbc:mysql://${fes[0].Host}:${fes[0].QueryPort}/"
        logger.info("url: " + url)

        def result = sql 'SELECT DATABASE()'

        sql """ DROP TABLE IF EXISTS ${testTable} """

        sql """ CREATE TABLE ${testTable}
                (
                    siteid INT DEFAULT '10',
                    citycode SMALLINT NOT NULL,
                    username VARCHAR(32) DEFAULT '',
                    pv BIGINT DEFAULT '0'
                )
                DUPLICATE KEY(siteid, citycode, username)
                DISTRIBUTED BY HASH(siteid) BUCKETS 1
                PROPERTIES (
                "disable_auto_compaction" = "true"
                )
        """

        // getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
        getBackendIpHttpAndBrpcPort(backendId_to_backendIP, backendId_to_backendHttpPort, backendId_to_backendBrpcPort);

        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId

        sql """ insert into ${testTable}(siteid, citycode, username, pv) values (1, 1, "xxx", 1); """
        sql """ delete from ${testTable} where siteid=2; """
        sql """ delete from ${testTable} where siteid=2; """
        sql """ delete from ${testTable} where siteid=2; """
        sql """ delete from ${testTable} where siteid=2; """
        sql """ delete from ${testTable} where siteid=2; """
        sql """ delete from ${testTable} where siteid=2; """
        sql """ delete from ${testTable} where siteid=2; """

        sql """ sync """
        sql "select * from ${testTable}"
        def tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")

        triggerCumulativeCompaction(tablet)
        waitForCompaction(tablet)
        triggerCumulativeCompaction(tablet)
        waitForCompaction(tablet)
        triggerBaseCompaction(tablet)
        waitForCompaction(tablet)

        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")
        def base_compaction_finished = false
        Set<String> final_rowsets = new HashSet<>();
        for (int i = 0; i < 100; i++) {
            tablet_status = getTabletStatus(tablet)
            if (tablet_status["rowsets"].size() == 2) {
                base_compaction_finished = true
                final_rowsets.addAll(tablet_status["rowsets"])
                break
            }
            sleep(500)
        }
        assertTrue(base_compaction_finished)

        def be_host = backendId_to_backendIP[tablet.BackendId]
        def be_http_port = backendId_to_backendHttpPort[tablet.BackendId]

        for (int i = 0; i < final_rowsets.size(); i++) {
            def rowsetStr = final_rowsets[i]
            def start_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[0].toInteger()
            def end_version = rowsetStr.split(" ")[0].replace('[', '').replace(']', '').split("-")[1].toInteger()
            def rowset_id = rowsetStr.split(" ")[4]
            if (start_version == 0) {
                continue
            }

            logger.info("final rowset ${i}, start: ${start_version}, end: ${end_version}, id: ${rowset_id}")
            def data = Http.GET("http://${be_host}:${be_http_port}/api/file_cache?op=list_cache&value=${rowset_id}_0.dat", true)
            logger.info("file cache data: ${data}")
            assertTrue(data.size() > 0)
        }
    }
}
