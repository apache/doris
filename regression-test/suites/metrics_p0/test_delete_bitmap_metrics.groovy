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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_delete_bitmap_metrics", "p0") {
    if (!isCloudMode()) {
        return
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string: [:]]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_param = { paramName, paramValue ->
        // for eache be node, set paramName=paramValue
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }

    def reset_be_param = { paramName ->
        // for eache be node, reset paramName to default
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def original_value = backendId_to_params.get(id).get(paramName)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, original_value))
            assertTrue(out.contains("OK"))
        }
    }

    def get_be_param = { paramName ->
        // for eache be node, get param value by default
        def paramValue = ""
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            // get the config value from be
            def (code, out, err) = curl("GET", String.format("http://%s:%s/api/show_config?conf_item=%s", beIp, bePort, paramName))
            assertTrue(code == 0)
            assertTrue(out.contains(paramName))
            // parsing
            def resultList = parseJson(out)[0]
            assertTrue(resultList.size() == 4)
            // get original value
            paramValue = resultList[2]
            backendId_to_params.get(id, [:]).put(paramName, paramValue)
        }
    }

    def getLocalDeleteBitmapStatus = { be_host, be_http_port, tablet_id ->
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_local?tablet_id=")
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

    def getMSDeleteBitmapStatus = { be_host, be_http_port, tablet_id ->
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_ms?tablet_id=")
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

    def getAggCacheDeleteBitmapStatus = { be_host, be_http_port, tablet_id, boolean verbose=false ->
        boolean running = true
        StringBuilder sb = new StringBuilder();
        sb.append("curl -X GET http://${be_host}:${be_http_port}")
        sb.append("/api/delete_bitmap/count_agg_cache?tablet_id=")
        sb.append(tablet_id)
        if (verbose) {
            sb.append("&verbose=true")
        }

        String command = sb.toString()
        logger.info(command)
        def process = command.execute()
        def code = process.waitFor()
        def out = process.getText()
        logger.info("Get agg cache delete bitmap count status:  =" + code + ", out=" + out)
        assertEquals(code, 0)
        def deleteBitmapStatus = parseJson(out.trim())
        return deleteBitmapStatus
    }

    String[][] backends = sql """ show backends """
    assertTrue(backends.size() > 0)
    String backendId;
    def backendIdToBackendIP = [:]
    def backendIdToBackendBrpcPort = [:]
    for (String[] backend in backends) {
        if (backend[9].equals("true")) {
            backendIdToBackendIP.put(backend[0], backend[1])
            backendIdToBackendBrpcPort.put(backend[0], backend[5])
        }
    }
    backendId = backendIdToBackendIP.keySet()[0]
    def getMetricsMethod = { check_func ->
        httpTest {
            endpoint backendIdToBackendIP.get(backendId) + ":" + backendIdToBackendBrpcPort.get(backendId)
            uri "/brpc_metrics"
            op "get"
            check check_func
        }
    }

    def testTable = "test_delete_bitmap_metrics"
    def timeout = 10000
    sql """ DROP TABLE IF EXISTS ${testTable}"""
    def testTableDDL = """
        create table ${testTable}
            (
            `plan_id` bigint(20) NOT NULL,
            `target_id` int(20) NOT NULL,
            `target_name` varchar(255) NOT NULL
            )
            ENGINE=OLAP
            UNIQUE KEY(`plan_id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`plan_id`) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true"
            );
    """
    sql testTableDDL
    get_be_param("check_tablet_delete_bitmap_interval_seconds")
    try {
        set_be_param("check_tablet_delete_bitmap_interval_seconds", "10")
        sql "sync"

        sql "sync"
        sql """ INSERT INTO ${testTable} VALUES (0,0,'1'),(1,1,'1'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'2'),(2,2,'2'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'3'),(3,3,'3'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'4'),(4,4,'4'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'5'),(5,5,'5'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'6'),(6,6,'6'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'7'),(7,7,'7'); """
        sql """ INSERT INTO ${testTable} VALUES (0,0,'8'),(8,8,'8'); """

        qt_sql "select * from ${testTable} order by plan_id"


        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        def local_delete_bitmap_count = 0
        def ms_delete_bitmap_count = 0
        def local_delete_bitmap_cardinality = 0;
        def ms_delete_bitmap_cardinality = 0;
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def tablet_info = sql_return_maparray """ show tablet ${tablet_id}; """
            logger.info("tablet: " + tablet_info)
            String trigger_backend_id = tablet.BackendId

            // before compaction, delete_bitmap_count is (rowsets num - 1)
            local_delete_bitmap_count = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
            local_delete_bitmap_cardinality = getLocalDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
            logger.info("local_delete_bitmap_count:" + local_delete_bitmap_count)
            logger.info("local_delete_bitmap_cardinality:" + local_delete_bitmap_cardinality)
            assertTrue(local_delete_bitmap_count == 7)
            assertTrue(local_delete_bitmap_cardinality == 7)

            if (isCloudMode()) {
                ms_delete_bitmap_count = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).delete_bitmap_count
                ms_delete_bitmap_cardinality = getMSDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id).cardinality
                logger.info("ms_delete_bitmap_count:" + ms_delete_bitmap_count)
                logger.info("ms_delete_bitmap_cardinality:" + ms_delete_bitmap_cardinality)
                assertTrue(ms_delete_bitmap_count == 7)
                assertTrue(ms_delete_bitmap_cardinality == 7)

                def status = getAggCacheDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id)
                logger.info("agg cache status: ${status}")
                assert status.delete_bitmap_count == 8
                assert status.cardinality == 7
                assert status.size > 0

                status = getAggCacheDeleteBitmapStatus(backendId_to_backendIP[trigger_backend_id], backendId_to_backendHttpPort[trigger_backend_id], tablet_id, true)
                logger.info("agg cache verbose status: ${status}")
            }

            def tablet_delete_bitmap_count = 0;
            def base_rowset_delete_bitmap_count = 0;
            int retry_time = 0;
            while (retry_time < 10) {
                log.info("retry_time: ${retry_time}")
                getMetricsMethod.call() {
                    respCode, body ->
                        logger.info("test get delete bitmap count resp Code {}", "${respCode}".toString())
                        assertEquals("${respCode}".toString(), "200")
                        String out = "${body}".toString()
                        def strs = out.split('\n')
                        for (String line in strs) {
                            if (line.startsWith("tablet_max_delete_bitmap_score")) {
                                logger.info("find: {}", line)
                                tablet_delete_bitmap_count = line.replaceAll("tablet_max_delete_bitmap_score ", "").toInteger()
                                break
                            }
                        }
                        for (String line in strs) {
                            if (line.startsWith("tablet_max_base_rowset_delete_bitmap_score")) {
                                logger.info("find: {}", line)
                                base_rowset_delete_bitmap_count = line.replaceAll("tablet_max_base_rowset_delete_bitmap_score ", "").toInteger()
                                break
                            }
                        }
                }
                if (tablet_delete_bitmap_count > 0 && base_rowset_delete_bitmap_count > 0) {
                    break;
                } else {
                    Thread.sleep(10000)
                    retry_time++;
                }
            }
            assertTrue(tablet_delete_bitmap_count > 0)
            assertTrue(base_rowset_delete_bitmap_count > 0)
        }
    } finally {
        reset_be_param("check_tablet_delete_bitmap_interval_seconds")
    }

}
