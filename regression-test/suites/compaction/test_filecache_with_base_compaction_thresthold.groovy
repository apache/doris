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

 
suite("test_filecache_with_base_compaction_thresthold", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(1)

    options.beConfigs.add('enable_flush_file_cache_async=false')
    options.beConfigs.add('file_cache_enter_disk_resource_limit_mode_percent=99')
    options.beConfigs.add('enable_evict_file_cache_in_advance=false')
    // 80MB file cache size
    options.beConfigs.add('file_cache_path=[{"path":"/opt/apache-doris/be/storage/file_cache","total_size":83886080,"query_limit":83886080}]')

    def testTable = "test_filecache_with_base_compaction_thresthold"
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
            Thread.sleep(20000)
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

        sql """ DROP TABLE IF EXISTS ${testTable} force;"""

        // ================= case1: input_rowsets_hit_cache_ratio=0.703611, _input_rowsets_cached_size=79656542, _input_rowsets_total_size=113210974 =================
        // real_ratio > file_cache_keep_base_compaction_output_min_hit_ratio = 0.7, it will write file cache
        sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                ss_sold_date_sk bigint,
                ss_sold_time_sk bigint,
                ss_item_sk bigint,
                ss_customer_sk bigint,
                ss_cdemo_sk bigint,
                ss_hdemo_sk bigint,
                ss_addr_sk bigint,
                ss_store_sk bigint,
                ss_promo_sk bigint,
                ss_ticket_number bigint,
                ss_quantity integer,
                ss_wholesale_cost decimal(7,2),
                ss_list_price decimal(7,2),
                ss_sales_price decimal(7,2),
                ss_ext_discount_amt decimal(7,2),
                ss_ext_sales_price decimal(7,2),
                ss_ext_wholesale_cost decimal(7,2),
                ss_ext_list_price decimal(7,2),
                ss_ext_tax decimal(7,2),
                ss_coupon_amt decimal(7,2),
                ss_net_paid decimal(7,2),
                ss_net_paid_inc_tax decimal(7,2),
                ss_net_profit decimal(7,2)
            )
            DUPLICATE KEY(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk)
            DISTRIBUTED BY HASH(ss_customer_sk) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )

        """

        getBackendIpHttpAndBrpcPort(backendId_to_backendIP, backendId_to_backendHttpPort, backendId_to_backendBrpcPort);

        def tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId

        streamLoad {
            table testTable

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'

            file """${getS3Url()}/regression/tpcds/sf1/store_sales.dat.gz"""

            time 10000 // limit inflight 10s
            check { res, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${res}".toString())
                def json = parseJson(res)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }

        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """

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
        for (int i = 0; i < 60; i++) {
            tablet_status = getTabletStatus(tablet)
            if (tablet_status["rowsets"].size() == 2) {
                base_compaction_finished = true
                final_rowsets.addAll(tablet_status["rowsets"])
                break
            }
            sleep(10000)
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
            def segments = data.stream()
                .filter(item -> !item.endsWith("_idx"))
                .count();
            logger.info("segments: ${segments}")
            assertTrue(segments > 0)
        }

        def (code_0, out_0, err_0) = curl("GET", "http://${be_host}:${backendId_to_backendBrpcPort[tablet.BackendId]}/vars/base_compaction_input_size")
        logger.info("base_compaction_input_size: ${out_0}")
        def size = out_0.trim().split(":")[1].trim().toInteger()
        assertTrue(size > 100 * 1024 * 1024)

        (code_0, out_0, err_0) = curl("GET", "http://${be_host}:${backendId_to_backendBrpcPort[tablet.BackendId]}/vars/base_compaction_input_cached_size")
        logger.info("base_compaction_input_cached_size: ${out_0}")
        size = out_0.trim().split(":")[1].trim().toInteger()
        assertTrue(size > 70 * 1024 * 1024)

        // =================case2: input_rowsets_hit_cache_ratio=0.703524, total_cached_size=62914560 (60MB), total_size=113177376 =================
        //  real_ratio < file_cache_keep_base_compaction_output_min_hit_ratio = 0.8, so not write file cache
        result = Http.GET("http://${be_host}:${be_http_port}/api/file_cache?op=clear&sync=true", true)
        logger.info("clear file cache data: ${result}")

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
            assertTrue(data.size() == 0)
        }

        def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", be_host, be_http_port, "file_cache_keep_base_compaction_output_min_hit_ratio", 0.8))
        assertTrue(out.contains("OK"))
        sql """ DROP TABLE IF EXISTS ${testTable} force;"""

        sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                ss_sold_date_sk bigint,
                ss_sold_time_sk bigint,
                ss_item_sk bigint,
                ss_customer_sk bigint,
                ss_cdemo_sk bigint,
                ss_hdemo_sk bigint,
                ss_addr_sk bigint,
                ss_store_sk bigint,
                ss_promo_sk bigint,
                ss_ticket_number bigint,
                ss_quantity integer,
                ss_wholesale_cost decimal(7,2),
                ss_list_price decimal(7,2),
                ss_sales_price decimal(7,2),
                ss_ext_discount_amt decimal(7,2),
                ss_ext_sales_price decimal(7,2),
                ss_ext_wholesale_cost decimal(7,2),
                ss_ext_list_price decimal(7,2),
                ss_ext_tax decimal(7,2),
                ss_coupon_amt decimal(7,2),
                ss_net_paid decimal(7,2),
                ss_net_paid_inc_tax decimal(7,2),
                ss_net_profit decimal(7,2)
            )
            DUPLICATE KEY(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk)
            DISTRIBUTED BY HASH(ss_customer_sk) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        tablet = tablets[0]
        tablet_id = tablet.TabletId

        streamLoad {
            table testTable

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'

            file """${getS3Url()}/regression/tpcds/sf1/store_sales.dat.gz"""

            time 10000 // limit inflight 10s
            check { res, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${res}".toString())
                def json = parseJson(res)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }

        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """

        sql """ sync """
        sql "select * from ${testTable}"
        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")

        triggerCumulativeCompaction(tablet)
        waitForCompaction(tablet)
        triggerCumulativeCompaction(tablet)
        waitForCompaction(tablet)
        triggerBaseCompaction(tablet)
        waitForCompaction(tablet)

        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")
        base_compaction_finished = false
        final_rowsets = new HashSet<>();
        for (int i = 0; i < 60; i++) {
            tablet_status = getTabletStatus(tablet)
            if (tablet_status["rowsets"].size() == 2) {
                base_compaction_finished = true
                final_rowsets.addAll(tablet_status["rowsets"])
                break
            }
            sleep(10000)
        }
        assertTrue(base_compaction_finished)

        be_host = backendId_to_backendIP[tablet.BackendId]
        be_http_port = backendId_to_backendHttpPort[tablet.BackendId]

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

            def segments = data.stream()
                .filter(item -> !item.endsWith("_idx"))
                .count();
            logger.info("segments: ${segments}")
            assertTrue(segments == 0)
        }

        (code_0, out_0, err_0) = curl("GET", "http://${be_host}:${backendId_to_backendBrpcPort[tablet.BackendId]}/vars/base_compaction_input_size")
        logger.info("base_compaction_input_size: ${out_0}")
        size = out_0.trim().split(":")[1].trim().toInteger()
        assertTrue(size > 2 * 100 * 1024 * 1024)

        (code_0, out_0, err_0) = curl("GET", "http://${be_host}:${backendId_to_backendBrpcPort[tablet.BackendId]}/vars/base_compaction_input_cached_size")
        logger.info("base_compaction_input_cached_size: ${out_0}")
        size = out_0.trim().split(":")[1].trim().toInteger()
        assertTrue(size > 70 * 1024 * 1024)

        // =================case3: file_cache_keep_base_compaction_output_min_hit_ratio = 0 =================
        //  real_ratio > file_cache_keep_base_compaction_output_min_hit_ratio = 0, it will write file cache
        result = Http.GET("http://${be_host}:${be_http_port}/api/file_cache?op=clear&sync=true", true)
        logger.info("clear file cache data: ${result}")

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
            assertTrue(data.size() == 0)
        }

        (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", be_host, be_http_port, "file_cache_keep_base_compaction_output_min_hit_ratio", 0))
        assertTrue(out.contains("OK"))
        sql """ DROP TABLE IF EXISTS ${testTable} force;"""

        sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                ss_sold_date_sk bigint,
                ss_sold_time_sk bigint,
                ss_item_sk bigint,
                ss_customer_sk bigint,
                ss_cdemo_sk bigint,
                ss_hdemo_sk bigint,
                ss_addr_sk bigint,
                ss_store_sk bigint,
                ss_promo_sk bigint,
                ss_ticket_number bigint,
                ss_quantity integer,
                ss_wholesale_cost decimal(7,2),
                ss_list_price decimal(7,2),
                ss_sales_price decimal(7,2),
                ss_ext_discount_amt decimal(7,2),
                ss_ext_sales_price decimal(7,2),
                ss_ext_wholesale_cost decimal(7,2),
                ss_ext_list_price decimal(7,2),
                ss_ext_tax decimal(7,2),
                ss_coupon_amt decimal(7,2),
                ss_net_paid decimal(7,2),
                ss_net_paid_inc_tax decimal(7,2),
                ss_net_profit decimal(7,2)
            )
            DUPLICATE KEY(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk)
            DISTRIBUTED BY HASH(ss_customer_sk) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        tablet = tablets[0]
        tablet_id = tablet.TabletId

        streamLoad {
            table testTable

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'

            file """${getS3Url()}/regression/tpcds/sf1/store_sales.dat.gz"""

            time 10000 // limit inflight 10s
            check { res, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${res}".toString())
                def json = parseJson(res)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }

        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """

        sql """ sync """
        sql "select * from ${testTable}"
        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")

        triggerCumulativeCompaction(tablet)
        waitForCompaction(tablet)
        triggerCumulativeCompaction(tablet)
        waitForCompaction(tablet)
        triggerBaseCompaction(tablet)
        waitForCompaction(tablet)

        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")
        base_compaction_finished = false
        final_rowsets = new HashSet<>();
        for (int i = 0; i < 60; i++) {
            tablet_status = getTabletStatus(tablet)
            if (tablet_status["rowsets"].size() == 2) {
                base_compaction_finished = true
                final_rowsets.addAll(tablet_status["rowsets"])
                break
            }
            sleep(10000)
        }
        assertTrue(base_compaction_finished)

        be_host = backendId_to_backendIP[tablet.BackendId]
        be_http_port = backendId_to_backendHttpPort[tablet.BackendId]

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

            def segments = data.stream()
                .filter(item -> !item.endsWith("_idx"))
                .count();
            logger.info("segments: ${segments}")
            assertTrue(segments > 0)
            assertTrue(data.size() > 0)
        }

        (code_0, out_0, err_0) = curl("GET", "http://${be_host}:${backendId_to_backendBrpcPort[tablet.BackendId]}/vars/base_compaction_input_size")
        logger.info("base_compaction_input_size: ${out_0}")
        size = out_0.trim().split(":")[1].trim().toInteger()
        assertTrue(size > 3 * 100 * 1024 * 1024)

        (code_0, out_0, err_0) = curl("GET", "http://${be_host}:${backendId_to_backendBrpcPort[tablet.BackendId]}/vars/base_compaction_input_cached_size")
        logger.info("base_compaction_input_cached_size: ${out_0}")
        size = out_0.trim().split(":")[1].trim().toInteger()
        assertTrue(size > 70 * 1024 * 1024)

        // =================case4: file_cache_keep_base_compaction_output_min_hit_ratio = 1 =================
        //  real_ratio < file_cache_keep_base_compaction_output_min_hit_ratio = 1, it will write not file cache
        result = Http.GET("http://${be_host}:${be_http_port}/api/file_cache?op=clear&sync=true", true)
        logger.info("clear file cache data: ${result}")

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
            assertTrue(data.size() == 0)
        }

        (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", be_host, be_http_port, "file_cache_keep_base_compaction_output_min_hit_ratio", 1))
        assertTrue(out.contains("OK"))
        sql """ DROP TABLE IF EXISTS ${testTable} force;"""

        sql """
            CREATE TABLE IF NOT EXISTS ${testTable} (
                ss_sold_date_sk bigint,
                ss_sold_time_sk bigint,
                ss_item_sk bigint,
                ss_customer_sk bigint,
                ss_cdemo_sk bigint,
                ss_hdemo_sk bigint,
                ss_addr_sk bigint,
                ss_store_sk bigint,
                ss_promo_sk bigint,
                ss_ticket_number bigint,
                ss_quantity integer,
                ss_wholesale_cost decimal(7,2),
                ss_list_price decimal(7,2),
                ss_sales_price decimal(7,2),
                ss_ext_discount_amt decimal(7,2),
                ss_ext_sales_price decimal(7,2),
                ss_ext_wholesale_cost decimal(7,2),
                ss_ext_list_price decimal(7,2),
                ss_ext_tax decimal(7,2),
                ss_coupon_amt decimal(7,2),
                ss_net_paid decimal(7,2),
                ss_net_paid_inc_tax decimal(7,2),
                ss_net_profit decimal(7,2)
            )
            DUPLICATE KEY(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk)
            DISTRIBUTED BY HASH(ss_customer_sk) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true"
            )
        """

        tablets = sql_return_maparray """ show tablets from ${testTable}; """
        logger.info("tablets: " + tablets)
        assertEquals(1, tablets.size())
        tablet = tablets[0]
        tablet_id = tablet.TabletId

        streamLoad {
            table testTable

            // default label is UUID:
            // set 'label' UUID.randomUUID().toString()

            // default column_separator is specify in doris fe config, usually is '\t'.
            // this line change to ','
            set 'column_separator', '|'
            set 'compress_type', 'GZ'

            file """${getS3Url()}/regression/tpcds/sf1/store_sales.dat.gz"""

            time 10000 // limit inflight 10s
            check { res, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${res}".toString())
                def json = parseJson(res)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(json.NumberTotalRows, json.NumberLoadedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }

        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """
        sql """ delete from ${testTable} where ss_item_sk=2; """

        sql """ sync """

        sql "select * from ${testTable}"
        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")

        triggerCumulativeCompaction(tablet)
        waitForCompaction(tablet)
        triggerCumulativeCompaction(tablet)
        waitForCompaction(tablet)

        sql "select * from ${testTable}"
        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")

        triggerBaseCompaction(tablet)
        waitForCompaction(tablet)

        tablet_status = getTabletStatus(tablet)
        logger.info("tablet status: ${tablet_status}")
        base_compaction_finished = false
        final_rowsets = new HashSet<>();
        for (int i = 0; i < 60; i++) {
            tablet_status = getTabletStatus(tablet)
            if (tablet_status["rowsets"].size() == 2) {
                base_compaction_finished = true
                final_rowsets.addAll(tablet_status["rowsets"])
                break
            }
            sleep(10000)
        }
        assertTrue(base_compaction_finished)

        be_host = backendId_to_backendIP[tablet.BackendId]
        be_http_port = backendId_to_backendHttpPort[tablet.BackendId]

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

            def segments = data.stream()
                .filter(item -> !item.endsWith("_idx"))
                .count();
            logger.info("segments: ${segments}")
            assertTrue(segments == 0)
        }

        (code_0, out_0, err_0) = curl("GET", "http://${be_host}:${backendId_to_backendBrpcPort[tablet.BackendId]}/vars/base_compaction_input_size")
        logger.info("base_compaction_input_size: ${out_0}")
        size = out_0.trim().split(":")[1].trim().toInteger()
        assertTrue(size > 4 * 100 * 1024 * 1024)

        (code_0, out_0, err_0) = curl("GET", "http://${be_host}:${backendId_to_backendBrpcPort[tablet.BackendId]}/vars/base_compaction_input_cached_size")
        logger.info("base_compaction_input_cached_size: ${out_0}")
        size = out_0.trim().split(":")[1].trim().toInteger()
        assertTrue(size > 70 * 1024 * 1024)
    }
}
