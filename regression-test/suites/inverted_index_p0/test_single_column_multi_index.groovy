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

suite("test_single_column_multi_index", "nonConcurrent") {
    def isCloudMode = isCloudMode()
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    
    boolean disableAutoCompaction = false

    def tableName = "test_single_column_multi_index"

    // Function to create the test table
    def createTestTable = { ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
          CREATE TABLE ${tableName} (
            `@timestamp` int(11) NULL COMMENT "",
            `clientip` varchar(20) NULL COMMENT "",
            `request` text NULL COMMENT "",
            `status` int(11) NULL COMMENT "",
            `size` int(11) NULL COMMENT "",
            INDEX request_keyword_idx (`request`) USING INVERTED COMMENT '',
            INDEX request_text_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT ''
          ) ENGINE=OLAP
          DUPLICATE KEY(`@timestamp`)
          COMMENT "OLAP"
          DISTRIBUTED BY RANDOM BUCKETS 1
          PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
          )
        """
    }

    def load_httplogs_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1, load_to_single_tablet = 'true' ->
        
        // load the json data
        streamLoad {
            table "${table_name}"
            
            // set http request header params
            set 'label', label + "_" + UUID.randomUUID().toString()
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            file file_name // import json file
            time 10000 // limit inflight 10s
            if (expected_succ_rows >= 0) {
                set 'max_filter_ratio', '1'
            }

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
		        if (ignore_failure && expected_succ_rows < 0) { return }
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    if (expected_succ_rows >= 0) {
                        assertEquals(json.NumberLoadedRows, expected_succ_rows)
                    } else {
                        assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                        assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }

    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def check_config = { String key, String value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
            logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def configList = parseJson(out.trim())
            assert configList instanceof List
            for (Object ele in (List) configList) {
                assert ele instanceof List<String>
                if (((List<String>) ele)[0] == key) {
                    assertEquals(value, ((List<String>) ele)[2])
                }
            }
        }
    }

    def get_rowset_count = { tablets ->
        int rowsetCount = 0
        for (def tablet in tablets) {
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        return rowsetCount
    }

    def trigger_full_compaction_on_tablets = { tablets ->
        for (def tablet : tablets) {
            String tablet_id = tablet.TabletId
            String backend_id = tablet.BackendId
            int times = 1

            String compactionStatus;
            do{
                def (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                ++times
                sleep(2000)
                compactionStatus = parseJson(out.trim()).status.toLowerCase();
            } while (compactionStatus!="success" && times<=10 && compactionStatus!="e-6010")


            if (compactionStatus == "fail") {
                assertEquals(disableAutoCompaction, false)
                logger.info("Compaction was done automatically!")
            }
            if (disableAutoCompaction && compactionStatus!="e-6010") {
                assertEquals("success", compactionStatus)
            }
        }
    }

    def wait_full_compaction_done = { tablets ->
        for (def tablet in tablets) {
            boolean running = true
            do {
                Thread.sleep(1000)
                String tablet_id = tablet.TabletId
                String backend_id = tablet.BackendId
                def (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("success", compactionStatus.status.toLowerCase())
                running = compactionStatus.run_status
            } while (running)
        }
    }

    // Function to load test data
    def loadTestData = { times = 10 ->
        for (int i = 0; i < times; i++) {
            load_httplogs_data.call(tableName, 'test_single_column_multi_index', 'true', 'json', 'documents-1000.json')
        }
        sql "sync"
    }

    // Function to run match queries with debug points
    def runMatchQueries = { ->
        sql """ set enable_common_expr_pushdown = true; """
        sql """ set enable_common_expr_pushdown_for_inverted_index = true; """
        GetDebugPoint().enableDebugPointForAllBEs("VMatchPredicate.execute")

        try {
            GetDebugPoint().enableDebugPointForAllBEs("inverted_index_reader._select_best_reader", [type: 0])
            try {
                qt_sql """ select count() from ${tableName} where (request match 'images'); """
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("inverted_index_reader._select_best_reader")
            }

            GetDebugPoint().enableDebugPointForAllBEs("inverted_index_reader._select_best_reader", [type: 1])
            try {
                qt_sql """ select count() from ${tableName} where (request = 'GET /images/hm_bg.jpg HTTP/1.0'); """
            } finally {
                GetDebugPoint().disableDebugPointForAllBEs("inverted_index_reader._select_best_reader")
            }
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("VMatchPredicate.execute")
        }
    }

    // Function to check and update BE config
    def checkAndUpdateBeConfig = { ->
        def invertedIndexCompactionEnable = false
        def has_update_be_config = false
        
        String backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        
        logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List

        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == "inverted_index_compaction_enable") {
                invertedIndexCompactionEnable = Boolean.parseBoolean(((List<String>) ele)[2])
                logger.info("inverted_index_compaction_enable: ${((List<String>) ele)[2]}")
            }
            if (((List<String>) ele)[0] == "disable_auto_compaction") {
                disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
                logger.info("disable_auto_compaction: ${((List<String>) ele)[2]}")
            }
        }
        
        return invertedIndexCompactionEnable
    }

    // Main test execution
    try {
        sql """ set global enable_match_without_inverted_index = false """

        createTestTable()
        loadTestData()
        runMatchQueries()

        def invertedIndexCompactionEnable = checkAndUpdateBeConfig()
        
        try {
            set_be_config.call("inverted_index_compaction_enable", "true")
            check_config.call("inverted_index_compaction_enable", "true")

            def tablets = sql_return_maparray """ show tablets from ${tableName}; """
            int replicaNum = 1
            def dedup_tablets = deduplicate_tablets(tablets)
            if (dedup_tablets.size() > 0) {
                replicaNum = Math.round(tablets.size() / dedup_tablets.size())
                if (replicaNum != 1 && replicaNum != 3) {
                    assert(false)
                }
            }
            
            // Verify rowset count before compaction
            int rowsetCount = get_rowset_count.call(tablets)
            assert (rowsetCount == 11 * replicaNum)

            // Run compaction
            trigger_full_compaction_on_tablets.call(tablets)
            wait_full_compaction_done.call(tablets)

            // Verify rowset count after compaction
            rowsetCount = get_rowset_count.call(tablets)
            if (isCloudMode) {
                assert (rowsetCount == (1 + 1) * replicaNum)
            } else {
                assert (rowsetCount == 1 * replicaNum)
            }

            runMatchQueries()
        } finally {
            set_be_config.call("inverted_index_compaction_enable", invertedIndexCompactionEnable.toString())
        }
    } finally {
        sql """ set global enable_match_without_inverted_index = true """
    }
}