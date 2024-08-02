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

suite("test_show_data", "p0") {
    // define a sql table
    def testTable = "test_show_data_httplogs"
    def delta_time = 5000
    def timeout = 60000
    String database = context.config.getDbNameByFile(context.file)

    def create_httplogs_dup_table = {testTablex ->
        // multi-line sql
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${testTablex} (
                          `@timestamp` int(11) NULL,
                          `clientip` varchar(20) NULL,
                          `request` text NULL,
                          `status` int(11) NULL,
                          `size` int(11) NULL,
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`@timestamp`)
                        DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
                        PROPERTIES (
                            "replication_allocation" = "tag.location.default: 1"
                        );
                        """
    }

    def load_httplogs_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1 ->

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

    def wait_for_show_data_finish = { table_name, OpTimeout, origin_size ->
        def useTime = 0
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            result = sql """show data from ${database}.${table_name};"""
            if (result.size() > 0) {
                logger.info(table_name + " show data, detail: " + result[0].toString())
                def size = result[0][2].replace(" KB", "").toDouble()
                if (size > origin_size) {
                    return size
                }
            }
            useTime = t
            Thread.sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_show_data_finish timeout, useTime=${useTime}")
        return "wait_timeout"
    }

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        def useTime = 0
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            Thread.sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout, useTime=${useTime}")
    }

    def wait_for_last_build_index_on_table_finish = { table_name, OpTimeout ->
        def useTime = 0
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() > 0) {
                def last_job_state = alter_res[alter_res.size()-1][7];
                if (last_job_state == "FINISHED" || last_job_state == "CANCELLED") {
                    logger.info(table_name + " last index job finished, state: " + last_job_state + ", detail: " + alter_res)
                    return last_job_state;
                }
            }
            useTime = t
            Thread.sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_last_build_index_on_table_finish timeout, useTime=${useTime}")
        return "wait_timeout"
    }

    try {
        sql "DROP TABLE IF EXISTS ${testTable}"

        create_httplogs_dup_table.call(testTable)

        load_httplogs_data.call(testTable, 'test_httplogs_load', 'true', 'json', 'documents-1000.json')

        sql "sync"
        def no_index_size = wait_for_show_data_finish(testTable, 300000, 0)
        assertTrue(no_index_size != "wait_timeout")
        sql """ ALTER TABLE ${testTable} ADD INDEX idx_request (`request`) USING INVERTED PROPERTIES("parser" = "english") """
        wait_for_latest_op_on_table_finish(testTable, timeout)

        // BUILD INDEX and expect state is RUNNING
        sql """ BUILD INDEX idx_request ON ${testTable} """
        def state = wait_for_last_build_index_on_table_finish(testTable, timeout)
        assertEquals(state, "FINISHED")
        def with_index_size = wait_for_show_data_finish(testTable, 300000, no_index_size)
        assertTrue(with_index_size != "wait_timeout")
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}

suite("test_show_data_with_compaction", "p0") {
    // define a sql table
    def tableWithIndexCompaction = "test_with_index_compaction"
    def tableWithOutIndexCompaction = "test_without_index_compaction"
    def delta_time = 5000
    def timeout = 60000
    def alter_res = "null"
    def useTime = 0
    String database = context.config.getDbNameByFile(context.file)
    boolean invertedIndexCompactionEnable = true

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    backend_id = backendId_to_backendIP.keySet()[0]
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
    }

    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def create_table_with_index = {testTablex ->
        // multi-line sql
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${testTablex} (
                          `@timestamp` int(11) NULL,
                          `clientip` varchar(20) NULL,
                          `request` text NULL,
                          `status` int(11) NULL,
                          `size` int(11) NULL,
                          INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser"="english") COMMENT ''
                        ) ENGINE=OLAP
                        DUPLICATE KEY(`@timestamp`)
                        DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
                        PROPERTIES (
                            "replication_allocation" = "tag.location.default: 1",
                            "disable_auto_compaction" = "true"
                        );
                        """
    }

    def load_httplogs_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1 ->

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

    def wait_for_show_data_finish = { table_name, OpTimeout, origin_size, maxRetries = 5 ->
        def size = origin_size
        def retries = 0
        def last_size = origin_size

        while (retries < maxRetries) {
            for (int t = 0; t < OpTimeout; t += delta_time) {
                def result = sql """show data from ${database}.${table_name};"""
                if (result.size() > 0) {
                    logger.info(table_name + " show data, detail: " + result[0].toString())
                    size = result[0][2].replace(" KB", "").toDouble()
                }
                useTime += delta_time
                Thread.sleep(delta_time)

                // If size changes, break the for loop to check in the next while iteration
                if (size != origin_size && size != last_size) {
                    break
                }
            }

            if (size != last_size) {
                last_size = size
            } else {
                // If size didn't change during the last OpTimeout period, return size
                if (size != origin_size) {
                    return size
                }
            }

            retries++
        }
        return "wait_timeout"
    }


    try {
        
        def run_compaction_and_wait = { tableName ->
            //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,QueryHits,PathHash,MetaUrl,CompactionStatus
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """

            // trigger compactions for all tablets in ${tableName}
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                backend_id = tablet.BackendId
                (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactJson = parseJson(out.trim())
                if (compactJson.status.toLowerCase() == "fail") {
                    logger.info("Compaction was done automatically!")
                } else {
                    assertEquals("success", compactJson.status.toLowerCase())
                }
            }

            // wait for all compactions done
            for (def tablet in tablets) {
                boolean running = true
                do {
                    Thread.sleep(1000)
                    String tablet_id = tablet.TabletId
                    backend_id = tablet.BackendId
                    (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                    logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                    assertEquals(code, 0)
                    def compactionStatus = parseJson(out.trim())
                    assertEquals("success", compactionStatus.status.toLowerCase())
                    running = compactionStatus.run_status
                } while (running)
            }
        }

        set_be_config.call("inverted_index_compaction_enable", "false")
        sql "DROP TABLE IF EXISTS ${tableWithIndexCompaction}"
        create_table_with_index.call(tableWithIndexCompaction)

        load_httplogs_data.call(tableWithIndexCompaction, '1', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithIndexCompaction, '2', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithIndexCompaction, '3', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithIndexCompaction, '4', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithIndexCompaction, '5', 'true', 'json', 'documents-1000.json')

        sql "sync"

        run_compaction_and_wait(tableWithIndexCompaction)
        def with_index_size = wait_for_show_data_finish(tableWithIndexCompaction, 60000, 0)
        assertTrue(with_index_size != "wait_timeout")

    } finally {
        // sql "DROP TABLE IF EXISTS ${tableWithIndexCompaction}"
        set_be_config.call("inverted_index_compaction_enable", invertedIndexCompactionEnable.toString())
    }
}
