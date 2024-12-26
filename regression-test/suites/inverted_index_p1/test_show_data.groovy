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

suite("test_show_data", "p1") {
    // define a sql table
    def testTableWithoutIndex = "test_show_data_httplogs_without_index"
    def testTableWithIndex = "test_show_data_httplogs_with_index"
    def delta_time = 5000
    def timeout = 60000
    def alter_res = "null"
    def useTime = 0
    String database = context.config.getDbNameByFile(context.file)

    def create_httplogs_table_without_index = {testTablex ->
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
                            "replication_allocation" = "tag.location.default: 1",
                            "disable_auto_compaction" = "true"
                        );
                        """
    }
    def create_httplogs_table_with_index = {testTablex ->
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

    def wait_for_show_data_finish = { table_name, OpTimeout, origin_size ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            def result = sql """show data from ${database}.${table_name};"""
            if (result.size() > 0) {
                logger.info(table_name + " show data, detail: " + result[0].toString())
                def size = result[0][2].replace(" KB", "").toDouble()
                if (size != origin_size) {
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
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                return
            }
            useTime = t
            Thread.sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout, useTime=${useTime}")
    }

    def wait_for_last_build_index_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() == 0) {
                return "FINISHED"
            }
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
        logger.info("wait_for_last_build_index_on_table_finish debug: " + alter_res)
        assertTrue(useTime <= OpTimeout, "wait_for_last_build_index_on_table_finish timeout, useTime=${useTime}")
        return "wait_timeout"
    }

    try {
        sql "DROP TABLE IF EXISTS ${testTableWithoutIndex}"

        create_httplogs_table_without_index.call(testTableWithoutIndex)

        load_httplogs_data.call(testTableWithoutIndex, 'test_httplogs_load_without_index', 'true', 'json', 'documents-1000.json')

        sql "sync"
        def no_index_size = wait_for_show_data_finish(testTableWithoutIndex, 300000, 0)
        assertTrue(no_index_size != "wait_timeout")
        sql """ ALTER TABLE ${testTableWithoutIndex} ADD INDEX idx_request (`request`) USING INVERTED PROPERTIES("parser" = "english") """
        wait_for_latest_op_on_table_finish(testTableWithoutIndex, timeout)

        sql """ BUILD INDEX idx_request ON ${testTableWithoutIndex} """
        def state = wait_for_last_build_index_on_table_finish(testTableWithoutIndex, timeout)
        assertEquals(state, "FINISHED")
        def with_index_size = wait_for_show_data_finish(testTableWithoutIndex, 300000, no_index_size)
        assertTrue(with_index_size != "wait_timeout")

        sql """ ALTER TABLE ${testTableWithoutIndex} DROP INDEX idx_request """
        wait_for_latest_op_on_table_finish(testTableWithoutIndex, timeout)
        def another_no_index_size = wait_for_show_data_finish(testTableWithoutIndex, 300000, with_index_size)
        sql "DROP TABLE IF EXISTS ${testTableWithIndex}"
        create_httplogs_table_with_index.call(testTableWithIndex)
        load_httplogs_data.call(testTableWithIndex, 'test_httplogs_load_with_index', 'true', 'json', 'documents-1000.json')
        def another_with_index_size = wait_for_show_data_finish(testTableWithIndex, 300000, 0)
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}

suite("test_show_data_for_bkd", "p1") {
    // define a sql table
    def testTableWithoutBKDIndex = "test_show_data_httplogs_without_bkd_index"
    def testTableWithBKDIndex = "test_show_data_httplogs_with_bkd_index"
    def delta_time = 5000
    def timeout = 60000
    def alter_res = "null"
    def useTime = 0
    String database = context.config.getDbNameByFile(context.file)

    def create_httplogs_table_without_bkd_index = {testTablex ->
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
                            "replication_allocation" = "tag.location.default: 1",
                            "disable_auto_compaction" = "true"
                        );
                        """
    }
    def create_httplogs_table_with_bkd_index = {testTablex ->
        // multi-line sql
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${testTablex} (
                          `@timestamp` int(11) NULL,
                          `clientip` varchar(20) NULL,
                          `request` text NULL,
                          `status` int(11) NULL,
                          `size` int(11) NULL,
                          INDEX status_idx (`status`) USING INVERTED
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

    def wait_for_show_data_finish = { table_name, OpTimeout, origin_size ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            def result = sql """show data from ${database}.${table_name};"""
            if (result.size() > 0) {
                logger.info(table_name + " show data, detail: " + result[0].toString())
                def size = result[0][2].replace(" KB", "").toDouble()
                if (size != origin_size) {
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
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                return
            }
            useTime = t
            Thread.sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout, useTime=${useTime}")
    }

    def wait_for_last_build_index_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() == 0) {
                return "FINISHED"
            }
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
        sql "DROP TABLE IF EXISTS ${testTableWithoutBKDIndex}"

        create_httplogs_table_without_bkd_index.call(testTableWithoutBKDIndex)

        load_httplogs_data.call(testTableWithoutBKDIndex, 'test_httplogs_load_without_bkd_index', 'true', 'json', 'documents-1000.json')

        sql "sync"
        def no_index_size = wait_for_show_data_finish(testTableWithoutBKDIndex, 300000, 0)
        assertTrue(no_index_size != "wait_timeout")
        sql """ ALTER TABLE ${testTableWithoutBKDIndex} ADD INDEX idx_status (`status`) USING INVERTED; """
        wait_for_latest_op_on_table_finish(testTableWithoutBKDIndex, timeout)

        // BUILD INDEX and expect state is RUNNING
        sql """ BUILD INDEX idx_status ON ${testTableWithoutBKDIndex} """
        def state = wait_for_last_build_index_on_table_finish(testTableWithoutBKDIndex, timeout)
        assertEquals(state, "FINISHED")
        def with_index_size = wait_for_show_data_finish(testTableWithoutBKDIndex, 300000, no_index_size)
        assertTrue(with_index_size != "wait_timeout")

        sql """ ALTER TABLE ${testTableWithoutBKDIndex} DROP INDEX idx_status """
        wait_for_latest_op_on_table_finish(testTableWithoutBKDIndex, timeout)
        def another_no_index_size = wait_for_show_data_finish(testTableWithoutBKDIndex, 300000, with_index_size)

        sql "DROP TABLE IF EXISTS ${testTableWithBKDIndex}"
        create_httplogs_table_with_bkd_index.call(testTableWithBKDIndex)
        load_httplogs_data.call(testTableWithBKDIndex, 'test_httplogs_load_with_bkd_index', 'true', 'json', 'documents-1000.json')
        def another_with_index_size = wait_for_show_data_finish(testTableWithBKDIndex, 300000, 0)
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}

suite("test_show_data_multi_add", "p1") {
    // define a sql table
    def testTableWithoutIndex = "test_show_data_httplogs_multi_add_without_index"
    def testTableWithIndex = "test_show_data_httplogs_multi_add_with_index"
    def delta_time = 5000
    def timeout = 60000
    def alter_res = "null"
    def useTime = 0
    String database = context.config.getDbNameByFile(context.file)

    def create_httplogs_table_without_index = {testTablex ->
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
                            "replication_allocation" = "tag.location.default: 1",
                            "disable_auto_compaction" = "true"
                        );
                        """
    }
    def create_httplogs_table_with_index = {testTablex ->
        // multi-line sql
        def result = sql """
                        CREATE TABLE IF NOT EXISTS ${testTablex} (
                          `@timestamp` int(11) NULL,
                          `clientip` varchar(20) NULL,
                          `request` text NULL,
                          `status` int(11) NULL,
                          `size` int(11) NULL,
                          INDEX status_idx (`status`) USING INVERTED,
                          INDEX request_idx (`request`) USING INVERTED
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

    def wait_for_show_data_finish = { table_name, OpTimeout, origin_size ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            def result = sql """show data from ${database}.${table_name};"""
            if (result.size() > 0) {
                logger.info(table_name + " show data, detail: " + result[0].toString())
                def size = result[0][2].replace(" KB", "").toDouble()
                if (size != origin_size) {
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
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(3000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                return
            }
            useTime = t
            Thread.sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout, useTime=${useTime}")
    }

    def wait_for_last_build_index_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() == 0) {
                return "FINISHED"
            }
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
        sql "DROP TABLE IF EXISTS ${testTableWithoutIndex}"

        create_httplogs_table_without_index.call(testTableWithoutIndex)

        load_httplogs_data.call(testTableWithoutIndex, 'test_show_data_httplogs_multi_add_without_index', 'true', 'json', 'documents-1000.json')

        sql "sync"
        def no_index_size = wait_for_show_data_finish(testTableWithoutIndex, 300000, 0)
        assertTrue(no_index_size != "wait_timeout")
        sql """ ALTER TABLE ${testTableWithoutIndex} ADD INDEX idx_status (`status`) USING INVERTED; """
        wait_for_latest_op_on_table_finish(testTableWithoutIndex, timeout)

        // BUILD INDEX and expect state is RUNNING
        sql """ BUILD INDEX idx_status ON ${testTableWithoutIndex} """
        def state = wait_for_last_build_index_on_table_finish(testTableWithoutIndex, timeout)
        assertEquals(state, "FINISHED")
        def with_index_size1 = wait_for_show_data_finish(testTableWithoutIndex, 300000, no_index_size)
        assertTrue(with_index_size1 != "wait_timeout")

        sql """ ALTER TABLE ${testTableWithoutIndex} ADD INDEX request_idx (`request`) USING INVERTED; """
        wait_for_latest_op_on_table_finish(testTableWithoutIndex, timeout)

        // BUILD INDEX and expect state is RUNNING
        sql """ BUILD INDEX request_idx ON ${testTableWithoutIndex} """
        def state2 = wait_for_last_build_index_on_table_finish(testTableWithoutIndex, timeout)
        assertEquals(state2, "FINISHED")
        def with_index_size2 = wait_for_show_data_finish(testTableWithoutIndex, 300000, with_index_size1)
        assertTrue(with_index_size2 != "wait_timeout")

        sql "DROP TABLE IF EXISTS ${testTableWithIndex}"
        create_httplogs_table_with_index.call(testTableWithIndex)
        load_httplogs_data.call(testTableWithIndex, 'test_show_data_httplogs_multi_add_with_index', 'true', 'json', 'documents-1000.json')
        def another_with_index_size = wait_for_show_data_finish(testTableWithIndex, 300000, 0)
    } finally {
        //try_sql("DROP TABLE IF EXISTS ${testTable}")
    }
}

suite("test_show_data_with_compaction", "p1") {
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

    def backend_id = backendId_to_backendIP.keySet()[0]
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
        for (backendId: backendId_to_backendIP.keySet()) {
            def (code1, out1, err1) = update_be_config(backendId_to_backendIP.get(backendId), backendId_to_backendHttpPort.get(backendId), key, value)
            logger.info("update config: code=" + code1 + ", out=" + out1 + ", err=" + err1)
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

    def create_table_run_compaction_and_wait = { test_name ->
        sql """ DROP TABLE IF EXISTS ${test_name}; """
        sql """
            CREATE TABLE ${test_name} (
                `id` int(11) NULL,
                `name` varchar(255) NULL,
                `hobbies` text NULL,
                `score` int(11) NULL,
                index index_name (name) using inverted,
                index index_hobbies (hobbies) using inverted properties("parser"="english"),
                index index_score (score) using inverted
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES ( "replication_num" = "1", "disable_auto_compaction" = "true");
        """

        sql """ INSERT INTO ${test_name} VALUES (1, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${test_name} VALUES (1, "bason", "bason hate pear", 99); """
        sql """ INSERT INTO ${test_name} VALUES (2, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${test_name} VALUES (2, "bason", "bason hate pear", 99); """
        sql """ INSERT INTO ${test_name} VALUES (3, "andy", "andy love apple", 100); """
        sql """ INSERT INTO ${test_name} VALUES (3, "bason", "bason hate pear", 99); """
        def data_size = wait_for_show_data_finish(test_name, 60000, 0)
        assertTrue(data_size != "wait_timeout")
        trigger_and_wait_compaction(test_name, "full")
        data_size = wait_for_show_data_finish(test_name, 60000, data_size)
        assertTrue(data_size != "wait_timeout")
        return data_size
    }

    try {

        set_be_config.call("inverted_index_compaction_enable", "true")
        sql "DROP TABLE IF EXISTS ${tableWithIndexCompaction}"
        create_table_with_index.call(tableWithIndexCompaction)

        load_httplogs_data.call(tableWithIndexCompaction, '1', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithIndexCompaction, '2', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithIndexCompaction, '3', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithIndexCompaction, '4', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithIndexCompaction, '5', 'true', 'json', 'documents-1000.json')

        sql "sync"

        def with_index_size = wait_for_show_data_finish(tableWithIndexCompaction, 60000, 0)
        assertTrue(with_index_size != "wait_timeout")
        trigger_and_wait_compaction(tableWithIndexCompaction, "full")
        with_index_size = wait_for_show_data_finish(tableWithIndexCompaction, 60000, with_index_size)
        assertTrue(with_index_size != "wait_timeout")

        set_be_config.call("inverted_index_compaction_enable", "false")

        sql "DROP TABLE IF EXISTS ${tableWithOutIndexCompaction}"
        create_table_with_index.call(tableWithOutIndexCompaction)
        load_httplogs_data.call(tableWithOutIndexCompaction, '6', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithOutIndexCompaction, '7', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithOutIndexCompaction, '8', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithOutIndexCompaction, '9', 'true', 'json', 'documents-1000.json')
        load_httplogs_data.call(tableWithOutIndexCompaction, '10', 'true', 'json', 'documents-1000.json')

        def another_with_index_size = wait_for_show_data_finish(tableWithOutIndexCompaction, 60000, 0)
        assertTrue(another_with_index_size != "wait_timeout")
        trigger_and_wait_compaction(tableWithOutIndexCompaction, "full")
        another_with_index_size = wait_for_show_data_finish(tableWithOutIndexCompaction, 60000, another_with_index_size)
        assertTrue(another_with_index_size != "wait_timeout")

        logger.info("with_index_size is {}, another_with_index_size is {}", with_index_size, another_with_index_size)
        assertEquals(another_with_index_size, with_index_size)

        set_be_config.call("inverted_index_compaction_enable", "true")

        def tableName = "test_inverted_index_compaction"
        def data_size_1 = create_table_run_compaction_and_wait(tableName)

        set_be_config.call("inverted_index_compaction_enable", "false")
        def data_size_2 = create_table_run_compaction_and_wait(tableName)

        logger.info("data_size_1 is {}, data_size_2 is {}", data_size_1, data_size_2)
        assertEquals(data_size_1, data_size_2)

    } finally {
        // sql "DROP TABLE IF EXISTS ${tableWithIndexCompaction}"
        // sql "DROP TABLE IF EXISTS ${tableWithOutIndexCompaction}"
        set_be_config.call("inverted_index_compaction_enable", invertedIndexCompactionEnable.toString())
    }
}
