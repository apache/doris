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