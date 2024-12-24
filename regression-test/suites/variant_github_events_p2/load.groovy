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

suite("regression_test_variant_github_events_p2", "nonConcurrent,p2"){
    // prepare test table
    def timeout = 300000
    def delta_time = 1000
    def alter_res = "null"
    def useTime = 0

    def wait_for_latest_op_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW ALTER TABLE COLUMN WHERE TableName = "${table_name}" ORDER BY CreateTime DESC LIMIT 1;"""
            alter_res = alter_res.toString()
            if(alter_res.contains("FINISHED")) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " latest alter job finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_op_on_table_finish timeout")
    }

    def wait_for_build_index_on_partition_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}";"""
            def expected_finished_num = alter_res.size();
            def finished_num = 0;
            for (int i = 0; i < expected_finished_num; i++) {
                logger.info(table_name + " build index job state: " + alter_res[i][7] + i)
                if (alter_res[i][7] == "FINISHED") {
                    ++finished_num;
                }
            }
            if (finished_num == expected_finished_num) {
                sleep(10000) // wait change table state to normal
                logger.info(table_name + " all build index jobs finished, detail: " + alter_res)
                break
            }
            useTime = t
            sleep(delta_time)
        }
        assertTrue(useTime <= OpTimeout, "wait_for_latest_build_index_on_partition_finish timeout")
    }

    def wait_for_last_build_index_on_table_finish = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() == 0) {
                logger.info(table_name + " last index job finished")
                return "SKIPPED"
            }
            if (alter_res.size() > 0) {
                def last_job_state = alter_res[alter_res.size()-1][7];
                if (last_job_state == "FINISHED" || last_job_state == "CANCELLED") {
                    sleep(10000) // wait change table state to normal
                    logger.info(table_name + " last index job finished, state: " + last_job_state + ", detail: " + alter_res)
                    return last_job_state;
                }
            }
            useTime = t
            sleep(delta_time)
        }
        logger.info("wait_for_last_build_index_on_table_finish debug: " + alter_res)
        assertTrue(useTime <= OpTimeout, "wait_for_last_build_index_on_table_finish timeout")
        return "wait_timeout"
    }

    def wait_for_last_build_index_on_table_running = { table_name, OpTimeout ->
        for(int t = delta_time; t <= OpTimeout; t += delta_time){
            alter_res = sql """SHOW BUILD INDEX WHERE TableName = "${table_name}" ORDER BY JobId """

            if (alter_res.size() == 0) {
                logger.info(table_name + " last index job finished")
                return "SKIPPED"
            }
            if (alter_res.size() > 0) {
                def last_job_state = alter_res[alter_res.size()-1][7];
                if (last_job_state == "RUNNING") {
                    logger.info(table_name + " last index job running, state: " + last_job_state + ", detail: " + alter_res)
                    return last_job_state;
                }
            }
            useTime = t
            sleep(delta_time)
        }
        logger.info("wait_for_last_build_index_on_table_running debug: " + alter_res)
        assertTrue(useTime <= OpTimeout, "wait_for_last_build_index_on_table_running timeout")
        return "wait_timeout"
    }


    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    } 
    sql "set enable_memtable_on_sink_node = true"
    def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true'
            set 'format', 'json'
            set 'max_filter_ratio', '0.1'
            file file_name // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition

            check { result, exception, startTime, endTime ->
                if (exception != null) {
                        throw exception
                }
                logger.info("Stream load ${file_name} result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                // assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    def table_name = "github_events"
    sql """DROP TABLE IF EXISTS ${table_name}"""
    table_name = "github_events"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant not null
            -- INDEX idx_var(v) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4 
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "bloom_filter_columns" = "v");
    """
    set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "1")
    // 2015
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-1.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-2.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-3.json'}""")

    // build inverted index at middle of loading the data
    // ADD INDEX
    sql """ ALTER TABLE github_events ADD INDEX idx_var (`v`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") """
    wait_for_latest_op_on_table_finish("github_events", timeout)

    // 2022
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-16.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-10.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-22.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-23.json'}""")

    // BUILD INDEX and expect state is FINISHED 
    sql """ BUILD INDEX idx_var ON  github_events"""
    state = wait_for_last_build_index_on_table_finish("github_events", timeout)
    assertEquals("FINISHED", state)

    // add bloom filter at the end of loading data 

    def tablets = sql_return_maparray """ show tablets from github_events; """
    // trigger compactions for all tablets in github_events
    trigger_and_wait_compaction("github_events", "cumulative")

    sql """set enable_match_without_inverted_index = false"""
    sql """ set enable_common_expr_pushdown = true """
    // filter by bloom filter
    qt_sql """select cast(v["payload"]["pull_request"]["additions"] as int)  from github_events where cast(v["repo"]["name"] as string) = 'xpressengine/xe-core' order by 1;"""
    qt_sql """select * from github_events where  cast(v["repo"]["name"] as string) = 'xpressengine/xe-core' order by 1 limit 10"""
    // query with inverted index
    qt_sql """select cast(v["payload"]["pull_request"]["additions"] as int)  from github_events where v["repo"]["name"] match 'xpressengine' order by 1;"""
    qt_sql """select count()  from github_events where v["repo"]["name"] match 'apache' order by 1;"""
}
