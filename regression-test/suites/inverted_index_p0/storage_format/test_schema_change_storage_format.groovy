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

suite("test_local_schema_change_storge_format", "p0") {
    if (isCloudMode()) {
        return;
    }
    def calc_file_crc_on_tablet = { ip, port, tablet ->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s", ip, port, tablet))
    }
    def set_be_config = { key, value ->
        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
        logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
    }

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
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant,
            change_column double,
            INDEX idx_var(v) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "true");
    """

    set_be_config.call("memory_limitation_per_thread_for_schema_change_bytes", "6294967296")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-1.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-2.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-3.json'}""") 

    def getJobState = { indexName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${indexName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    def wait_for_schema_change = { ->
        int max_try_time = 3000
        while (max_try_time--){
            String result = getJobState(table_name)
            if (result == "FINISHED") {
                sleep(3000)
                break
            } else {
                if (result == "RUNNING") {
                    sleep(3000)
                }
                if (max_try_time < 1){
                    assertEquals(1,2)
                }
            }
        }
    }
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    tablets = sql_return_maparray """ show tablets from ${table_name}; """
    String tablet_id = tablets[0].TabletId
    String backend_id = tablets[0].BackendId
    String ip = backendId_to_backendIP.get(backend_id)
    String port = backendId_to_backendHttpPort.get(backend_id)
    def (code_0, out_0, err_0) = calc_file_crc_on_tablet(ip, port, tablet_id)
    logger.info("Run calc_file_crc_on_tablet: code=" + code_0 + ", out=" + out_0 + ", err=" + err_0)
    assertTrue(code_0 == 0)
    assertTrue(out_0.contains("crc_value"))
    assertTrue(out_0.contains("used_time_ms"))
    assertEquals("0", parseJson(out_0.trim()).start_version)
    assertEquals("5", parseJson(out_0.trim()).end_version)
    assertEquals("5", parseJson(out_0.trim()).rowset_count)
    // inverted index format = v2, 4 segments + 4 inverted index file
    assertEquals("8", parseJson(out_0.trim()).file_count)

    sql """ ALTER TABLE ${table_name} modify COLUMN change_column text"""
    wait_for_schema_change.call()

    tablets = sql_return_maparray """ show tablets from ${table_name}; """
    tablet_id = tablets[0].TabletId
    backend_id = tablets[0].BackendId
    ip = backendId_to_backendIP.get(backend_id)
    port = backendId_to_backendHttpPort.get(backend_id)
    def (code_1, out_1, err_1) = calc_file_crc_on_tablet(ip, port, tablet_id)
    logger.info("Run calc_file_crc_on_tablet: code=" + code_1 + ", out=" + out_1 + ", err=" + err_1)
    assertTrue(code_1 == 0)
    assertTrue(out_1.contains("crc_value"))
    assertTrue(out_1.contains("used_time_ms"))
    assertEquals("0", parseJson(out_1.trim()).start_version)
    assertEquals("5", parseJson(out_1.trim()).end_version)
    assertEquals("5", parseJson(out_1.trim()).rowset_count)
    // inverted index format = v2, 4 segments + 4 inverted index file
    assertEquals("8", parseJson(out_1.trim()).file_count)

    // sql """ALTER TABLE ${table_name} drop index idx_var"""
    // double_write.call()
    // qt_sql "select v['type'], v['id'], v['created_at'] from ${table_name} where cast(v['id'] as bigint) != 25061216922 order by k,  cast(v['id'] as bigint) limit 10"


    set_be_config.call("memory_limitation_per_thread_for_schema_change_bytes", "2147483648")
}
