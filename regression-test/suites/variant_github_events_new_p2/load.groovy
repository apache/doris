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

suite("regression_test_variant_github_events_p2", "nonConcurrent,p2"){
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    } 
    def load_json_data = {table_name, file_name ->
        // load the json data
        streamLoad {
            table "${table_name}"

            // set http request header params
            set 'read_json_by_line', 'true' 
            set 'format', 'json' 
            set 'max_filter_ratio', '0.1'
            set 'memtable_on_sink_node', 'true'
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
    set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "1")

    def table_name = "github_events"
    sql """DROP TABLE IF EXISTS ${table_name}"""
    table_name = "github_events"
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            k bigint,
            v variant,
            INDEX idx_var(v) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 4 
        properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """
    // 2015
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-1.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-2.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-3.json'}""")
    // 2022
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-16.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-10.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-22.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2022-11-07-23.json'}""")
    // TODO fix compaction issue, this case could be stable
    qt_sql """select cast(v["payload"]["pull_request"]["additions"] as int)  from github_events where cast(v["repo"]["name"] as string) = 'xpressengine/xe-core' order by 1;"""
    // TODO add test case that some certain columns are materialized in some file while others are not materilized(sparse)

    sql """DROP TABLE IF EXISTS github_events_2"""
    sql """
        CREATE TABLE IF NOT EXISTS `github_events_2` (
        `k` BIGINT NULL,
        `v` text NULL,
        INDEX idx_var (`v`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
        ) ENGINE = OLAP DUPLICATE KEY(`k`) COMMENT 'OLAP' DISTRIBUTED BY HASH(`k`) BUCKETS 4 PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql """
        insert into github_events_2 select 1, cast(v["repo"]["name"] as string) FROM github_events;
    """
    // insert batches of nulls
    for(int t = 0; t <= 10; t += 1){ 
        long k = 9223372036854775107 + t
        sql """INSERT INTO github_events VALUES (${k}, NULL)"""
    }
    sql """ALTER TABLE github_events SET("bloom_filter_columns" = "v")"""
    // wait for add bloom filter finished
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='github_events' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }
    int max_try_time = 200
    while (max_try_time--){
        String result = getJobState("github_events")
        if (result == "FINISHED") {
            break
        } else {
            sleep(2000)
            if (max_try_time < 1){
                assertEquals(1,2)
            }
        }
    }
    sql """ALTER TABLE github_events ADD COLUMN v2 variant DEFAULT NULL"""
    for(int t = 0; t <= 10; t += 1){ 
        long k = 9223372036854775107 + t
        sql """INSERT INTO github_events VALUES (${k}, '{"aaaa" : 1234, "bbbb" : "11ssss"}', '{"xxxx" : 1234, "yyyy" : [1.111]}')"""
    }
    sql """ALTER TABLE github_events DROP COLUMN v2"""
    sql """DELETE FROM github_events where k >= 9223372036854775107"""

    qt_sql_select_count """ select count(*) from github_events_2; """
}
