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

suite("double_write_schema_change_with_variant", "nonConcurrent") {
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
        DISTRIBUTED BY HASH(k) BUCKETS 2
        properties("replication_num" = "1", "disable_auto_compaction" = "false");
    """

    set_be_config.call("memory_limitation_per_thread_for_schema_change_bytes", "6294967296")
    set_be_config.call("write_buffer_size", "10240")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-1.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-2.json'}""")
    load_json_data.call(table_name, """${getS3Url() + '/regression/gharchive.m/2015-01-01-3.json'}""") 

    def getJobState = { indexName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${indexName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    def insert_sql = """ insert into ${table_name} values(1, '{"id":"25061216922","type":"PushEvent","actor":{"id":100067519,"login":"savorfamily","display_login":"savorfamily","gravatar_id":"123","url":"https://api.github.com/users/savorfamily","avatar_url":"https://avatars.githubusercontent.com/u/100067519?"},"repo":{"id":461434218,"name":"savorfamily/upptime","url":"https://api.github.com/repos/savorfamily/upptime"},"payload":{"push_id":11572320522,"size":1,"distinct_size":1,"ref":"refs/heads/master","head":"81106d369f763cb729d9d77610ace252c9db53f0","before":"2bf823a4febcf809da126828ecef7617c8cc48ea","commits":[{"sha":"81106d369f763cb729d9d77610ace252c9db53f0","author":{"email":"73812536+upptime-bot@users.noreply.github.com","name":"Upptime Bot"},"message":":bento: Update graphs [skip ci]","distinct":true,"url":"https://api.github.com/repos/savorfamily/upptime/commits/81106d369f763cb729d9d77610ace252c9db53f0"}]},"public":true,"created_at":"2022-11-07T02:00:00Z"}', "123111.0") """

    def double_write = { ->
        int max_try_time = 3000
        while (max_try_time--){
            String result = getJobState(table_name)
            if (result == "FINISHED") {
                sleep(3000)
                break
            } else {
                if (result == "RUNNING") {
                    sql insert_sql
                }
                sleep(200)
                if (max_try_time < 1){
                    assertEquals(1,2)
                }
            }
        }
    }

    qt_sql "select v['type'], v['id'], v['created_at'] from ${table_name} where cast(v['id'] as bigint) != 25061216922 order by k, cast(v['id'] as bigint) limit 10"

    sql """ ALTER TABLE ${table_name} modify COLUMN change_column text"""
    double_write.call()

    sql """ALTER TABLE ${table_name} drop index idx_var"""
    double_write.call()
    qt_sql "select v['type'], v['id'], v['created_at'] from ${table_name} where cast(v['id'] as bigint) != 25061216922 order by k,  cast(v['id'] as bigint) limit 10"

    // createMV("create materialized view xxx as select k, sum(k) from ${table_name} group by k order by k;")
    // qt_sql "select v['type'], v['id'], v['created_at'] from ${table_name} where cast(v['id'] as bigint) != 25061216922 order by k,  cast(v['id'] as bigint) limit 10"
    // restore configs
    set_be_config.call("memory_limitation_per_thread_for_schema_change_bytes", "2147483648")
    set_be_config.call("write_buffer_size", "209715200")
}
