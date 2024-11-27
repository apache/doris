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

suite("regression_test_variant_logdata", "nonConcurrent,p2"){
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

    def create_table = { table_name, key_type="DUPLICATE", buckets=(new Random().nextInt(15) + 1).toString()  ->
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql """
            CREATE TABLE IF NOT EXISTS ${table_name} (
                k bigint,
                v variant
            )
            ${key_type} KEY(`k`)
            DISTRIBUTED BY HASH(k) BUCKETS ${buckets}
            properties("replication_num" = "1", "disable_auto_compaction" = "false");
        """
    }
    // 12. streamload remote file
    table_name = "logdata"
    create_table.call(table_name, "DUPLICATE", "4")
    // sql "set enable_two_phase_read_opt = false;"
    // no sparse columns
    set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "1.0")
    load_json_data.call(table_name, """${getS3Url() + '/regression/load/logdata.json'}""")
    qt_sql_32 """ select json_extract(v, "\$.json.parseFailed") from logdata where  json_extract(v, "\$.json.parseFailed") != 'null' order by k limit 1;"""
    qt_sql_32_1 """select cast(v['json']['parseFailed'] as string) from  logdata where cast(v['json']['parseFailed'] as string) is not null and k = 162 limit 1;"""
    sql "truncate table ${table_name}"

    // 0.95 default ratio    
    set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "0.95")
    load_json_data.call(table_name, """${getS3Url() + '/regression/load/logdata.json'}""")
    qt_sql_33 """ select json_extract(v,"\$.json.parseFailed") from logdata where  json_extract(v,"\$.json.parseFailed") != 'null' order by k limit 1;"""
    qt_sql_33_1 """select cast(v['json']['parseFailed'] as string) from  logdata where cast(v['json']['parseFailed'] as string) is not null and k = 162 limit 1;"""
    sql "truncate table ${table_name}"

    // always sparse column
    set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "0.95")
    load_json_data.call(table_name, """${getS3Url() + '/regression/load/logdata.json'}""")
    qt_sql_34 """ select json_extract(v, "\$.json.parseFailed") from logdata where  json_extract(v,"\$.json.parseFailed") != 'null' order by k limit 1;"""
    sql "truncate table ${table_name}"
    qt_sql_35 """select json_extract(v,"\$.json.parseFailed")  from logdata where k = 162 and  json_extract(v,"\$.json.parseFailed") != 'null';"""
    qt_sql_35_1 """select cast(v['json']['parseFailed'] as string) from  logdata where cast(v['json']['parseFailed'] as string) is not null and k = 162 limit 1;"""
    // TODO add test case that some certain columns are materialized in some file while others are not materilized(sparse)
    // unique table
    set_be_config.call("variant_ratio_of_defaults_as_sparse_column", "1")
}