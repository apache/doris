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

suite("test_stream_load_with_inverted_index_p0", "p0") {

    def set_be_config = { key, value ->
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def tableName = "test_stream_load_with_inverted_index"
    def calc_file_crc_on_tablet = { ip, port, tablet ->
        return curl("GET", String.format("http://%s:%s/api/calc_crc?tablet_id=%s", ip, port, tablet))
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

    String backend_id;
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def test = { format -> 
        sql """ DROP TABLE IF EXISTS ${tableName}; """
        sql """
            CREATE TABLE ${tableName} (
                k bigint,
                v variant,
                INDEX idx_v (`v`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`k`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`k`) BUCKETS 10
            PROPERTIES ( "replication_num" = "1", "inverted_index_storage_format" = "${format}", "disable_auto_compaction" = "true");
        """

        load_json_data.call(tableName, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
        
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = calc_file_crc_on_tablet(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run calc file: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def resultJson = parseJson(out.trim())
            assertEquals(resultJson.start_version, "0")
            assertEquals(resultJson.end_version, "2")
            assertEquals(resultJson.rowset_count, "2")
        }
        qt_sql_1 """
        select cast(v["repo"]["name"] as string) from ${tableName} where cast(v["repo"]["name"] as string) match_phrase_prefix "davesbingrewardsbot";
        """

        sql """ DROP TABLE IF EXISTS ${tableName}; """
    }

    set_be_config("inverted_index_ram_dir_enable", "true")
    test.call("V1")
    test.call("V2")
    set_be_config("inverted_index_ram_dir_enable", "false")
    test.call("V1")
    test.call("V2")
    set_be_config("inverted_index_ram_dir_enable", "true")
}