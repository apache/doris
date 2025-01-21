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


suite("test_inverted_index_file_size", "nonConcurrent"){
    def tableName = "test_inverted_index_file_size"

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_config = { key, value ->

        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def load_data = {
        // load the json data
        streamLoad {
            table "${tableName}"

            set 'read_json_by_line', 'true'
            set 'format', 'json'
            file 'documents-1000.json' // import json file
            time 10000 // limit inflight 10s

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                log.info("Stream load result: ${result}".toString())
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
            }
        }
    }

    def test_table = { format ->
        sql "DROP TABLE IF EXISTS ${tableName}"
        sql """
            CREATE TABLE ${tableName} (
                `@timestamp` int(11) NULL COMMENT "",
                `clientip` varchar(20) NULL COMMENT "",
                `request` text NULL COMMENT "",
                `status` varchar(11) NULL COMMENT "",
                `size` int(11) NULL COMMENT "",
                INDEX clientip_idx (`clientip`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
                INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english", "support_phrase" = "true") COMMENT '',
                INDEX status_idx (`status`) USING INVERTED COMMENT '',
                INDEX size_idx (`size`) USING INVERTED COMMENT ''
            ) ENGINE=OLAP
            DUPLICATE KEY(`@timestamp`)
            COMMENT "OLAP"
            DISTRIBUTED BY RANDOM BUCKETS 1
            PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "${format}"
            );
        """

        load_data.call()
        load_data.call()
        load_data.call()
        load_data.call()
        load_data.call()

        qt_sql """ select count() from ${tableName} where clientip match '17.0.0.0' and request match 'GET' and status match '200' and size > 200 """
        qt_sql """ select count() from ${tableName} where clientip match_phrase '17.0.0.0' and request match_phrase 'GET' and status match '200' and size > 200 """
        trigger_and_wait_compaction(tableName, "full")
        qt_sql """ select count() from ${tableName} where clientip match '17.0.0.0' and request match 'GET' and status match '200' and size > 200 """
        qt_sql """ select count() from ${tableName} where clientip match_phrase '17.0.0.0' and request match_phrase 'GET' and status match '200' and size > 200 """

    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("file_size_not_in_rowset_meta")
        set_be_config.call("inverted_index_compaction_enable", "true")
        test_table.call("V1")
        test_table.call("V2")
        set_be_config.call("inverted_index_compaction_enable", "false")
        test_table.call("V1")
        test_table.call("V2")
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("file_size_not_in_rowset_meta")
        set_be_config.call("inverted_index_compaction_enable", "true")
    }

}
