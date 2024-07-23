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


suite("test_agg_table_with_inverted_index", "p0"){
    def aggTable = "test_agg_table_with_inverted_index"
    sql "DROP TABLE IF EXISTS ${aggTable}"

    sql """
        CREATE TABLE IF NOT EXISTS ${aggTable}
        (
        `@timestamp` int(11) NULL,
        `clientip` varchar(20) NULL,
        `request` varchar(1000) NULL,
        `status` int(11) NULL,
        `size` int(11) SUM DEFAULT "0",
        INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
        INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true") COMMENT '',
        INDEX status_idx (`status`) USING INVERTED COMMENT ''
        ) AGGREGATE KEY(`@timestamp`, `clientip`, `request`, `status`)
        DISTRIBUTED BY HASH (`@timestamp`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "compaction_policy" = "time_series",
        "inverted_index_storage_format" = "v2",
        "compression" = "ZSTD"
        );
    """
    // load the json data
    streamLoad {
        table "${aggTable}"
        
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
            assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
            assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
        }
    }

    sql "sync"
    try {
        GetDebugPoint().enableDebugPointForAllBEs("match.invert_index_not_support_execute_match")
        sql """ SELECT COUNT FROM ${aggTable} WHERE request MATCH_ALL 'GET'; """
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("match.invert_index_not_support_execute_match")
    }
    
        
}