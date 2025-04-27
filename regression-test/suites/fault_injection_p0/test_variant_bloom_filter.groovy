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

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_variant_bloom_filter", "nonConcurrent") {

    def index_table = "test_variant_bloom_filter"

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

    sql """DROP TABLE IF EXISTS ${index_table}"""
    sql """
        CREATE TABLE IF NOT EXISTS ${index_table} (
            k bigint,
            v variant
        )
        DUPLICATE KEY(`k`)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        properties("replication_num" = "1", "disable_auto_compaction" = "false", "bloom_filter_columns" = "v");
    """
    load_json_data.call(index_table, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    load_json_data.call(index_table, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    load_json_data.call(index_table, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    load_json_data.call(index_table, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")
    load_json_data.call(index_table, """${getS3Url() + '/regression/gharchive.m/2015-01-01-0.json'}""")

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def tablets = sql_return_maparray """ show tablets from ${index_table}; """


    for (def tablet in tablets) {
        int beforeSegmentCount = 0
        String tablet_id = tablet.TabletId
        def (code, out, err) = curl("GET", tablet.CompactionStatus)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        for (String rowset in (List<String>) tabletJson.rowsets) {
            beforeSegmentCount += Integer.parseInt(rowset.split(" ")[1])
        }
        assertEquals(beforeSegmentCount, 5)
    }

    // trigger compactions for all tablets in ${tableName}
    trigger_and_wait_compaction(index_table, "full")

    for (def tablet in tablets) {
        int afterSegmentCount = 0
        String tablet_id = tablet.TabletId
        (code, out, err) = curl("GET", tablet.CompactionStatus)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        for (String rowset in (List<String>) tabletJson.rowsets) {
            logger.info("rowset is: " + rowset)
            afterSegmentCount += Integer.parseInt(rowset.split(" ")[1])
        }
         assertEquals(afterSegmentCount, 1)
    }


    try {
        GetDebugPoint().enableDebugPointForAllBEs("bloom_filter_must_filter_data")

        // number
        qt_sql1 """ select cast(v['repo']['id'] as int) from ${index_table} where cast(v['repo']['id'] as int) = 20291263; """

        // string
        qt_sql2 """ select cast(v['repo']['name'] as text) from ${index_table} where cast(v['repo']['name'] as text) = "ridget/dotfiles"; """
    } finally {
    GetDebugPoint().disableDebugPointForAllBEs("bloom_filter_must_filter_data")
    }
}
