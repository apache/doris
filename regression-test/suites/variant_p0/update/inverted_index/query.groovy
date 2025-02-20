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

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("update_test_index_query", "p0") {

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

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def compaction = {compact_table_name ->

        def tablets = sql_return_maparray """ show tablets from ${compact_table_name}; """

        // trigger compactions for all tablets in ${tableName}
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def backend_id = tablet.BackendId
            def (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            assertEquals("success", compactJson.status.toLowerCase())
        }

        // wait for all compactions done
        for (def tablet in tablets) {
            Awaitility.await().atMost(30, TimeUnit.MINUTES).untilAsserted(() -> {
                Thread.sleep(1000)
                String tablet_id = tablet.TabletId
                def backend_id = tablet.BackendId
                def (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("compaction task for this tablet is not running", compactionStatus.msg.toLowerCase())
            });
        }

        
        for (def tablet in tablets) {
            int afterSegmentCount = 0
            String tablet_id = tablet.TabletId
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
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
    }

    def normal_check = {check_table_name->
        sql """set enable_match_without_inverted_index = false """ 
        sql """ set inverted_index_skip_threshold = 0 """
        qt_sql """ select count() from ${check_table_name} """
        qt_sql """ select count() from ${check_table_name} where cast (v['repo']['name'] as string) match 'github'"""
        qt_sql """ select count() from ${check_table_name} where cast (v['actor']['id'] as int) > 1575592 """
        qt_sql """ select count() from ${check_table_name} where cast (v['actor']['id'] as int) > 1575592 and  cast (v['repo']['name'] as string) match 'github'"""
    }

    def table_name = "test_update_index_compact"

    for (int i = 0; i < 2; i++) {
        load_json_data.call(table_name, """${getS3Url() + '/regression/load/ghdata_sample.json'}""")
    }

   GetDebugPoint().enableDebugPointForAllBEs("segment_iterator.apply_inverted_index")

    normal_check.call(table_name)

    compaction.call(table_name)

    normal_check.call(table_name)

    table_name = "test_update_index_compact2"

    normal_check.call(table_name)

    compaction.call(table_name)

    normal_check.call(table_name)


    def schema_change = {schema_change_table_name ->
        def tablets = sql_return_maparray """ show tablets from ${schema_change_table_name}; """
        Set<String> rowsetids = new HashSet<>();
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                int segmentCount = Integer.parseInt(rowset.split(" ")[1])
                if (segmentCount == 0) {
                    continue;
                }
                String rowsetid = rowset.split(" ")[4];
                rowsetids.add(rowsetid)
                logger.info("rowsetid: " + rowsetid)
            }
        }
        sql """ alter table ${schema_change_table_name} modify column v variant null"""
        Awaitility.await().atMost(30, TimeUnit.MINUTES).untilAsserted(() -> {
            Thread.sleep(1000)
            tablets = sql_return_maparray """ show tablets from ${schema_change_table_name}; """
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                def (code, out, err) = curl("GET", tablet.CompactionStatus)
                logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def tabletJson = parseJson(out.trim())
                assert tabletJson.rowsets instanceof List
                for (String rowset in (List<String>) tabletJson.rowsets) {
                    int segmentCount = Integer.parseInt(rowset.split(" ")[1])
                    if (segmentCount == 0) {
                        continue;
                    }
                    String rowsetid = rowset.split(" ")[4];
                    logger.info("rowsetid: " + rowsetid)
                    assertTrue(!rowsetids.contains(rowsetid))
                }
            }
        });
    }


    def table_name_sc = "test_update_index_sc"

    for (int i = 0; i < 2; i++) {
        load_json_data.call(table_name_sc, """${getS3Url() + '/regression/load/ghdata_sample.json'}""")
    }

    normal_check.call(table_name_sc)

    schema_change.call(table_name_sc)

    normal_check.call(table_name_sc)

    table_name_sc = "test_update_index_sc2"

    normal_check.call(table_name_sc)

    schema_change.call(table_name_sc)

    normal_check.call(table_name_sc)

   GetDebugPoint().disableDebugPointForAllBEs("segment_iterator.apply_inverted_index")
}
