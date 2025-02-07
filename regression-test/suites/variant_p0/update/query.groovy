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

suite("update_test_query", "p0") {

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

    def table_name = "test_update"

    for (int i = 0; i < 10; i++) {
        load_json_data.call(table_name, """${getS3Url() + '/regression/load/ghdata_sample.json'}""")
    }

    def normal_check = {
        qt_sql """ select count() from ${table_name} """
        qt_sql """ select v['actor'] from ${table_name} order by k limit 1"""
        qt_sql """ select count(cast (v['repo']['url'] as text)) from ${table_name} group by cast (v['type'] as text) """
        qt_sql """ select max(cast (v['public'] as tinyint)) from ${table_name}"""
    }

    // mv1, mv2
    def mv_check = {
        qt_sql """ select max(kk) from (select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name}) as mv; """
        qt_sql """ select min(kk) from (select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name}) as mv; """
        qt_sql """ select count(kk) from (select (abs(cast(v['repo']['id'] as int)) + cast(v['payload']['review']['user']['id'] as int) + 20) as kk from ${table_name}) as mv; """
        qt_sql """ SELECT sum(cast(v['public'] as int))  FROM ${table_name} group by cast(v['type'] as text); """
    }

    // mv3, mv4 
    def mv_check2 = {
        qt_sql """ select max(element) from (select (abs(cast(v['org']['id'] as int)) + cast(v['payload']['comment']['id'] as int) + 30) as element from ${table_name}) as mv2; """
        qt_sql """ select min(element) from (select (abs(cast(v['org']['id'] as int)) + cast(v['payload']['comment']['id'] as int) + 30) as element from ${table_name}) as mv2; """
        qt_sql """ select count(element) from (select (abs(cast(v['org']['id'] as int)) + cast(v['payload']['comment']['id'] as int) + 30) as element from ${table_name}) as mv2; """
        qt_sql """ SELECT cast(v['payload']['before'] as text)  FROM ${table_name} order by cast(v['actor']['id'] as int) limit 1; """
    }

    createMV ("create materialized view table_mv3 as select (abs(cast(v['org']['id'] as int)) + cast(v['payload']['comment']['id'] as int) + 30) as element from ${table_name};")

     sql """
        CREATE MATERIALIZED VIEW table_mv4 BUILD IMMEDIATE REFRESH AUTO ON MANUAL DISTRIBUTED BY RANDOM BUCKETS 1 PROPERTIES
('replication_num' = '1') AS SELECT cast(v['payload']['before'] as text), cast(v['actor']['id'] as int)  FROM ${table_name};
    """
    waitingMTMVTaskFinishedByMvName("table_mv4")

    normal_check.call()
    mv_check.call()
    mv_check2.call()

    trigger_and_wait_compaction(table_name, "full")

    normal_check.call()
    mv_check.call()
    mv_check2.call()

    def table_name_sc = "test_update_sc"

    for (int i = 0; i < 10; i++) {
        load_json_data.call(table_name_sc, """${getS3Url() + '/regression/load/ghdata_sample.json'}""")
    }

    def schema_change = {
        def tablets = sql_return_maparray """ show tablets from ${table_name_sc}; """
        Set<String> rowsetids = new HashSet<>();
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
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
        sql """ alter table ${table_name_sc} modify column v variant null"""
        Awaitility.await().atMost(30, TimeUnit.MINUTES).untilAsserted(() -> {
            Thread.sleep(30000)
            tablets = sql_return_maparray """ show tablets from ${table_name_sc}; """
            for (def tablet in tablets) {
                String tablet_id = tablet.TabletId
                (code, out, err) = curl("GET", tablet.CompactionStatus)
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

    def sc_check = {
        qt_sql """ select count() from ${table_name_sc} """
        qt_sql """ select v['actor'] from ${table_name_sc} order by k limit 1"""
        qt_sql """ select count(cast (v['repo']['url'] as text)) from ${table_name_sc} group by cast (v['type'] as text) """
        qt_sql """ select max(cast (v['public'] as tinyint)) from ${table_name_sc}"""
    }

    sc_check.call()
    schema_change.call()
    sc_check.call()

}
