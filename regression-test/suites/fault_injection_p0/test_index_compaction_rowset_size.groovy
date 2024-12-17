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

suite("test_index_compaction_rowset_size", "p0, nonConcurrent") {

    def show_table_name = "test_index_compaction_rowset_size"

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }
    set_be_config.call("inverted_index_compaction_enable", "true")

    sql "DROP TABLE IF EXISTS ${show_table_name}"
    sql """ 
        CREATE TABLE ${show_table_name} (
            `@timestamp` int(11) NULL,
            `clientip` varchar(20) NULL,
            `request` varchar(500) NULL,
            `status` int NULL,
            `size` int NULL,
            INDEX clientip_idx (`clientip`) USING INVERTED COMMENT '',
            INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "unicode") COMMENT '',
            INDEX size_idx (`size`) USING INVERTED COMMENT '',
        ) ENGINE=OLAP
        DUPLICATE KEY(`@timestamp`, `clientip`)
        DISTRIBUTED BY HASH(`@timestamp`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "compaction_policy" = "time_series",
            "time_series_compaction_file_count_threshold" = "20",
            "disable_auto_compaction" = "true"
        );
    """

    def compaction = {

        def tablets = sql_return_maparray """ show tablets from ${show_table_name}; """

        for (def tablet in tablets) {
            int beforeSegmentCount = 0
            String tablet_id = tablet.TabletId
            (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            for (String rowset in (List<String>) tabletJson.rowsets) {
                beforeSegmentCount += Integer.parseInt(rowset.split(" ")[1])
            }
            assertEquals(beforeSegmentCount, 12)
        }

        // trigger compactions for all tablets in ${tableName}
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            assertEquals("success", compactJson.status.toLowerCase())
        }

        // wait for all compactions done
        for (def tablet in tablets) {
            Awaitility.await().atMost(10, TimeUnit.MINUTES).untilAsserted(() -> {
                Thread.sleep(5000)
                String tablet_id = tablet.TabletId
                backend_id = tablet.BackendId
                (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
                assertEquals(code, 0)
                def compactionStatus = parseJson(out.trim())
                assertEquals("compaction task for this tablet is not running", compactionStatus.msg.toLowerCase())
            });
        }

        
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
    }

   

    // 1. load data
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """
    sql """ INSERT INTO ${show_table_name} VALUES (100, "andy", "andy love apple", 100, 200); """

    try {
        GetDebugPoint().enableDebugPointForAllBEs("check_after_compaction_file_size")
        // 2. compaction
        compaction.call()
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("check_after_compaction_file_size")
    }
    

}
