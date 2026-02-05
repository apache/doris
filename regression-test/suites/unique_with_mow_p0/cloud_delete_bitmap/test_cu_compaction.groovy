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

import org.apache.doris.regression.suite.ClusterOptions

suite("test_cu_compaction", "docker") {
    def options = new ClusterOptions()
    options.beConfigs += [
        'delete_bitmap_store_version=2',
        'delete_bitmap_max_bytes_store_in_fdb=-1',
        'enable_delete_bitmap_store_v2_check_correctness=true',
        'enable_debug_points=true',
        'enable_java_support=false',
         // trash
        'pending_data_expire_time_sec=0',
        'path_gc_check_interval_second=1',
        'trash_file_expire_time_sec=0',
        'tablet_rowset_stale_sweep_time_sec=1',
        'min_garbage_sweep_interval=1'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    def checkRowsetNum = { tablet, rowsetNum ->
        String compactionUrl = tablet["CompactionStatus"]
        def (code, out, err) = curl("GET", compactionUrl)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        return tabletJson.rowsets.size() <= rowsetNum
    }

    docker(options) {
        sql """
            CREATE TABLE test_cu_compaction (
                `k` int(11) NOT NULL,
                `v` int(11) NOT NULL
            ) ENGINE=OLAP
            unique KEY(`k`)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
                "disable_auto_compaction"="true",
                "replication_num" = "1"
            ); 
        """

        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        def backendId_to_params = [string: [:]]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)

        def tablets = sql_return_maparray "show tablets from test_cu_compaction"
        assertEquals(1, tablets.size())
        def tablet = tablets[0]
        String tablet_id = tablet.TabletId
        def backend_id = tablet.BackendId

        def triggerCompaction = { start_version, end_version, rowsetNum ->
            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                    [tablet_id: "${tablet.TabletId}", start_version: start_version, end_version: end_version])
            def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            logger.info("compact json: " + compactJson)

            // check rowset num
            def check_result = false
            for (int i = 0; i < 20; i++) {
                check_result = checkRowsetNum(tablet, rowsetNum)
                if (check_result) {
                    break
                }
                sleep(100)
            }
            assertTrue(check_result)
        }

        // v2
        sql """ insert into test_cu_compaction values(11, 11), (12, 12), (13, 13), (14, 14), (15, 15), (16, 16), (17, 17), (18, 18), (19, 19) """
        // v3
        sql """ insert into test_cu_compaction values(21, 21), (22, 22), (23, 23), (24, 24), (25, 25), (26, 26), (27, 27), (28, 28), (29, 29) """
        // v4
        sql """ insert into test_cu_compaction values(31, 31), (32, 32), (33, 33), (34, 34), (35, 35), (36, 36), (37, 37), (38, 38), (39, 39) """
        // v5
        sql """ insert into test_cu_compaction values(41, 41), (42, 42), (43, 43), (44, 44), (45, 45), (46, 46), (47, 47), (48, 48), (49, 49),
                       (11, 41), (21, 41), (31, 41) """
        // v6
        sql """ insert into test_cu_compaction values(51, 51), (52, 52), (53, 53), (54, 54), (55, 55), (56, 56), (57, 57), (58, 58), (59, 59),
                       (12, 51), (22, 51), (32, 51), (42, 51) """
        // v7
        sql """ insert into test_cu_compaction values(61, 61), (62, 62), (63, 63), (64, 64), (65, 65), (66, 66), (67, 67), (68, 68), (69, 69),
                       (13, 61), (23, 61), (33, 61), (43, 61), (53, 61) """
        order_qt_select_1 "SELECT * FROM test_cu_compaction;"

        // trigger compaction
        triggerCompaction(2, 4, 5)
        order_qt_select_2 "SELECT * FROM test_cu_compaction;"

        triggerCompaction(5, 7, 3)
        order_qt_select_3 "SELECT * FROM test_cu_compaction;"

        // v8
        sql """ insert into test_cu_compaction values (75, 75), (76, 76), (77, 77), (78, 78), (79, 79),
                       (14, 71), (24, 71), (34, 71), (44, 71), (54, 71), (64, 71) """
        // v9
        sql """ insert into test_cu_compaction values (85, 85), (86, 86), (87, 87), (88, 88), (89, 89),
                       (15, 81), (25, 81), (35, 81), (45, 81), (55, 81), (65, 81), (75, 81) """
        // v10
        sql """ insert into test_cu_compaction values (99, 99),
                       (16, 91), (26, 91), (36, 91), (46, 91), (56, 91), (66, 91), (76, 91), (86, 91) """
        order_qt_select_4 "SELECT * FROM test_cu_compaction;"

        triggerCompaction(8, 9, 5)
        order_qt_select_5 "SELECT * FROM test_cu_compaction;"

        List<List<Object>> cnt = sql """ select k,count(*) a  from test_cu_compaction group by k having a > 1; """
        log.info("ensure there are no duplicated keys")
        assertEquals(0, cnt.size())
    }

}
