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

suite("test_ordered_compaction_num_seg_rows","nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def custoBeConfig = [
        ordered_data_compaction_min_segment_size : 1,
        enable_ordered_data_compaction: true,
        segments_key_bounds_truncation_threshold: -1 // this config may be fuzzied to value that will make the condition of ordered compaction not met, so manually set it to -1 to disable the truncation
    ]
    setBeConfigTemporary(custoBeConfig) {

        String backend_id;
        def backendId_to_backendIP = [:]
        def backendId_to_backendHttpPort = [:]
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

        backend_id = backendId_to_backendIP.keySet()[0]
        def (code1, out1, err1) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))

        logger.info("Show config: code=" + code1 + ", out=" + out1 + ", err=" + err1)
        assert code1 == 0


        def tableName = "test_ordered_compaction_num_seg_rows"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` int ,
                `v` int ,
            ) engine=olap
            duplicate KEY(k)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            properties(
                "replication_num" = "1",
                "disable_auto_compaction" = "true")
            """

        sql """ INSERT INTO ${tableName} VALUES (10,10),(12,12),(14,14)"""
        sql """ INSERT INTO ${tableName} VALUES (20,20),(21,21),(22,22),(23,23),(24,24)"""
        sql """ INSERT INTO ${tableName} VALUES (30,30),(31,31)"""
        qt_sql "select * from ${tableName} order by k;"

        def check_rs_metas = { tbl, check_func -> 
            def compactionUrl = sql_return_maparray("show tablets from ${tbl};").get(0).MetaUrl
            def (code, out, err) = curl("GET", compactionUrl)
            assert code == 0
            def jsonMeta = parseJson(out.trim())
            logger.info("==== tablet_meta.rs_metas: ${jsonMeta.rs_metas}")
            check_func(jsonMeta.rs_metas)
        }

        def tabletStats = sql_return_maparray("show tablets from ${tableName};")
        def tabletId = tabletStats[0].TabletId
        def tabletBackendId = tabletStats[0].BackendId
        def tabletBackend
        def backends = sql_return_maparray('show backends')
        for (def be : backends) {
            if (be.BackendId == tabletBackendId) {
                tabletBackend = be
                break;
            }
        }
        logger.info("==== tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");

        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()

        def do_cumu_compaction = { def tbl, def tablet_id, int start, int end ->
            GetDebugPoint().enableDebugPointForAllBEs("SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets", [tablet_id: "${tablet_id}", start_version: "${start}", end_version: "${end}"])
            trigger_and_wait_compaction(tbl, "cumulative")
            GetDebugPoint().disableDebugPointForAllBEs("SizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
        }

        try {
            // [2-2],[3-3],[4-4] -> [2,4]
            do_cumu_compaction(tableName, tabletId, 2, 4)
            qt_sql "select * from ${tableName} order by k;"

            check_rs_metas(tableName, {def rowsets ->
                assert rowsets.size() == 2
                def num_segment_rows = rowsets[1].num_segment_rows
                logger.info("==== num_segment_rows: ${num_segment_rows}")
                assert num_segment_rows.size() == 3
                assert num_segment_rows[0] == 3
                assert num_segment_rows[1] == 5
                assert num_segment_rows[2] == 2
            })

        } catch (Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
