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

suite("test_cloud_calc_sync_version","docker") {
    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'cloud_tablet_rebalancer_interval_second=1',
    ]
    options.enableDebugPoints()
    options.cloudMode = true

    docker(options) {
        def write_cluster = "write_cluster"
        def read_cluster = "read_cluster"

        // Add two clusters
        cluster.addBackend(1, write_cluster)
        cluster.addBackend(1, read_cluster)

        sql "use @${write_cluster}"
        logger.info("==== switch to write cluster")
        def tableName = "test_cloud_calc_sync_version"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """
            CREATE TABLE IF NOT EXISTS ${tableName} (
                `k` int ,
                `v` int ,
            ) engine=olap
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            properties(
                "replication_num" = "1",
                "disable_auto_compaction" = "true")
            """

        sql """ INSERT INTO ${tableName} VALUES (1,10)"""
        sql """ INSERT INTO ${tableName} VALUES (2,20)"""
        sql """ INSERT INTO ${tableName} VALUES (3,30)"""
        qt_sql "select * from ${tableName} order by k;"

        def check_rs_metas = { tbl, check_func -> 
            def compactionUrl = sql_return_maparray("show tablets from ${tbl};").get(0).CompactionStatus
            def (code, out, err) = curl("GET", compactionUrl)
            assert code == 0
            def jsonMeta = parseJson(out.trim())
            logger.info("==== rowsets: ${jsonMeta.rowsets}, cumu point: ${jsonMeta["cumulative point"]}")
            check_func(jsonMeta.rowsets, jsonMeta["cumulative point"])
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

        def do_cumu_compaction = { def tbl, def tablet_id, int start, int end, int cp ->
            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::new_cumulative_point", [tablet_id: "${tablet_id}", cumu_point: "${cp}"])
            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets", [tablet_id: "${tablet_id}", start_version: "${start}", end_version: "${end}"])

            trigger_and_wait_compaction(tbl, "cumulative")

            GetDebugPoint().disableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::new_cumulative_point")
            GetDebugPoint().disableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
        }

        try {
            // [2-2],[3-3],[4-4] -> [2,4]
            do_cumu_compaction(tableName, tabletId, 2, 4, 5)
            qt_sql "select * from ${tableName} order by k;"
            check_rs_metas(tableName, {def rowsets, def cumu_point ->
                assert rowsets.size() == 2
                assert cumu_point as int == 5
                assert rowsets[1].contains("[2-4]")
            })

            sql """ INSERT INTO ${tableName} VALUES (4,40)""" // ver=5
            sql """ INSERT INTO ${tableName} VALUES (5,50)""" // ver=6
            sql "sync;"

            GetDebugPoint().enableDebugPointForAllBEs("CloudFullCompaction::execute_compact.block", [tablet_id: "${tabletId}"])
            def t1 = thread("full compaction") {
                // [2,4],[5-5],[6-6] -> [2,6]
                sql "use @${write_cluster}"
                trigger_and_wait_compaction(tableName, "full")
            }

            sleep(1500)
            sql """ INSERT INTO ${tableName} VALUES (1,60)""" // ver=7
            sql """ INSERT INTO ${tableName} VALUES (2,70)""" // ver=8
            sql "sync;"
            qt_write_cluster_new_write "select * from ${tableName} order by k;"


            // read cluster sync rowsets [2-4],[5-5],[6-6],[7-7],[8-8], bc_cnt=0, cc_cnt=1, cp=4
            sql "use @${read_cluster}"
            logger.info("==== switch to read cluster")
            qt_read_cluster_query "select * from ${tableName} order by k;"
            check_rs_metas(tableName, {def rowsets, def cumu_point ->
                assert rowsets.size() == 6
                assert cumu_point as int == 5
                assert rowsets[1].contains("[2-4]")
                assert rowsets[2].contains("[5-5]")
                assert rowsets[3].contains("[6-6]")
                assert rowsets[4].contains("[7-7]")
                assert rowsets[5].contains("[8-8]")
            })


            sql "use @${write_cluster}"
            logger.info("==== switch to write cluster")
            GetDebugPoint().disableDebugPointForAllBEs("CloudFullCompaction::execute_compact.block")
            t1.get()
            qt_write_cluster_full_compaction "select * from ${tableName} order by k;"
            check_rs_metas(tableName, {def rowsets, def cumu_point ->
                assert rowsets.size() == 4
                assert cumu_point as int == 7 // updated by full compaction
                assert rowsets[1].contains("[2-6]")
                assert rowsets[2].contains("[7-7]")
                assert rowsets[3].contains("[8-8]")
            })


            do_cumu_compaction(tableName, tabletId, 7, 8, 7)
            qt_write_cluster_cumu_compaction "select * from ${tableName} order by k;"
            check_rs_metas(tableName, {def rowsets, def cumu_point ->
                assert rowsets.size() == 3
                assert cumu_point as int == 7
                assert rowsets[1].contains("[2-6]")
                assert rowsets[2].contains("[7-8]")
            })

            sql """ INSERT INTO ${tableName} VALUES (1,80)""" // ver=9
            sql "sync;"
            qt_write_cluster_new_write "select * from ${tableName} order by k;"


            // read cluster will read dup keys of ver=9 to ver=7 because it will not sync rowset [7-8]
            sql "use @${read_cluster}"
            logger.info("==== switch to read cluster")
            sql "set disable_nereids_rules=ELIMINATE_GROUP_BY;"
            qt_read_cluster_check_dup_key "select k,count() from ${tableName} group by k having count()>1;"
            qt_read_cluster_res "select * from ${tableName} order by k;"

        } catch (Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
            GetDebugPoint().clearDebugPointsForAllFEs()
        }
    }
}
