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

import org.junit.Assert

suite("test_cloud_full_compaction_do_lease","nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def tableName = "test_cloud_full_compaction_do_lease"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName}
            (k int, v1 int, v2 int )
            UNIQUE KEY(k)
            DISTRIBUTED BY HASH (k) 
            BUCKETS 1  PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write"="true",
                "disable_auto_compaction" = "true");
        """

    (1..20).each{ id -> 
        sql """insert into ${tableName} select number, number, number from numbers("number"="10");"""
    }

    qt_sql "select count(1) from ${tableName};"

    def backends = sql_return_maparray('show backends')
    def tabletStats = sql_return_maparray("show tablets from ${tableName};")
    assert tabletStats.size() == 1
    def tabletId = tabletStats[0].TabletId
    def tabletBackendId = tabletStats[0].BackendId
    def tabletBackend
    for (def be : backends) {
        if (be.BackendId == tabletBackendId) {
            tabletBackend = be
            break;
        }
    }
    logger.info("tablet ${tabletId} on backend ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}");

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    def customBeConfig = [
        lease_compaction_interval_seconds : 2
    ]

    setBeConfigTemporary(customBeConfig) {
        // the default value of lease_compaction_interval_seconds is 20s, which means
        // the compaction lease thread will sleep for 20s first, we sleep 20s in case
        // so that compaction lease thread can be scheduled as we expect(2s)
        Thread.sleep(20000)
        try {
            // block the full compaction
            GetDebugPoint().enableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")

            GetDebugPoint().enableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets",
                    [tablet_id:"${tabletId}", start_version:"2", end_version:"10"]);

            {
                // trigger full compaction, it will be blokced in modify_rowsets
                logger.info("trigger full compaction on BE ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
                def (code, out, err) = be_run_full_compaction(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                assert  code == 0
                def compactJson = parseJson(out.trim())
                assert "success" == compactJson.status.toLowerCase()
            }
            
            // wait until the full compaction job's lease timeout(lease_compaction_interval_seconds * 4)
            Thread.sleep(10000);

            {
                // trigger cumu compaction
                logger.info("trigger cumu compaction on BE ${tabletBackend.Host} with backendId=${tabletBackend.BackendId}")
                def (code, out, err) = be_run_cumulative_compaction(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                assert code == 0
                def compactJson = parseJson(out.trim())
                // this will fail due to existing full compaction
                assert "e-2000" == compactJson.status.toLowerCase()
            }

            Thread.sleep(1000);

            // unblock full compaction
            GetDebugPoint().disableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")

            Thread.sleep(3000);

            {
                def (code, out, err) = be_show_tablet_status(tabletBackend.Host, tabletBackend.HttpPort, tabletId)
                assert code == 0
                def compactJson = parseJson(out.trim())
                assert compactJson["rowsets"].toString().contains("[2-21]")
            }
            

        } catch (Exception e) {
            logger.info(e.getMessage())
            assert false
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("CloudFullCompaction::modify_rowsets.block")
            GetDebugPoint().disableDebugPointForAllBEs("CloudSizeBasedCumulativeCompactionPolicy::pick_input_rowsets.set_input_rowsets")
        }
    }
}
