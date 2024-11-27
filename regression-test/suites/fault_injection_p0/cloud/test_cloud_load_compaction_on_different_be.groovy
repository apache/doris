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

suite("test_cloud_load_compaction_on_different_be","nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    def backends = sql_return_maparray('show backends')
    def replicaNum = 0
    def targetBackend = null
    for (def be : backends) {
        def alive = be.Alive.toBoolean()
        def decommissioned = be.SystemDecommissioned.toBoolean()
        if (alive && !decommissioned) {
            replicaNum++
            targetBackend = be
        }
    }

    if (replicaNum < 2) {
        return 
    }

    GetDebugPoint().clearDebugPointsForAllFEs()

    def tableName = "test_cloud_load_compaction_on_different_be"
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

    def tabletStats = sql_return_maparray("show tablets from ${tableName};")
    assertTrue(tabletStats.size() == 1)
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

    def idx = 0
    for (idx = 0; idx < backends.size(); idx++) {
        if (backends[idx].BackendId != tabletBackendId) {
            break;
        }
    }
    def anotherBackend = backends[idx]

    GetDebugPoint().clearDebugPointsForAllFEs()
    try {
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.sleep", [sleep_time: 10])

        def t1 = Thread.start {
            sql """insert into ${tableName} select number, number, number from numbers("number"="10");"""
        }

        def t2 = Thread.start {
            sleep(4);
            logger.info("trigger compaction on another BE ${anotherBackend.Host} with backendId=${anotherBackend.BackendId}")
            def (code, out, err) = be_run_full_compaction(anotherBackend.Host, anotherBackend.HttpPort, tabletId)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            assertEquals("success", compactJson.status.toLowerCase())
        }

        t1.join()
        t2.join()

        def res = sql "select k, count(*) from ${tableName} group by k having count(*) > 1;"
        assertTrue(res.size() == 0)
    } catch (Exception e) {
        logger.info(e.getMessage())
        assertTrue(false) 
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.sleep")
    }
}
