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

suite("test_group_commit_error", "nonConcurrent") {
    def tableName = "test_group_commit_error"
    def beConfigName = "group_commit_create_plan_timeout_ms"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def get_be_config = { String backend_id, String key ->
        def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
        logger.info("show config: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def configList = parseJson(out.trim())
        assert configList instanceof List
        for (Object ele in (List) configList) {
            assert ele instanceof List<String>
            if (((List<String>) ele)[0] == key) {
                return ((List<String>) ele)[2]
            }
        }
        assertTrue(false, "Failed to find BE config: " + key)
    }

    def set_be_config = { String key, String value ->
        for (String backend_id : backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
        }
    }

    def originBeConfig = [:]
    for (String backend_id : backendId_to_backendIP.keySet()) {
        originBeConfig[backend_id] = get_be_config(backend_id, beConfigName)
    }

    onFinish {
        for (String backend_id : originBeConfig.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id),
                    backendId_to_backendHttpPort.get(backend_id), beConfigName, originBeConfig[backend_id])
            logger.info("restore config: code=" + code + ", out=" + out + ", err=" + err)
        }
        GetDebugPoint().clearDebugPointsForAllBEs()
        GetDebugPoint().clearDebugPointsForAllFEs()
    }

    set_be_config(beConfigName, "20000")

    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DISTRIBUTED BY HASH(`k`) 
        BUCKETS 5 
        properties("replication_num" = "1", "group_commit_interval_ms"="2000")
    """

    GetDebugPoint().clearDebugPointsForAllBEs()
    GetDebugPoint().clearDebugPointsForAllFEs()
    try {
        GetDebugPoint().enableDebugPointForAllFEs("OlapInsertExecutor.beginTransaction.failed")
        sql """ set group_commit = async_mode """
        sql """ insert into ${tableName} values (1, 1) """
        assertTrue(false)
    } catch (Exception e) {
        logger.info("failed: " + e.getMessage())
        assertTrue(e.getMessage().contains("begin transaction failed"))
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("FragmentMgr.exec_plan_fragment.failed")
        sql """ set group_commit = async_mode """
        sql """ insert into ${tableName} values (1, 1) """
        assertTrue(false)
    } catch (Exception e) {
        logger.info("failed: " + e.getMessage())
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("FragmentMgr.exec_plan_fragment.failed")
        sql """ set group_commit = async_mode """
        sql """ insert into ${tableName} values (2, 2) """
    } catch (Exception e) {
        logger.info("failed: " + e.getMessage())
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("FragmentMgr._get_query_ctx.failed")
        sql """ set group_commit = async_mode """
        sql """ insert into ${tableName} values (3, 3) """
        assertTrue(false)
    } catch (Exception e) {
        logger.info("failed: " + e.getMessage())
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue.add_block.failed")
        sql """ set group_commit = async_mode """
        sql """ insert into ${tableName} values (4, 4) """
        assertTrue(false)
    } catch (Exception e) {
        logger.info("failed: " + e.getMessage())
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

    try {
        GetDebugPoint().enableDebugPointForAllBEs("LoadBlockQueue.add_block.block")
        Thread thread = new Thread(() -> {
            sql """ set group_commit = async_mode """
            sql """ insert into ${tableName} values (5, 4) """
        })
        thread.start()
        sleep(4000)
        GetDebugPoint().clearDebugPointsForAllBEs()
        thread.join()
        def result = sql "select count(*) from ${tableName}"
        logger.info("rowCount 0: ${result}")
    } catch (Exception e) {
        logger.warn("unexpected failed: " + e.getMessage())
        assertTrue(false, "unexpected failed: " + e.getMessage())
    } finally {
        GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
