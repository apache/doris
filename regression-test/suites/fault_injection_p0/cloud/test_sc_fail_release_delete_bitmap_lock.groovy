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

suite("test_sc_fail_release_delete_bitmap_lock", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }
    GetDebugPoint().clearDebugPointsForAllFEs()

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    def backendId_to_params = [string: [:]]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def set_be_param = { paramName, paramValue ->
        // for eache be node, set paramName=paramValue
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }

    def reset_be_param = { paramName ->
        // for eache be node, reset paramName to default
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def original_value = backendId_to_params.get(id).get(paramName)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, original_value))
            assertTrue(out.contains("OK"))
        }
    }

    def get_be_param = { paramName ->
        // for eache be node, get param value by default
        def paramValue = ""
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            // get the config value from be
            def (code, out, err) = curl("GET", String.format("http://%s:%s/api/show_config?conf_item=%s", beIp, bePort, paramName))
            assertTrue(code == 0)
            assertTrue(out.contains(paramName))
            // parsing
            def resultList = parseJson(out)[0]
            assertTrue(resultList.size() == 4)
            // get original value
            paramValue = resultList[2]
            backendId_to_params.get(id, [:]).put(paramName, paramValue)
        }
    }
    def customFeConfig = [schema_change_max_retry_time: 0]
    def tableName = "tbl_basic"

    def do_insert_into = {
        sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "Emily", 25),(2, "Benjamin", 35);"""
    }

    def waitForSC = {
        Awaitility.await().atMost(120, TimeUnit.SECONDS).pollDelay(100, TimeUnit.MILLISECONDS).pollInterval(100, TimeUnit.MILLISECONDS).until(() -> {
            def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1"
            assert res.size() == 1
            log.info("res[0].State:" + res[0].State)
            if (res[0].State == "FINISHED" || res[0].State == "CANCELLED") {
                return true;
            }
            return false;
        });
    }

    try {
        // create table
        sql """ drop table if exists ${tableName}; """

        sql """
        CREATE TABLE `${tableName}` (
            `id` int(11) NOT NULL,
            `name` varchar(10) NULL,
            `score` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true",
            "replication_num" = "1"
        );
        """
        setFeConfigTemporary(customFeConfig) {
            get_be_param("delete_bitmap_lock_expiration_seconds")
            set_be_param("delete_bitmap_lock_expiration_seconds", "60")
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (1, "AAA", 15);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (2, "BBB", 25);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (3, "CCC", 35);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (4, "DDD", 45);"""
            sql """ INSERT INTO ${tableName} (id, name, score) VALUES (5, "EEE", 55);"""
            def tablets = sql_return_maparray """ show tablets from ${tableName}; """
            logger.info("tablets: " + tablets)
            GetDebugPoint().enableDebugPointForAllBEs("CloudMetaMgr::test_update_delete_bitmap_fail")
            sql "alter table ${tableName} modify column score varchar(100);"
            waitForSC()
            def res = sql_return_maparray "SHOW ALTER TABLE COLUMN WHERE TableName='${tableName}' ORDER BY createtime DESC LIMIT 1"
            assert res[0].State == "CANCELLED"
            assert res[0].Msg.contains("[DELETE_BITMAP_LOCK_ERROR]test update delete bitmap failed")
            GetDebugPoint().disableDebugPointForAllBEs("CloudMetaMgr::test_update_delete_bitmap_fail")
            def now = System.currentTimeMillis()
            do_insert_into()
            def time_cost = System.currentTimeMillis() - now
            log.info("time_cost(ms): ${time_cost}")
            assertTrue(time_cost < 10000, "wait time should less than 10s")
        }
    } finally {
        reset_be_param("delete_bitmap_lock_expiration_seconds")
        GetDebugPoint().clearDebugPointsForAllBEs()
    }

}
