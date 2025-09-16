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

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_cumu_compaction_delay_fault_injection","nonConcurrent") {
    GetDebugPoint().clearDebugPointsForAllBEs()
    String backend_id;

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    backend_id = backendId_to_backendIP.keySet()[0]

    def set_be_param = { paramName, paramValue ->
        // for eache be node, set paramName=paramValue
        for (String id in backendId_to_backendIP.keySet()) {
            def beIp = backendId_to_backendIP.get(id)
            def bePort = backendId_to_backendHttpPort.get(id)
            def (code, out, err) = curl("POST", String.format("http://%s:%s/api/update_config?%s=%s", beIp, bePort, paramName, paramValue))
            assertTrue(out.contains("OK"))
        }
    }

    def trigger_cumu = { tableName ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        }
    }

    def assert_cumu_success = { tableName ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        def replicaNum = get_table_replica_num(tableName)
        logger.info("get table replica num: " + replicaNum)
        int rowsetCount = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 2 * replicaNum)
    }

    def assert_cumu_fail = { tableName ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """

        def replicaNum = get_table_replica_num(tableName)
        logger.info("get table replica num: " + replicaNum)
        int rowsetCount = 0
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            def (code, out, err) = curl("GET", tablet.CompactionStatus)
            logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def tabletJson = parseJson(out.trim())
            assert tabletJson.rowsets instanceof List
            rowsetCount +=((List<String>) tabletJson.rowsets).size()
        }
        assert (rowsetCount == 6 * replicaNum)
    }

    // ---------- PART 1 ----------
    // ---------- TEST 1-a ----------
    def tableName1a1 = "test_cumu_compaction_delay_fault_injection_1a1"
    def tableName1a2 = "test_cumu_compaction_delay_fault_injection_1a2"
    sql """ DROP TABLE IF EXISTS ${tableName1a1} force"""
    sql """ DROP TABLE IF EXISTS ${tableName1a2} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName1a1} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName1a1} VALUES (0,0)"""
    sql """ INSERT INTO ${tableName1a1} VALUES (1,0)"""
    sql """ INSERT INTO ${tableName1a1} VALUES (2,0)"""
    sql """ INSERT INTO ${tableName1a1} VALUES (3,0)"""
    sql """ INSERT INTO ${tableName1a1} VALUES (4,0)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName1a2} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName1a2} VALUES (0,0)"""
    sql """ INSERT INTO ${tableName1a2} VALUES (1,0)"""
    sql """ INSERT INTO ${tableName1a2} VALUES (2,0)"""
    sql """ INSERT INTO ${tableName1a2} VALUES (3,0)"""
    sql """ INSERT INTO ${tableName1a2} VALUES (4,0)"""

    // ---------- TEST 1-b ----------
    def tableName1b1 = "test_cumu_compaction_delay_fault_injection_1b1"
    def tableName1b2 = "test_cumu_compaction_delay_fault_injection_1b2"
    sql """ DROP TABLE IF EXISTS ${tableName1b1} force"""
    sql """ DROP TABLE IF EXISTS ${tableName1b2} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName1b1} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName1b1} VALUES (0,0)"""
    sql """ INSERT INTO ${tableName1b1} VALUES (1,0)"""
    sql """ INSERT INTO ${tableName1b1} VALUES (2,0)"""
    sql """ INSERT INTO ${tableName1b1} VALUES (3,0)"""
    sql """ INSERT INTO ${tableName1b1} VALUES (4,0)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName1b2} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName1b2} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName1b2} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName1b2} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName1b2} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName1b2} VALUES (4,0),(4,1),(4,2)"""

    // ---------- TEST 1-c ----------
    def tableName1c1 = "test_cumu_compaction_delay_fault_injection_1c1"
    def tableName1c2 = "test_cumu_compaction_delay_fault_injection_1c2"
    sql """ DROP TABLE IF EXISTS ${tableName1c1} force"""
    sql """ DROP TABLE IF EXISTS ${tableName1c2} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName1c1} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName1c1} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName1c1} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName1c1} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName1c1} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName1c1} VALUES (4,0),(4,1),(4,2)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName1c2} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName1c2} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName1c2} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName1c2} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName1c2} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName1c2} VALUES (4,0),(4,1),(4,2)"""

    GetDebugPoint().clearDebugPointsForAllBEs()
    def exception = false;
    try {
        GetDebugPoint().enableDebugPointForAllBEs("StorageEngine._submit_compaction_task.sleep")
        GetDebugPoint().enableDebugPointForAllBEs("CloudStorageEngine._submit_cumulative_compaction_task.sleep")
        GetDebugPoint().enableDebugPointForAllBEs("CompactionAction._handle_run_compaction.submit_cumu_task")
        GetDebugPoint().enableDebugPointForAllBEs("ThreadPool.set_max_threads.force_set")
        set_be_param("max_cumu_compaction_threads", "2");
        Thread.sleep(10000)
        set_be_param("disable_auto_compaction", "true");
        set_be_param("large_cumu_compaction_task_row_num_threshold", "10");

        Thread.sleep(10000)
        trigger_cumu(tableName1a1)
        trigger_cumu(tableName1a2)
        Thread.sleep(10000)
        assert_cumu_success(tableName1a1)
        assert_cumu_success(tableName1a2)

        Thread.sleep(10000)
        trigger_cumu(tableName1b1)
        trigger_cumu(tableName1b2)
        Thread.sleep(10000)
        assert_cumu_success(tableName1b1)
        assert_cumu_success(tableName1b2)

        Thread.sleep(10000)
        trigger_cumu(tableName1c1)
        trigger_cumu(tableName1c2)
        Thread.sleep(10000)
        assert_cumu_success(tableName1c1)
        assert_cumu_success(tableName1c2)
    } catch (Exception e) {
        logger.info(e.getMessage())
        exception = true;
    } finally {
        set_be_param("disable_auto_compaction", "false");
        set_be_param("max_cumu_compaction_threads", "-1");
        set_be_param("large_cumu_compaction_task_row_num_threshold", "1000000");
        GetDebugPoint().disableDebugPointForAllBEs("StorageEngine._submit_compaction_task.sleep")
        GetDebugPoint().disableDebugPointForAllBEs("CloudStorageEngine._submit_cumulative_compaction_task.sleep")
        GetDebugPoint().disableDebugPointForAllBEs("CompactionAction._handle_run_compaction.submit_cumu_task")
        GetDebugPoint().disableDebugPointForAllBEs("ThreadPool.set_max_threads.force_set")
        assertFalse(exception)
    }

    // ---------- PART 2 ----------
    // ---------- TEST 2-a ----------
    def tableName2a1 = "test_cumu_compaction_delay_fault_injection_2a1"
    def tableName2a2 = "test_cumu_compaction_delay_fault_injection_2a2"
    def tableName2a3 = "test_cumu_compaction_delay_fault_injection_2a3"
    sql """ DROP TABLE IF EXISTS ${tableName2a1} force"""
    sql """ DROP TABLE IF EXISTS ${tableName2a2} force"""
    sql """ DROP TABLE IF EXISTS ${tableName2a3} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2a1} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2a1} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName2a1} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName2a1} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName2a1} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName2a1} VALUES (4,0),(4,1),(4,2)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2a2} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2a2} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName2a2} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName2a2} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName2a2} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName2a2} VALUES (4,0),(4,1),(4,2)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2a3} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2a3} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName2a3} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName2a3} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName2a3} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName2a3} VALUES (4,0),(4,1),(4,2)"""

    // ---------- TEST 2-b ----------
    def tableName2b1 = "test_cumu_compaction_delay_fault_injection_2b1"
    def tableName2b2 = "test_cumu_compaction_delay_fault_injection_2b2"
    def tableName2b3 = "test_cumu_compaction_delay_fault_injection_2b3"
    sql """ DROP TABLE IF EXISTS ${tableName2b1} force"""
    sql """ DROP TABLE IF EXISTS ${tableName2b2} force"""
    sql """ DROP TABLE IF EXISTS ${tableName2b3} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2b1} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2b1} VALUES (0,0)"""
    sql """ INSERT INTO ${tableName2b1} VALUES (1,0)"""
    sql """ INSERT INTO ${tableName2b1} VALUES (2,0)"""
    sql """ INSERT INTO ${tableName2b1} VALUES (3,0)"""
    sql """ INSERT INTO ${tableName2b1} VALUES (4,0)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2b2} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2b2} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName2b2} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName2b2} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName2b2} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName2b2} VALUES (4,0),(4,1),(4,2)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2b3} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2b3} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName2b3} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName2b3} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName2b3} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName2b3} VALUES (4,0),(4,1),(4,2)"""

    // ---------- TEST 2-c ----------
    def tableName2c1 = "test_cumu_compaction_delay_fault_injection_2c1"
    def tableName2c2 = "test_cumu_compaction_delay_fault_injection_2c2"
    def tableName2c3 = "test_cumu_compaction_delay_fault_injection_2c3"
    sql """ DROP TABLE IF EXISTS ${tableName2c1} force"""
    sql """ DROP TABLE IF EXISTS ${tableName2c2} force"""
    sql """ DROP TABLE IF EXISTS ${tableName2c3} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2c1} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2c1} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName2c1} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName2c1} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName2c1} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName2c1} VALUES (4,0),(4,1),(4,2)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2c2} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2c2} VALUES (0,0),(0,1),(0,2)"""
    sql """ INSERT INTO ${tableName2c2} VALUES (1,0),(1,1),(1,2)"""
    sql """ INSERT INTO ${tableName2c2} VALUES (2,0),(2,1),(2,2)"""
    sql """ INSERT INTO ${tableName2c2} VALUES (3,0),(3,1),(3,2)"""
    sql """ INSERT INTO ${tableName2c2} VALUES (4,0),(4,1),(4,2)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2c3} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2c3} VALUES (0,0)"""
    sql """ INSERT INTO ${tableName2c3} VALUES (1,0)"""
    sql """ INSERT INTO ${tableName2c3} VALUES (2,0)"""
    sql """ INSERT INTO ${tableName2c3} VALUES (3,0)"""
    sql """ INSERT INTO ${tableName2c3} VALUES (4,0)"""

    // ---------- TEST 2-d ----------
    def tableName2d1 = "test_cumu_compaction_delay_fault_injection_2d1"
    def tableName2d2 = "test_cumu_compaction_delay_fault_injection_2d2"
    def tableName2d3 = "test_cumu_compaction_delay_fault_injection_2d3"
    sql """ DROP TABLE IF EXISTS ${tableName2d1} force"""
    sql """ DROP TABLE IF EXISTS ${tableName2d2} force"""
    sql """ DROP TABLE IF EXISTS ${tableName2d3} force"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2d1} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2d1} VALUES (0,0)"""
    sql """ INSERT INTO ${tableName2d1} VALUES (1,0)"""
    sql """ INSERT INTO ${tableName2d1} VALUES (2,0)"""
    sql """ INSERT INTO ${tableName2d1} VALUES (3,0)"""
    sql """ INSERT INTO ${tableName2d1} VALUES (4,0)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2d2} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2d2} VALUES (0,0)"""
    sql """ INSERT INTO ${tableName2d2} VALUES (1,0)"""
    sql """ INSERT INTO ${tableName2d2} VALUES (2,0)"""
    sql """ INSERT INTO ${tableName2d2} VALUES (3,0)"""
    sql """ INSERT INTO ${tableName2d2} VALUES (4,0)"""

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName2d3} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k)
        BUCKETS 1
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """
    sql """ INSERT INTO ${tableName2d3} VALUES (0,0)"""
    sql """ INSERT INTO ${tableName2d3} VALUES (1,0)"""
    sql """ INSERT INTO ${tableName2d3} VALUES (2,0)"""
    sql """ INSERT INTO ${tableName2d3} VALUES (3,0)"""
    sql """ INSERT INTO ${tableName2d3} VALUES (4,0)"""

    GetDebugPoint().clearDebugPointsForAllBEs()
    exception = false;
    try {
        GetDebugPoint().enableDebugPointForAllBEs("StorageEngine._submit_compaction_task.sleep")
        GetDebugPoint().enableDebugPointForAllBEs("CloudStorageEngine._submit_cumulative_compaction_task.sleep")
        GetDebugPoint().enableDebugPointForAllBEs("CompactionAction._handle_run_compaction.submit_cumu_task")
        GetDebugPoint().enableDebugPointForAllBEs("ThreadPool.set_max_threads.force_set")
        set_be_param("max_cumu_compaction_threads", "3");
        Thread.sleep(10000)
        set_be_param("disable_auto_compaction", "true");
        set_be_param("large_cumu_compaction_task_row_num_threshold", "10");

        Thread.sleep(10000)
        trigger_cumu(tableName2a1)
        trigger_cumu(tableName2a2)
        trigger_cumu(tableName2a3)
        Thread.sleep(10000)
        assert_cumu_success(tableName2a1)
        assert_cumu_success(tableName2a2)
        assert_cumu_fail(tableName2a3)

        Thread.sleep(10000)
        trigger_cumu(tableName2b1)
        trigger_cumu(tableName2b2)
        trigger_cumu(tableName2b3)
        Thread.sleep(10000)
        assert_cumu_success(tableName2b1)
        assert_cumu_success(tableName2b2)
        assert_cumu_success(tableName2b3)

        Thread.sleep(10000)
        trigger_cumu(tableName2c1)
        trigger_cumu(tableName2c2)
        trigger_cumu(tableName2c3)
        Thread.sleep(10000)
        assert_cumu_success(tableName2c1)
        assert_cumu_success(tableName2c2)
        assert_cumu_success(tableName2c3)

        Thread.sleep(10000)
        trigger_cumu(tableName2d1)
        trigger_cumu(tableName2d2)
        trigger_cumu(tableName2d3)
        Thread.sleep(10000)
        assert_cumu_success(tableName2d1)
        assert_cumu_success(tableName2d2)
        assert_cumu_success(tableName2d3)
    } catch (Exception e) {
        logger.info(e.getMessage())
        exception = true;
    } finally {
        set_be_param("disable_auto_compaction", "false");
        set_be_param("max_cumu_compaction_threads", "-1");
        set_be_param("large_cumu_compaction_task_row_num_threshold", "1000000");
        GetDebugPoint().disableDebugPointForAllBEs("StorageEngine._submit_compaction_task.sleep")
        GetDebugPoint().disableDebugPointForAllBEs("CloudStorageEngine._submit_cumulative_compaction_task.sleep")
        GetDebugPoint().disableDebugPointForAllBEs("CompactionAction._handle_run_compaction.submit_cumu_task")
        GetDebugPoint().disableDebugPointForAllBEs("ThreadPool.set_max_threads.force_set")
        assertFalse(exception)
    }
}
