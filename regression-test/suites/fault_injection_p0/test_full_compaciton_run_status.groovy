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

suite("test_full_compaction_run_status","nonConcurrent") {


    def tableName = "full_compaction_run_status_test"

    // test successful group commit async load
    sql """ DROP TABLE IF EXISTS ${tableName} """

    String backend_id;

    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    backend_id = backendId_to_backendIP.keySet()[0]

    sql """
        CREATE TABLE IF NOT EXISTS ${tableName} (
            `k` int ,
            `v` int ,
        ) engine=olap
        DISTRIBUTED BY HASH(`k`)
        BUCKETS 2
        properties(
            "replication_num" = "1",
            "disable_auto_compaction" = "true")
        """

    sql """ INSERT INTO ${tableName} VALUES (0,00)"""
    sql """ INSERT INTO ${tableName} VALUES (1,10)"""
    sql """ INSERT INTO ${tableName} VALUES (2,20)"""
    sql """ INSERT INTO ${tableName} VALUES (3,30)"""
    sql """ INSERT INTO ${tableName} VALUES (4,40)"""
    sql """ INSERT INTO ${tableName} VALUES (5,50)"""
    sql """ INSERT INTO ${tableName} VALUES (6,60)"""
    sql """ INSERT INTO ${tableName} VALUES (7,70)"""
    sql """ INSERT INTO ${tableName} VALUES (8,80)"""
    sql """ INSERT INTO ${tableName} VALUES (9,90)"""

    GetDebugPoint().clearDebugPointsForAllBEs()

    def exception = false;
    try {
        GetDebugPoint().enableDebugPointForAllBEs("FullCompaction.modify_rowsets.sleep")
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId

            def times = 1
            do{
                (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
                logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
                ++times
                sleep(1000)
            } while (parseJson(out.trim()).status.toLowerCase()!="success" && times<=10)

            (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            assertTrue(compactJson.msg.toLowerCase().contains("is running"))
        }
        Thread.sleep(30000)
        logger.info("sleep 30s to wait full compaction finish.")
        for (def tablet in tablets) {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId

            (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactJson = parseJson(out.trim())
            assertTrue(compactJson.msg.toLowerCase().contains("is not running"))
        }
    } catch (Exception e) {
        logger.info(e.getMessage())
        exception = true;
    } finally {
        GetDebugPoint().disableDebugPointForAllBEs("FullCompaction.modify_rowsets.sleep")
        assertFalse(exception)
    }
}
