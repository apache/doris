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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases
// and modified by Doris.

import org.codehaus.groovy.runtime.IOGroovyMethods

suite("test_schema_change_and_compaction", "nonConcurrent") {
    def tableName = "test_schema_change_and_compaction"

    def getAlterTableState = { job_state ->
        def retry = 0
        def last_state = ""
        while (true) {
            sleep(2000)
            def state = sql " show alter table column where tablename = '${tableName}' order by CreateTime desc limit 1"
            logger.info("alter table state: ${state}")
            last_state = state[0][9]
            if (state.size() > 0 && state[0][9] == job_state) {
                return
            }
            retry++
            if (retry >= 10) {
                break
            }
        }
        assertTrue(false, "alter table job state is ${last_state}, not ${job_state} after retry ${retry} times")
    }

    def block_convert_historical_rowsets = {
        if (isCloudMode()) {
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().enableDebugPointForAllBEs("SchemaChangeJob::_convert_historical_rowsets.block")
        }
    }

    def unblock = {
        if (isCloudMode()) {
            GetDebugPoint().disableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.block")
        } else {
            GetDebugPoint().disableDebugPointForAllBEs("SchemaChangeJob::_convert_historical_rowsets.block")
        }
    }

    onFinish {
        unblock()
    }

    sql """ DROP TABLE IF EXISTS ${tableName} force """
    sql """
        CREATE TABLE ${tableName} ( `k1` int(11), `k2` int(11), `v1` int(11), `v2` int(11) ) ENGINE=OLAP
        unique KEY(`k1`, `k2`) cluster by(v1) DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES ( "replication_num" = "1" );
    """
    sql """ insert into ${tableName} values(10, 20, 30, 40); """

    // alter table
    block_convert_historical_rowsets()
    sql """ alter table ${tableName} order by(k1, k2, v2, v1); """
    getAlterTableState("RUNNING")

    def tablets = sql_return_maparray """ show tablets from ${tableName}; """
    logger.info("tablets: ${tablets}")
    assertEquals(2, tablets.size())
    String alterTabletId = ""
    String alterTabletBackendId = ""
    String alterTabletCompactionUrl = ""
    for (Map<String, String> tablet : tablets) {
        if (tablet["State"] == "ALTER") {
            alterTabletId = tablet["TabletId"].toLong()
            alterTabletBackendId = tablet["BackendId"]
            alterTabletCompactionUrl = tablet["CompactionStatus"]
        }
    }
    logger.info("alterTabletId: ${alterTabletId}, alterTabletBackendId: ${alterTabletBackendId}, alterTabletCompactionUrl: ${alterTabletCompactionUrl}")
    assertTrue(!alterTabletId.isEmpty())

    // write some data
    sql """ insert into ${tableName} values(10, 20, 31, 40); """
    sql """ insert into ${tableName} values(10, 20, 32, 40); """
    sql """ insert into ${tableName} values(10, 20, 33, 40); """
    sql """ insert into ${tableName} values(10, 20, 34, 40); """
    sql """ insert into ${tableName} values(10, 20, 35, 40); """
    order_qt_select1 """ select * from ${tableName}; """

    // trigger compaction
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort)
    logger.info("ip: " + backendId_to_backendIP.get(alterTabletBackendId) + ", port: " + backendId_to_backendHttpPort.get(alterTabletBackendId))
    def (code, out, err) = be_run_cumulative_compaction(backendId_to_backendIP.get(alterTabletBackendId), backendId_to_backendHttpPort.get(alterTabletBackendId), alterTabletId+"")
    logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)

    // wait for compaction done
    def enable_new_tablet_do_compaction = get_be_param.call("enable_new_tablet_do_compaction")
    logger.info("enable_new_tablet_do_compaction: " + enable_new_tablet_do_compaction)
    boolean enable = enable_new_tablet_do_compaction.get(alterTabletBackendId).toBoolean()
    logger.info("enable: " + enable)
    for (int i = 0; i < 10; i++) {
        (code, out, err) = curl("GET", alterTabletCompactionUrl)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        if (isCloudMode()) {
            if (enable) {
                if(tabletJson.rowsets.size() < 5) {
                    break
                }
            } else {
                // "msg": "invalid tablet state. tablet_id="
                break
            }
        } else {
            if(tabletJson.rowsets.size() < 5) {
                break
            }
        }
        sleep(2000)
    }

    // unblock
    unblock()
    sql """ insert into ${tableName}(k1, k2, v1, v2) values(10, 20, 36, 40), (11, 20, 36, 40); """
    sql """ insert into ${tableName}(k1, k2, v1, v2) values(10, 20, 37, 40), (11, 20, 37, 40); """
    getAlterTableState("FINISHED")
    order_qt_select2 """ select * from ${tableName}; """
}
