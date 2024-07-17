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

import org.awaitility.Awaitility;
import static java.util.concurrent.TimeUnit.SECONDS;

suite('test_delete_sign_with_cumu_compaction') {
    def table = 'test_delete_sign_with_cumu_compaction'
    
    sql """ DROP TABLE IF EXISTS ${table};"""
    sql """
    CREATE TABLE ${table}
    (
        col1 TINYINT  NOT NULL,col2 BIGINT  NOT NULL,col3 DECIMAL(36, 13)  NOT NULL,
    )
    UNIQUE KEY(`col1`,`col2`,`col3`)
    DISTRIBUTED BY HASH(`col1`,`col2`,`col3`) BUCKETS 1
    PROPERTIES (
        "enable_unique_key_merge_on_write" = "false", "disable_auto_compaction"="true",
        "replication_num" = "1"
    );
    """

    String backend_id;
    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,PathHash,MetaUrl,CompactionStatus
    def tablet = (sql """ show tablets from ${table}; """)[0]
    backend_id = tablet[2]
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    def (code, out, err) = show_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id))
    logger.info("Show config: code=" + code + ", out=" + out + ", err=" + err)
    assertEquals(code, 0)
    def configList = parseJson(out.trim())
    assert configList instanceof List

    boolean disableAutoCompaction = true
    boolean allowDeleteWhenCumu = false
    for (Object ele in (List) configList) {
        assert ele instanceof List<String>
        if (((List<String>) ele)[0] == "disable_auto_compaction") {
            disableAutoCompaction = Boolean.parseBoolean(((List<String>) ele)[2])
        }
        if (((List<String>) ele)[0] == "enable_delete_when_cumu_compaction") {
            allowDeleteWhenCumu = Boolean.parseBoolean(((List<String>) ele)[2])
        }
    }

    if (!allowDeleteWhenCumu) {
        logger.info("Skip test compaction when cumu compaction because not enabled this config")
        return
    }

    def waitForCompaction = { be_host, be_http_port ->
        // wait for all compactions done
        Awaitility.await().atMost(30, SECONDS).pollInterval(1, SECONDS).until {
            String tablet_id = tablet[0]
            StringBuilder sb = new StringBuilder();
            sb.append("curl -X GET http://${be_host}:${be_http_port}")
            sb.append("/api/compaction/run_status?tablet_id=")
            sb.append(tablet_id)

            String command = sb.toString()
            logger.info(command)
            process = command.execute()
            code = process.waitFor()
            out = process.getText()
            logger.info("Get compaction status: code=" + code + ", out=" + out)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("success", compactionStatus.status.toLowerCase())

            !compactionStatus.run_status
        }
    }

    (1..10).each { i ->
        sql """INSERT into ${table} (col1,col2,col3) values (${i}, 2, 3)"""
    }
    be_run_cumulative_compaction(backendId_to_backendIP[backend_id], backendId_to_backendHttpPort[backend_id], tablet[0]);
    waitForCompaction(backendId_to_backendIP[backend_id], backendId_to_backendHttpPort[backend_id])

    (11..12).each { i ->
        sql """INSERT into ${table} (col1,col2,col3) values (${i}, 2, 3)"""
    }
    be_run_cumulative_compaction(backendId_to_backendIP[backend_id], backendId_to_backendHttpPort[backend_id], tablet[0]);
    waitForCompaction(backendId_to_backendIP[backend_id], backendId_to_backendHttpPort[backend_id])

    be_run_base_compaction(backendId_to_backendIP[backend_id], backendId_to_backendHttpPort[backend_id], tablet[0]);
    waitForCompaction(backendId_to_backendIP[backend_id], backendId_to_backendHttpPort[backend_id])

    (1..10).each { i ->
        sql """ INSERT into ${table} (col1,col2,col3,__DORIS_DELETE_SIGN__) values (${i}, 2, 3, 1) """
    }

    be_run_cumulative_compaction(backendId_to_backendIP[backend_id], backendId_to_backendHttpPort[backend_id], tablet[0]);
    waitForCompaction(backendId_to_backendIP[backend_id], backendId_to_backendHttpPort[backend_id])

    qt_select_default """ SELECT * FROM ${table} ORDER BY col1 """
    
}
