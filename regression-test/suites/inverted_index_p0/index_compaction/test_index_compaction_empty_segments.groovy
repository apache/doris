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

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_index_compaction_empty_segments", "p0") {

    def compaction_table_name = "test_index_compaction_empty_segments"
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
    def set_be_config = { key, value ->
        for (String backend_id: backendId_to_backendIP.keySet()) {
            def (code, out, err) = update_be_config(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), key, value)
            logger.info("update config: code=" + code + ", out=" + out + ", err=" + err)
        }
    }
    set_be_config.call("inverted_index_compaction_enable", "true")

    sql "DROP TABLE IF EXISTS ${compaction_table_name}"
    sql """ 
        CREATE TABLE ${compaction_table_name} (
            `k` int(11) NULL,
            `v` varchar(20) NULL,
            INDEX v_idx (`v`)USING INVERTED COMMENT ''
        ) ENGINE=OLAP
            DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "disable_auto_compaction" = "true"
        );
    """

    sql """ INSERT INTO ${compaction_table_name} VALUES (10, "andy"); """
    sql """ INSERT INTO ${compaction_table_name} VALUES (10, "tom"); """
    sql """ INSERT INTO ${compaction_table_name} VALUES (10, "jodie"); """
    sql """ INSERT INTO ${compaction_table_name} VALUES (10, "jerry"); """
    sql """ INSERT INTO ${compaction_table_name} VALUES (10, "ok"); """
    sql """ DELETE FROM ${compaction_table_name} where k = 10; """

    //TabletId,ReplicaId,BackendId,SchemaHash,Version,LstSuccessVersion,LstFailedVersion,LstFailedTime,LocalDataSize,RemoteDataSize,RowCount,State,LstConsistencyCheckTime,CheckVersion,VersionCount,QueryHits,PathHash,MetaUrl,CompactionStatus
    def tablets = sql_return_maparray """ show tablets from ${compaction_table_name}; """

    // trigger compactions for all tablets in ${tableName}
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        backend_id = tablet.BackendId
        (code, out, err) = be_run_full_compaction(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
        logger.info("Run compaction: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def compactJson = parseJson(out.trim())
        assertEquals("success", compactJson.status.toLowerCase())
    }

    // wait for all compactions done
    for (def tablet in tablets) {
        Awaitility.await().atMost(10, TimeUnit.MINUTES).untilAsserted(() -> {
            String tablet_id = tablet.TabletId
            backend_id = tablet.BackendId
            (code, out, err) = be_get_compaction_status(backendId_to_backendIP.get(backend_id), backendId_to_backendHttpPort.get(backend_id), tablet_id)
            logger.info("Get compaction status: code=" + code + ", out=" + out + ", err=" + err)
            assertEquals(code, 0)
            def compactionStatus = parseJson(out.trim())
            assertEquals("compaction task for this tablet is not running", compactionStatus.msg.toLowerCase())
            return compactionStatus.run_status;
        });
    }

    int afterSegmentCount = 0
    for (def tablet in tablets) {
        String tablet_id = tablet.TabletId
        (code, out, err) = curl("GET", tablet.CompactionStatus)
        logger.info("Show tablets status: code=" + code + ", out=" + out + ", err=" + err)
        assertEquals(code, 0)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        for (String rowset in (List<String>) tabletJson.rowsets) {
            logger.info("rowset is: " + rowset)
            afterSegmentCount += Integer.parseInt(rowset.split(" ")[1])
        }
    }
    assertEquals(afterSegmentCount, 0)
}
