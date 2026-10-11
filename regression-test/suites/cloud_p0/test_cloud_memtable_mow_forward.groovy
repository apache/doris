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

import org.apache.doris.regression.action.ProfileAction
import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.suite.client.BackendClientImpl
import org.apache.doris.thrift.TGetRealtimeExecStatusRequest
import org.apache.doris.thrift.TNetworkAddress
import org.apache.doris.thrift.TStatusCode
import org.apache.doris.thrift.TUniqueId

suite("test_cloud_memtable_mow_forward", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)
    options.enableDebugPoints()
    options.beConfigs += ['small_file_threshold_bytes=1048576',
                          'enable_merge_on_write_correctness_check=true']
    options.feConfigs += ['stream_load_default_memtable_on_sink_node=true']
    docker(options) {
        def backends = sql_return_maparray("SHOW BACKENDS")
        def awaitGroupProfile = { label, tabletId ->
            def idParts = label.substring('group_commit_'.length()).split('_')
            def queryId = new TUniqueId(Long.parseUnsignedLong(idParts[0], 16),
                    Long.parseUnsignedLong(idParts[1], 16))
            def request = new TGetRealtimeExecStatusRequest().setId(queryId).setReqType("profile")
            awaitUntil(60) {
                backends.any { be ->
                    def backend = new BackendClientImpl(
                            new TNetworkAddress(be.Host, be.BePort as int), be.HttpPort as int)
                    try {
                        backend.client.inputProtocol.transport.setTimeout(5000)
                        def response = backend.client.getRealtimeExecStatus(request)
                        if (response.status.statusCode == TStatusCode.NOT_FOUND) {
                            return false
                        }
                        if (response.status.statusCode != TStatusCode.OK) {
                            throw new IllegalStateException("Get BE profile failed: ${response.status}")
                        }
                        def nodes = response.reportExecStatusParams.queryProfile.fragmentIdToProfile
                                .values().flatten().collectMany { it.profile.nodes }
                        return nodes.any { it.name == "DeltaWriterV2 ${tabletId}" }
                    } finally {
                        backend.close()
                    }
                }
            }
        }
        def checkUniqueKeys = {
            sql """
                SELECT assert_true(COUNT(*) = 0, 'cloud_mow_forward contains duplicate keys') FROM (
                    SELECT k, COUNT(*) AS a FROM cloud_mow_forward GROUP BY k HAVING a > 1
                ) duplicates
            """
        }

        sql "DROP TABLE IF EXISTS cloud_mow_forward_source"
        sql """
            CREATE TABLE cloud_mow_forward_source (n BIGINT NOT NULL)
            DUPLICATE KEY(n) DISTRIBUTED BY HASH(n) BUCKETS 12
            PROPERTIES ("replication_num"="1")
        """
        sql "SET enable_memtable_on_sink_node=false"
        sql "INSERT INTO cloud_mow_forward_source SELECT number FROM numbers('number'='12000')"
        sql "SET parallel_pipeline_task_num=4"
        sql "SET profile_level=2"
        sql "SET enable_sql_cache=false"
        sql "SET enable_file_cache=false"
        [false, true].each { packed ->
            setBeConfigTemporary(['enable_packed_file': packed.toString()]) {
                sql "DROP TABLE IF EXISTS cloud_mow_forward"
                sql """
                    CREATE TABLE cloud_mow_forward (
                        k BIGINT NOT NULL, v BIGINT, seq BIGINT NOT NULL,
                        INDEX idx_k(k) USING INVERTED
                    ) UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
                    PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                                "function_column.sequence_col"="seq", "inverted_index_storage_format"="V2",
                                "disable_auto_compaction"="true", "group_commit_interval_ms"="200")
                """
                sql "SET enable_memtable_on_sink_node=false"
                sql "INSERT INTO cloud_mow_forward VALUES (0,-1,-1),(1,-1,-1),(2,-1,-1),(3,-1,-1)"
                sql "INSERT INTO cloud_mow_forward VALUES (0,0,0),(1,0,0),(2,0,0),(3,0,0)"
                sql "SET enable_memtable_on_sink_node=true"
                sql "SET enable_profile=true"
                sql """
                    /* cloud_mow_forward_${packed} */
                    INSERT INTO cloud_mow_forward SELECT n%4,n*10,n+1 FROM cloud_mow_forward_source
                """
                new ProfileAction(context).getProfileBySql("cloud_mow_forward_${packed}", ["DeltaWriterV2"])
                sql "SET enable_profile=false"
                quickTest("initial_${packed}", "SELECT * FROM cloud_mow_forward", true)
                checkUniqueKeys()
                quickTest("index_${packed}", "SELECT * FROM cloud_mow_forward WHERE k IN (1,3)", true)
                def tablet = sql_return_maparray("SHOW TABLETS FROM cloud_mow_forward")[0]
                def partition = sql_return_maparray("SHOW PARTITIONS FROM cloud_mow_forward")[0]
                def ms = cluster.getAllMetaservices()[0]
                getSegmentFilesFromMs("${ms.host}:${ms.httpPort}", tablet.TabletId, partition.VisibleVersion) {
                    code, body ->
                        assertEquals(200, code)
                        def meta = parseJson(body)
                        quickTest("layout_${packed}", """
                            SELECT ${meta.num_segments as int} > 1,
                                ${(meta.packed_slice_locations ?: [:]).size() > 0},
                                ${meta.segments_file_size.size() == (meta.num_segments as int)},
                                ${meta.segments_file_size.every { (it as long) > 0 }},
                                ${meta.inverted_index_file_info.size() == (meta.num_segments as int)},
                                ${meta.inverted_index_file_info.every { (it.index_size as long) > 0 }}
                        """, true)
                }
                sql "INSERT INTO cloud_mow_forward VALUES (0,-1,1)"
                sql "INSERT INTO cloud_mow_forward (k,v,seq,__DORIS_DELETE_SIGN__) VALUES (2,130000,13000,1)"
                sql "INSERT INTO cloud_mow_forward VALUES (2,-1,1)"
                quickTest("deleted_${packed}", "SELECT * FROM cloud_mow_forward", true)
                sql "INSERT INTO cloud_mow_forward VALUES (2,140000,14000)"
                streamLoad {
                    table "cloud_mow_forward"
                    set "column_separator", ","
                    set "memtable_on_sink_node", "true"
                    set "group_commit", "off_mode"
                    inputText "0,150000,15000\n3,-1,1\n"
                    check { result, exception, startTime, endTime ->
                        if (exception != null) { throw exception }
                        def response = parseJson(result)
                        quickTest("stream_status_${packed}", "SELECT '${response.Status}'", true)
                    }
                }
                quickTest("stream_${packed}", "SELECT * FROM cloud_mow_forward", true)
                def originalEnableProfile = sql("SHOW GLOBAL VARIABLES LIKE 'enable_profile'")[0][1]
                def originalProfileLevel = sql("SHOW GLOBAL VARIABLES LIKE 'profile_level'")[0][1]
                def profileBlock = "VTabletWriterV2.close.profile_ready"
                def groupLabel = ""
                try {
                    sql "SET GLOBAL enable_profile=true"
                    sql "SET GLOBAL profile_level=2"
                    GetDebugPoint().enableDebugPointForAllBEs(profileBlock, [timeout: "120"])
                    streamLoad {
                        table "cloud_mow_forward"
                        set "column_separator", ","
                        set "group_commit", "async_mode"
                        set "memtable_on_sink_node", "false"
                        unset "label"
                        inputText "1,160000,16000\n"
                        check { result, exception, startTime, endTime ->
                            if (exception != null) { throw exception }
                            def response = parseJson(result)
                            groupLabel = response.Label
                            quickTest("group_status_${packed}", "SELECT '${response.Status}', '${response.GroupCommit}'", true)
                        }
                    }
                    awaitGroupProfile(groupLabel, tablet.TabletId)
                } finally {
                    GetDebugPoint().disableDebugPointForAllBEs(profileBlock)
                    sql "SET GLOBAL enable_profile=${originalEnableProfile}"
                    sql "SET GLOBAL profile_level=${originalProfileLevel}"
                }
                awaitUntil(60) {
                    (sql "SELECT v FROM cloud_mow_forward WHERE k=1")[0][0] == 160000
                }
                quickTest("group_${packed}", "SELECT * FROM cloud_mow_forward", true)
                checkUniqueKeys()
                trigger_and_wait_compaction("cloud_mow_forward", "full")
                quickTest("compacted_${packed}", "SELECT * FROM cloud_mow_forward", true)
                checkUniqueKeys()
            }
        }
        // MOW without sink upload forwards files and calculates bitmaps on the target BE.
        sql "DROP TABLE IF EXISTS cloud_memtable_mow_fallback"
        sql """
            CREATE TABLE cloud_memtable_mow_fallback (k BIGINT NOT NULL, v BIGINT)
            UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true")
        """
        sql "INSERT INTO cloud_memtable_mow_fallback VALUES (1,10)"
        sql "INSERT INTO cloud_memtable_mow_fallback VALUES (1,20)"
        order_qt_mow_fallback "SELECT * FROM cloud_memtable_mow_fallback"

        sql "DROP TABLE IF EXISTS cloud_mow_forward_broker"
        sql """
            CREATE TABLE cloud_mow_forward_broker (k BIGINT NOT NULL, v BIGINT)
            UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                        "disable_auto_compaction"="true")
        """
        sql "INSERT INTO cloud_mow_forward_broker VALUES (50,-1)"
        def brokerLabel = "mow_forward_" + UUID.randomUUID().toString().replace('-', '_')
        sql "SET enable_profile=true"
        try {
            sql """
                LOAD LABEL ${brokerLabel} (
                    DATA INFILE("s3://${getS3BucketName()}/regression/load/data/basic_data.csv")
                    INTO TABLE cloud_mow_forward_broker COLUMNS TERMINATED BY "|" FORMAT AS "CSV"
                    (k, c01, c02, c03, c04, c05, c06, c07, c08, c09,
                        c10, c11, c12, c13, c14, c15, c16, c17, c18) SET (v=k*2)
                ) WITH S3 (
                    "AWS_ACCESS_KEY"="${getS3AK()}", "AWS_SECRET_KEY"="${getS3SK()}",
                    "AWS_ENDPOINT"="${getS3Endpoint()}", "AWS_REGION"="${getS3Region()}",
                    "provider"="${getS3Provider()}"
                ) PROPERTIES ("load_parallelism"="1")
            """
            waitForBrokerLoadDone(brokerLabel)
            def load = sql_return_maparray("SHOW LOAD WHERE LABEL = '${brokerLabel}'")[0]
            new ProfileAction(context).getProfile(load.JobId.toString(), ["DeltaWriterV2"])
        } finally {
            sql "SET enable_profile=false"
        }
        sql """
            SELECT assert_true(COUNT(*) = 0, 'cloud_mow_forward_broker contains duplicate keys') FROM (
                SELECT k, COUNT(*) AS a FROM cloud_mow_forward_broker GROUP BY k HAVING a > 1
            ) duplicates
        """
        order_qt_broker_rows "SELECT k,v FROM cloud_mow_forward_broker"

        sql "DROP TABLE IF EXISTS cloud_memtable_partial_update"
        sql """
            CREATE TABLE cloud_memtable_partial_update (k INT NOT NULL, v INT, untouched INT)
            UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true")
        """
        sql "INSERT INTO cloud_memtable_partial_update VALUES (1,10,100)"
        streamLoad {
            table "cloud_memtable_partial_update"
            set "column_separator", ","
            set "columns", "k,v"
            set "partial_columns", "true"
            set "memtable_on_sink_node", "true"
            set "group_commit", "off_mode"
            inputText "1,20\n"
            check { result, exception, startTime, endTime ->
                if (exception != null) { throw exception }
                def response = parseJson(result)
                sql "SELECT assert_true('${response.Status}' = 'Success', 'partial-update Stream Load failed')"
            }
        }
        sql """
            SELECT assert_true(COUNT(*) = 1 AND SUM(v) = 20 AND SUM(untouched) = 100, 'partial update result mismatch')
            FROM cloud_memtable_partial_update WHERE k = 1
        """
    }
}
