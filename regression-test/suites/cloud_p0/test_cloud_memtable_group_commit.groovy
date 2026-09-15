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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.suite.client.BackendClientImpl
import org.apache.doris.thrift.TGetRealtimeExecStatusRequest
import org.apache.doris.thrift.TNetworkAddress
import org.apache.doris.thrift.TStatusCode
import org.apache.doris.thrift.TUniqueId

suite("test_cloud_memtable_group_commit", "p0, docker") {
    def options = new ClusterOptions()
    options.cloudMode = true
    options.setFeNum(1)
    options.setBeNum(3)
    options.enableDebugPoints()
    options.feConfigs += ['stream_load_default_memtable_on_sink_node=true',
                          'cloud_stream_load_default_memtable_sink_upload=true']
    docker(options) {
        sql "DROP TABLE IF EXISTS cloud_memtable_group_commit"
        sql """
            CREATE TABLE cloud_memtable_group_commit (k BIGINT NOT NULL, v BIGINT)
            UNIQUE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
            PROPERTIES ("replication_num"="1", "enable_unique_key_merge_on_write"="true",
                        "group_commit_interval_ms"="200", "disable_auto_compaction"="true")
        """
        sql "INSERT INTO cloud_memtable_group_commit VALUES (1,10),(2,20)"
        // Internal Group Commit plans use a new session, not this SQL connection's variables.
        sql "SET GLOBAL enable_profile=true"
        sql "SET GLOBAL profile_level=2"
        def tablet = sql_return_maparray("SHOW TABLETS FROM cloud_memtable_group_commit")[0]
        def backends = sql_return_maparray("SHOW BACKENDS")
        def block = "VTabletWriterV2.close.profile_ready"
        try {
            GetDebugPoint().enableDebugPointForAllBEs(block, [timeout: "120"])
            def label = ""
            streamLoad {
                table "cloud_memtable_group_commit"
                set "column_separator", ","
                set "group_commit", "async_mode"
                // The internal load must use the FE defaults rather than this outer request's flag.
                set "memtable_on_sink_node", "false"
                set "cloud_memtable_sink_upload", "false"
                unset "label"
                inputText "1,100\n3,30\n"
                check { result, exception, startTime, endTime ->
                    if (exception != null) {
                        throw exception
                    }
                    logger.info("Cloud memtable Group Commit response: {}", result)
                    def json = parseJson(result)
                    label = json.Label
                    order_qt_response """
                        SELECT '${json.Status}', ${json.GroupCommit == true}, '${json.GroupCommitMode}',
                            ${label.startsWith('group_commit_')}, ${json.NumberTotalRows},
                            ${json.NumberLoadedRows}, ${json.NumberFilteredRows}
                    """
                }
            }
            def idParts = label.substring('group_commit_'.length()).split('_')
            def queryId = new TUniqueId(Long.parseUnsignedLong(idParts[0], 16),
                    Long.parseUnsignedLong(idParts[1], 16))
            def request = new TGetRealtimeExecStatusRequest().setId(queryId).setReqType("profile")
            // Probe the BEs for this internal query; its writer remains alive at the debug point.
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
                        def profile = response.reportExecStatusParams.queryProfile
                        def nodes = profile.fragmentIdToProfile.values().flatten()
                                .collectMany { it.profile.nodes }
                        return nodes.any { it.name == "DeltaWriterV2 ${tablet.TabletId}" } &&
                                nodes.any {
                                    it.infoStrings['CloudMemtableSinkUpload'] == 'true' &&
                                            it.infoStrings['CloudMemtableMowBitmap'] == 'true'
                                }
                    } finally {
                        backend.close()
                    }
                }
            }
            order_qt_pending_rows "SELECT * FROM cloud_memtable_group_commit"
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs(block)
        }
        // Publication can only finish after releasing the writer.
        awaitUntil(60) {
            (sql "SELECT COUNT(*) FROM cloud_memtable_group_commit")[0][0] == 3
        }
        order_qt_rows "SELECT * FROM cloud_memtable_group_commit"
    }
}
