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
import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite("test_mow_async_publish_local_close_retry", "docker") {
    def options = new ClusterOptions()
    options.cloudMode = false
    options.enableDebugPoints()
    options.beNum = 3
    // This test must recover using the local rowset, not by copying a healthy replica.
    options.feConfigs += ["disable_tablet_scheduler=true"]
    options.beConfigs += ["enable_auto_clone_on_mow_publish_missing_version=false"]
    docker(options) {
        def backends = sql_return_maparray("SHOW BACKENDS")
        def slow = backends[0]
        def healthy = backends[1]
        def closeWait = "TxnManager.commit_txn.wait"
        def asyncFail = "AsyncTabletPublishTask.handle.fail"
        try {
            sql "DROP TABLE IF EXISTS test_mow_async_publish_local_close_retry FORCE"
            sql """
                CREATE TABLE test_mow_async_publish_local_close_retry (
                    k INT NOT NULL,
                    v INT NOT NULL
                ) UNIQUE KEY(k)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                PROPERTIES (
                    "replication_num"="3",
                    "enable_unique_key_merge_on_write"="true",
                    "disable_auto_compaction"="true"
                )
            """
            DebugPoint.enableDebugPoint(slow.Host, slow.HttpPort.toInteger(), NodeType.BE,
                                        closeWait, [duration:"10000"])
            DebugPoint.enableDebugPoint(slow.Host, slow.HttpPort.toInteger(), NodeType.BE,
                                        asyncFail)
            // Two healthy replicas can complete while the third is still closing locally.
            sql "INSERT INTO test_mow_async_publish_local_close_retry VALUES (1, 10), (2, 20)"
            def expected = sql "SELECT * FROM test_mow_async_publish_local_close_retry ORDER BY k"
            def tablets = sql_return_maparray("SHOW TABLETS FROM test_mow_async_publish_local_close_retry")
            def tabletId = tablets[0].TabletId
            def statusText = { be ->
                def (code, out, err) = be_show_tablet_status(be.Host, be.HttpPort, tabletId)
                assertEquals(0, code)
                return out
            }
            // Let local close finish while async execution is deliberately failing.
            Thread.sleep(12000)
            DebugPoint.disableDebugPoint(slow.Host, slow.HttpPort.toInteger(), NodeType.BE, closeWait)
            assertTrue(statusText(healthy).contains("[2-2]"))
            assertFalse(statusText(slow).contains("[2-2]"))

            // Do not issue another write: it could create an unrelated re-publish trigger.
            DebugPoint.disableDebugPoint(slow.Host, slow.HttpPort.toInteger(), NodeType.BE, asyncFail)
            awaitUntil(60) {
                statusText(slow).contains("[2-2]")
            }
            assertEquals(expected, sql("SELECT * FROM test_mow_async_publish_local_close_retry ORDER BY k"))
        } finally {
            DebugPoint.disableDebugPoint(slow.Host, slow.HttpPort.toInteger(), NodeType.BE, closeWait)
            DebugPoint.disableDebugPoint(slow.Host, slow.HttpPort.toInteger(), NodeType.BE, asyncFail)
        }
    }
}
