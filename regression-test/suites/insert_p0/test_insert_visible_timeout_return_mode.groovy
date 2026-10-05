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

suite("test_insert_visible_timeout_return_mode", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def debugPoint = "PublishVersionDaemon.stop_publish"
    def debugPointTimeoutSeconds = "10"
    def debugPointManager = GetDebugPoint()

    // Prepare a single-replica table so publish blocking deterministically drives the visible timeout path.
    sql """ DROP TABLE IF EXISTS test_insert_visible_timeout_return_mode_tbl FORCE """
    sql """
        CREATE TABLE test_insert_visible_timeout_return_mode_tbl (
            `k1` INT,
            `k2` INT
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    def debugPointEnabled = false
    try {
        // PublishVersionDaemon only runs on the master FE. Enable the debug point on all FEs so
        // this return-mode test does not depend on which FE the pipeline uses as its entry point.
        debugPointManager.enableDebugPointForAllFEs(debugPoint,
                [timeout: debugPointTimeoutSeconds])
        debugPointEnabled = true

        sql """ SET insert_visible_timeout_ms = 1000 """

        // Verify the default committed mode returns success after the visible wait times out.
        sql """ SET insert_visible_timeout_return_mode = 'committed' """
        sql """ INSERT INTO test_insert_visible_timeout_return_mode_tbl VALUES (1, 10) """

        // The insert returned successfully in committed mode, but publish is still blocked.
        def rowCountBeforePublish = sql """ SELECT COUNT(*) FROM test_insert_visible_timeout_return_mode_tbl """
        assertEquals(0L, rowCountBeforePublish[0][0] as long)

        // Verify the error mode returns the publish-timeout error to the client while keeping the txn committed.
        sql """ SET insert_visible_timeout_return_mode = 'error' """
        test {
            sql """ INSERT INTO test_insert_visible_timeout_return_mode_tbl VALUES (2, 20) """
            exception "transaction commit successfully, BUT data did not become visible within insert_visible_timeout_ms and will be visible later."
        }
    } finally {
        if (debugPointEnabled) {
            debugPointManager.disableDebugPointForAllFEs(debugPoint)
        }
    }

    // Wait for FE publish to resume so both committed transactions become visible before checking final data.
    def visible = false
    for (int i = 0; i < 15; i++) {
        def rowCount = sql """ SELECT COUNT(*) FROM test_insert_visible_timeout_return_mode_tbl """
        if ((rowCount[0][0] as long) == 2L) {
            visible = true
            break
        }
        sleep(1000)
    }
    assertTrue(visible, "Rows should become visible after publish resumes")

    order_qt_final_select """ SELECT * FROM test_insert_visible_timeout_return_mode_tbl ORDER BY k1 """
}
