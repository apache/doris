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

import org.apache.doris.regression.util.DebugPoint
import org.apache.doris.regression.util.NodeType

suite("test_insert_visible_timeout_return_mode", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    def tableName = "test_insert_visible_timeout_return_mode_tbl"
    def debugPoint = "PublishVersionDaemon.stop_publish"
    // Use the configured FE HTTP endpoint so the case also works when SHOW FRONTENDS exposes loopback addresses.
    def feHttpAddress = context.config.feHttpAddress
    def feHost = feHttpAddress.split(":")[0]
    def feHttpPort = Integer.parseInt(feHttpAddress.split(":")[1])

    // Prepare a single-replica table so publish blocking deterministically drives the visible timeout path.
    sql """ DROP TABLE IF EXISTS ${tableName} FORCE """
    sql """
        CREATE TABLE ${tableName} (
            `k1` INT,
            `k2` INT
        )
        DISTRIBUTED BY HASH(`k1`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    try {
        // Block FE publish so inserts can commit but remain non-visible until the debug point is removed.
        DebugPoint.enableDebugPoint(feHost, feHttpPort, NodeType.FE, debugPoint)

        sql """ SET insert_visible_timeout_ms = 1000 """

        // Verify the default committed mode returns success after the visible wait times out.
        sql """ SET insert_visible_timeout_return_mode = 'committed' """
        sql """ INSERT INTO ${tableName} VALUES (1, 10) """

        // Verify the error mode returns the publish-timeout error to the client while keeping the txn committed.
        sql """ SET insert_visible_timeout_return_mode = 'error' """
        test {
            sql """ INSERT INTO ${tableName} VALUES (2, 20) """
            exception "transaction commit successfully, BUT data did not become visible within insert_visible_timeout_ms and will be visible later."
        }
    } finally {
        try {
            DebugPoint.disableDebugPoint(feHost, feHttpPort, NodeType.FE, debugPoint)
        } catch (Throwable e) {
            logger.warn("Failed to disable debug point ${debugPoint}", e)
        }
    }

    // Wait for FE publish to resume so both committed transactions become visible before checking final data.
    def visible = false
    for (int i = 0; i < 15; i++) {
        def rowCount = sql """ SELECT COUNT(*) FROM ${tableName} """
        if ((rowCount[0][0] as long) == 2L) {
            visible = true
            break
        }
        sleep(1000)
    }
    assertTrue(visible, "Rows should become visible after publish resumes")

    order_qt_final_select """ SELECT * FROM ${tableName} ORDER BY k1 """
}
