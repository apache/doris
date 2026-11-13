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

import java.text.SimpleDateFormat

// A shared-nothing @incr read drains earlier transactions on its tables. A load that stays open
// past change_visible_timeout_ms must fail the read with ERR_INCR_VISIBLE_WAIT_TIMEOUT (5101),
// the code clients retry on, and the same window must be readable once the load finishes.
suite("test_changelog_incr_visible_wait_timeout") {
    if (isCloudMode()) {
        return
    }
    sql "DROP TABLE IF EXISTS test_changelog_incr_visible_wait_timeout"
    sql """
        CREATE TABLE test_changelog_incr_visible_wait_timeout (id INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "changelog.enable" = "true")
    """
    sql "INSERT INTO test_changelog_incr_visible_wait_timeout VALUES (1), (2)"

    def incrTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    long current = (sql "SELECT CURRENT_TSO_PHYSICAL_TIME FROM information_schema.tso_status")[0][0] as long
    long windowEnd = (Math.floorDiv(current, 1000L) + 1L) * 1000L
    awaitUntil(60, 0.1) {
        ((sql "SELECT CURRENT_TSO_PHYSICAL_TIME FROM information_schema.tso_status")[0][0] as long) >= windowEnd
    }
    String windowEndText = incrTimeFormat.format(new Date(windowEnd))
    String windowSql = """
        SELECT id FROM test_changelog_incr_visible_wait_timeout@incr('endTimestamp' = '${windowEndText}')
        ORDER BY id
    """

    def finishTxn = { long id, String operation ->
        streamLoad {
            table "test_changelog_incr_visible_wait_timeout"
            set 'txn_id', id.toString()
            set 'txn_operation', operation
            inputText ""
            twoPhaseCommit true
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                assertEquals("success", parseJson(result).status.toLowerCase())
            }
        }
    }

    long txnId = 0
    boolean finished = false
    try {
        // Leave a load on the table open in PRECOMMITTED, so the read's transaction drain cannot finish.
        streamLoad {
            table "test_changelog_incr_visible_wait_timeout"
            set 'column_separator', ','
            set 'two_phase_commit', 'true'
            inputText "3\n"
            time 10000
            check { result, exception, startTime, endTime ->
                if (exception != null) {
                    throw exception
                }
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                txnId = json.TxnId as long
            }
        }

        sql "SET change_visible_timeout_ms = 1000"
        test {
            sql windowSql
            exception "ERR_INCR_VISIBLE_WAIT_TIMEOUT"
        }

        finishTxn(txnId, "commit")
        finished = true
        awaitUntil(60, 0.5) {
            def txn = sql_return_maparray "SHOW TRANSACTION WHERE ID = ${txnId}"
            txn.size() == 1 && txn[0].TransactionStatus == "VISIBLE"
        }
        // Retry exactly the same window. The committed load lands after the window end, so only
        // the rows inserted before the window end are returned.
        def rows = sql windowSql
        assertEquals([[1], [2]], rows.collect { [it[0] as int] })
    } finally {
        if (txnId > 0 && !finished) {
            try {
                finishTxn(txnId, "abort")
            } catch (Throwable t) {
                logger.warn("failed to abort txn ${txnId}", t)
            }
        }
    }
}
