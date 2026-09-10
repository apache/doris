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

import org.apache.doris.regression.util.Http

import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicReference

suite("test_committed_tso", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }
    sql "DROP TABLE IF EXISTS test_committed_tso"
    sql """
        CREATE TABLE test_committed_tso (id INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1", "binlog.enable" = "true", "binlog.format" = "ROW")
    """
    sql "INSERT INTO test_committed_tso VALUES (1), (2)"
    def currentPhysicalTime = {
        (sql "SELECT CURRENT_TSO_PHYSICAL_TIME FROM information_schema.tso_status")[0][0] as long
    }
    def committedPhysicalTime = {
        def value = (sql "SELECT COMMITTED_TSO_PHYSICAL_TIME FROM information_schema.tso_status")[0][0]
        value == null ? 0L : value as long
    }
    def formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.systemDefault())
    def timestamp = { long millis -> formatter.format(Instant.ofEpochMilli(millis)) }
    long historicalEnd = (Math.floorDiv(currentPhysicalTime(), 1000L) + 1L) * 1000L
    awaitUntil(60, 0.1) { committedPhysicalTime() >= historicalEnd }
    order_qt_readable_prefix """
        SELECT COMMITTED_TSO > 0,
               bit_shift_right(COMMITTED_TSO, 18) = COMMITTED_TSO_PHYSICAL_TIME,
               COMMITTED_TSO <= CURRENT_TSO
        FROM information_schema.tso_status
    """
    order_qt_historical_window """
        SELECT id FROM test_committed_tso@incr('endTimestamp' = '${timestamp(historicalEnd)}') ORDER BY id
    """

    def masterHttpAddress = "${getMasterIp()}:${getMasterPort()}"
    def pendingMetric = { String name ->
        String metrics = Http.GET("http://${masterHttpAddress}/metrics", false, false)
        def matcher = metrics =~ /(?m)^doris_fe_${name}(?:\{[^\n]*\})?\s+(\d+)$/
        if (!matcher.find()) {
            throw new IllegalStateException("Missing TSO metric: ${name}")
        }
        Long.parseLong(matcher.group(1))
    }
    AtomicReference<Throwable> loadError = new AtomicReference<>()
    Thread loadThread
    try {
        GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.commitTxn.blockAfterTso")
        loadThread = Thread.start {
            try {
                sql "INSERT INTO test_committed_tso VALUES (3)"
            } catch (Throwable t) {
                loadError.set(t)
            }
        }
        // Observe registration, rather than guessing when the submit thread reaches the debug point.
        awaitUntil(60, 0.1) { pendingMetric("tso_pending_transactions") > 0 }
        long pendingTso = pendingMetric("tso_oldest_pending_tso")
        long blockedEnd = (Math.floorDiv(pendingTso >> 18, 1000L) + 1L) * 1000L
        awaitUntil(60, 0.1) { currentPhysicalTime() >= blockedEnd }
        order_qt_pending_constrains_prefix """
            SELECT COMMITTED_TSO < ${pendingTso} FROM information_schema.tso_status
        """
        order_qt_history_with_later_write """
            SELECT id FROM test_committed_tso@incr('endTimestamp' = '${timestamp(historicalEnd)}') ORDER BY id
        """
        test {
            sql "SELECT id FROM test_committed_tso@incr('endTimestamp' = '${timestamp(blockedEnd)}')"
            exception "ERR_INCR_WINDOW_NOT_READY"
        }
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.commitTxn.blockAfterTso")
        loadThread.join(60000)
        if (loadThread.isAlive()) {
            throw new IllegalStateException("Commit did not complete after releasing the debug point")
        }
        if (loadError.get() != null) {
            throw loadError.get()
        }
        awaitUntil(60, 0.1) { committedPhysicalTime() >= blockedEnd }
        // Retry exactly the same window that was refused; it must include the pending write.
        order_qt_retry_original_window """
            SELECT id FROM test_committed_tso@incr('endTimestamp' = '${timestamp(blockedEnd)}') ORDER BY id
        """
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.commitTxn.blockAfterTso")
        if (loadThread != null) {
            loadThread.join(60000)
        }
    }
}
