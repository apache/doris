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

import java.util.concurrent.atomic.AtomicReference

suite("test_cloud_row_binlog_publish_conflict", "nonConcurrent") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP TABLE IF EXISTS test_mow_publish_conflict_with_binlog FORCE"

    sql """
        CREATE TABLE test_mow_publish_conflict_with_binlog (
            k1 INT,
            k2 INT,
            k3 INT,
            v1 INT,
            v2 STRING
        )
        UNIQUE KEY(k1, k2, k3)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """ALTER TABLE test_mow_publish_conflict_with_binlog
             ENABLE FEATURE "SEQUENCE_LOAD"
             WITH PROPERTIES ("function_column.sequence_type" = "int")"""

    sql """
        INSERT INTO test_mow_publish_conflict_with_binlog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__)
        VALUES
            (1, 1, 1, 10, '10', 1),
            (2, 2, 2, 20, '20', 1),
            (3, 3, 3, 30, '30', 1)
    """
    sql "sync"

    def block_publish = { token, passToken ->
        GetDebugPoint().enableDebugPointForAllFEs(
                "CloudGlobalTransactionMgr.tryCommitLock.enable_spin_wait",
                [token: token, pass_token: passToken])
    }

    def pass_publish = { token ->
        GetDebugPoint().enableDebugPointForAllFEs(
                "CloudGlobalTransactionMgr.tryCommitLock.enable_spin_wait",
                [token: token, pass_token: token])
    }

    AtomicReference<Throwable> txn2Error = new AtomicReference<>()
    AtomicReference<Throwable> txn4Error = new AtomicReference<>()

    try {
        GetDebugPoint().clearDebugPointsForAllFEs()
        block_publish("txn2", "txn1")

        def txn2 = Thread.start {
            try {
                sql "SET enable_unique_key_partial_update = true"
                sql """
                    INSERT INTO test_mow_publish_conflict_with_binlog(k1, k2, k3, v1, __DORIS_SEQUENCE_COL__)
                    VALUES (2, 2, 2, 2200, 3)
                """
            } catch (Throwable t) {
                txn2Error.set(t)
            }
        }

        // Wait for txn2 to reach the Cloud commit-lock debug point.
        Thread.sleep(10000)
        pass_publish("txn1")

        // Commit two partial-update transactions while Cloud commit is blocked.
        sql "SET enable_unique_key_partial_update = true"
        sql """
            INSERT INTO test_mow_publish_conflict_with_binlog(k1, k2, k3, v2, __DORIS_SEQUENCE_COL__)
            VALUES
                (2, 2, 2, '200', 2),
                (3, 3, 3, '300', 2)
        """

        block_publish("txn1", "txn2")
        txn2.join()
        if (txn2Error.get() != null) {
            throw txn2Error.get()
        }

        qt_row_binlog_publish_conflict """
            SELECT __DORIS_BINLOG_OP__ AS op,
                   k1,
                   k2,
                   k3,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_mow_publish_conflict_with_binlog")
            WHERE k1 IN (1, 2, 3)
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        sql "SET skip_delete_bitmap = true"
        qt_row_binlog_publish_conflict_skip_bitmap """
            SELECT __DORIS_BINLOG_OP__ AS op,
                   k1,
                   k2,
                   k3,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_mow_publish_conflict_with_binlog")
            WHERE k1 IN (1, 2, 3)
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        // Reset session variable before testing upsert path.
        sql "SET skip_delete_bitmap = false"
        sql "SET enable_unique_key_partial_update = false"
        block_publish("txn4", "txn3")

        def txn4 = Thread.start {
            try {
                sql """
                    INSERT INTO test_mow_publish_conflict_with_binlog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__)
                    VALUES (2, 2, 2, 2222, '2222', 5)
                """
            } catch (Throwable t) {
                txn4Error.set(t)
            }
        }

        // Wait for txn4 to reach the Cloud commit-lock debug point.
        Thread.sleep(10000)
        pass_publish("txn3")

        // Upsert (full columns) goes through a different write/publish path than partial-update.
        sql """
            INSERT INTO test_mow_publish_conflict_with_binlog(k1, k2, k3, v1, v2, __DORIS_SEQUENCE_COL__)
            VALUES
                (2, 2, 2, 2000, '2000', 4),
                (3, 3, 3, 3000, '3000', 4)
        """

        block_publish("txn3", "txn4")
        txn4.join()
        if (txn4Error.get() != null) {
            throw txn4Error.get()
        }

        qt_row_binlog_publish_conflict_upsert """
            SELECT __DORIS_BINLOG_OP__ AS op,
                   k1,
                   k2,
                   k3,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_mow_publish_conflict_with_binlog")
            WHERE k1 IN (2, 3)
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        sql "SET skip_delete_bitmap = true"
        qt_row_binlog_publish_conflict_upsert_skip_bitmap """
            SELECT __DORIS_BINLOG_OP__ AS op,
                   k1,
                   k2,
                   k3,
                   v1,
                   v2,
                   __BEFORE__v1__,
                   __BEFORE__v2__
            FROM binlog("table" = "test_mow_publish_conflict_with_binlog")
            WHERE k1 IN (2, 3)
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """
    } finally {
        sql "SET skip_delete_bitmap = false"
        sql "SET enable_unique_key_partial_update = false"
        GetDebugPoint().clearDebugPointsForAllFEs()
    }
}
