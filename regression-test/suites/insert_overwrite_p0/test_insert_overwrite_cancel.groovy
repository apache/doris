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

/**
 * What a cancelled overwrite owes the rows it has already committed, and what it owes the caller when it has
 * committed nothing.
 *
 * <p>An overwrite commits its rows into temporary partitions and publishes them with a swap afterwards, so a
 * cancellation that lands between the two halves cannot take the rows back: they are durable, and everything
 * the write read -- the base table stream offsets among it -- was committed with them. Dropping the temporary
 * partitions there, and answering the client with a success for an overwrite that never happened, is what
 * loses those rows against an advanced offset; the swap is what has to run instead. Where the insert committed
 * nothing at all -- its plan folded to an empty relation, so it took the insert's no-transaction path -- the
 * cancellation still has everything to take back, and completing the swap there would publish an empty table
 * for a statement the client cancelled.
 *
 * <p>Each case is injected by its own debug point in the overwrite, scoped by table name, so it lands on a
 * real statement rather than on a race. The same window is asserted twice: once for the rows it publishes,
 * once for the empty plan, whose statement fails only if the injection reached the window.
 */
suite("test_insert_overwrite_cancel", "nonConcurrent") {
    def beforePoint = "InsertOverwriteTableCommand.cancelBeforeTheInsertOfAnOverwrite"
    def betweenPoint = "InsertOverwriteTableCommand.cancelBetweenTheTwoHalvesOfAnOverwrite"

    GetDebugPoint().disableDebugPointForAllFEs(beforePoint)
    GetDebugPoint().disableDebugPointForAllFEs(betweenPoint)
    sql """DROP TABLE IF EXISTS test_iot_cancel_src"""
    sql """DROP TABLE IF EXISTS test_iot_cancel_dst"""
    sql """DROP TABLE IF EXISTS test_iot_cancel_flat_src"""
    sql """DROP TABLE IF EXISTS test_iot_cancel_flat_dst"""

    sql """
        CREATE TABLE test_iot_cancel_src (
            id BIGINT NOT NULL,
            dt DATE NOT NULL,
            amount INT
        ) ENGINE = OLAP
        UNIQUE KEY(id, dt)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES [('2026-01-01'), ('2026-02-01')),
            PARTITION p2 VALUES [('2026-02-01'), ('2026-03-01'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        CREATE TABLE test_iot_cancel_dst (
            id BIGINT NOT NULL,
            dt DATE NOT NULL,
            amount INT
        ) ENGINE = OLAP
        UNIQUE KEY(id, dt)
        PARTITION BY RANGE(dt) (
            PARTITION p1 VALUES [('2026-01-01'), ('2026-02-01')),
            PARTITION p2 VALUES [('2026-02-01'), ('2026-03-01'))
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """INSERT INTO test_iot_cancel_dst VALUES (1, '2026-01-10', 100), (2, '2026-02-10', 200)"""
    sql """INSERT INTO test_iot_cancel_src VALUES (3, '2026-01-20', 300), (4, '2026-02-20', 400)"""

    // Unpartitioned, for the empty-plan case below: a partitioned target gives the plan a coordinator exchange,
    // so the sink's child is not the empty relation and the insert commits an empty transaction instead of
    // taking the path that commits nothing.
    sql """
        CREATE TABLE test_iot_cancel_flat_src (
            id BIGINT NOT NULL,
            dt DATE NOT NULL,
            amount INT
        ) ENGINE = OLAP
        UNIQUE KEY(id, dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """
        CREATE TABLE test_iot_cancel_flat_dst (
            id BIGINT NOT NULL,
            dt DATE NOT NULL,
            amount INT
        ) ENGINE = OLAP
        UNIQUE KEY(id, dt)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true"
        )
    """
    sql """INSERT INTO test_iot_cancel_flat_dst VALUES (1, '2026-01-10', 100)"""
    sql """INSERT INTO test_iot_cancel_flat_src VALUES (3, '2026-01-20', 300)"""

    // A cancellation that lands before anything was committed has nothing to take back, so the statement has
    // to report it, and the table has to be where it was.
    try {
        GetDebugPoint().enableDebugPointForAllFEs(beforePoint,
                [table_name: "test_iot_cancel_dst"])
        test {
            sql """INSERT OVERWRITE TABLE test_iot_cancel_dst SELECT * FROM test_iot_cancel_src"""
            exception "insert overwrite is cancelled before registerTask"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(beforePoint)
    }
    order_qt_dst_after_the_cancelled_statement """SELECT id, dt, amount FROM test_iot_cancel_dst"""

    // A cancellation that lands after the rows are committed cannot take them back: they are in the temporary
    // partitions, and the swap is what publishes them. The statement reports the success it now is, and the
    // rows it read are the ones the table holds.
    try {
        GetDebugPoint().enableDebugPointForAllFEs(betweenPoint,
                [table_name: "test_iot_cancel_dst"])
        sql """INSERT OVERWRITE TABLE test_iot_cancel_dst SELECT * FROM test_iot_cancel_src"""
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(betweenPoint)
    }
    order_qt_dst_after_the_cancelled_window """SELECT id, dt, amount FROM test_iot_cancel_dst"""

    // The same cancellation, but the insert committed nothing: an empty plan takes the insert's
    // no-transaction path, so the temporary partitions hold no rows and there is no offset to keep. The
    // statement has to fail and the table has to keep the rows it had -- a swap here would publish the empty
    // result for a statement the client cancelled. This case also pins that the cancellation above is really
    // delivered: its assertion holds only if the injection reached the window.
    try {
        GetDebugPoint().enableDebugPointForAllFEs(betweenPoint,
                [table_name: "test_iot_cancel_flat_dst"])
        test {
            sql """INSERT OVERWRITE TABLE test_iot_cancel_flat_dst
                   SELECT id, dt, amount FROM test_iot_cancel_flat_src WHERE 1 = 0"""
            exception "insert overwrite is cancelled after an insert that committed nothing"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(betweenPoint)
    }
    order_qt_dst_after_the_cancelled_empty_overwrite """SELECT id, dt, amount FROM test_iot_cancel_flat_dst"""
}
