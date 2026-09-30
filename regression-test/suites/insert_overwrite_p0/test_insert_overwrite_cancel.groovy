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
 * not committed anything yet.
 *
 * <p>An overwrite commits its rows into temporary partitions and publishes them with a swap afterwards, so a
 * cancellation that lands between the two halves cannot take the rows back: they are durable, and everything
 * the write read -- the base table stream offsets among it -- was committed with them. Dropping the temporary
 * partitions there, and answering the client with a success for an overwrite that never happened, is what
 * loses those rows against an advanced offset. What the command has to do instead is finish the swap. A
 * cancellation that lands before the first half has committed anything has nothing to take back, and there
 * the statement has to fail rather than report the success of an overwrite that did not run.
 *
 * <p>Both are injected by one debug point in the overwrite, scoped by stage and by table name, so each case
 * lands on a real statement rather than on a race.
 */
suite("test_insert_overwrite_cancel", "nonConcurrent") {
    def cancelPoint = "InsertOverwriteTableCommand.cancelAnOverwrite"

    GetDebugPoint().disableDebugPointForAllFEs(cancelPoint)
    sql """DROP TABLE IF EXISTS test_iot_cancel_src"""
    sql """DROP TABLE IF EXISTS test_iot_cancel_dst"""

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

    // A cancellation that lands before the rows are committed has nothing to take back, so the statement has
    // to report it, and the table has to be where it was.
    try {
        GetDebugPoint().enableDebugPointForAllFEs(cancelPoint,
                [stage: "beforeTheInsert", table_name: "test_iot_cancel_dst"])
        test {
            sql """INSERT OVERWRITE TABLE test_iot_cancel_dst SELECT * FROM test_iot_cancel_src"""
            exception "insert overwrite is cancelled before registerTask"
        }
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(cancelPoint)
    }
    order_qt_dst_after_the_cancelled_statement """SELECT id, dt, amount FROM test_iot_cancel_dst"""

    // A cancellation that lands after the rows are committed cannot take them back: they are in the temporary
    // partitions, and the swap is what publishes them. The statement reports the success it now is, and the
    // rows it read are the ones the table holds.
    try {
        GetDebugPoint().enableDebugPointForAllFEs(cancelPoint,
                [stage: "afterTheInsert", table_name: "test_iot_cancel_dst"])
        sql """INSERT OVERWRITE TABLE test_iot_cancel_dst SELECT * FROM test_iot_cancel_src"""
    } finally {
        GetDebugPoint().disableDebugPointForAllFEs(cancelPoint)
    }
    order_qt_dst_after_the_cancelled_window """SELECT id, dt, amount FROM test_iot_cancel_dst"""
}
