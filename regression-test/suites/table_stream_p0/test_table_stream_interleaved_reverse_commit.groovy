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

// Two sessions interleave writes on the same base table but commit in reverse
// order: session A opens its txn first, yet commits AFTER session B. Streaming
// consumption must order rows by commit tso, so B (committed first) is consumed
// before A (committed later), regardless of the begin order.
suite("test_table_stream_interleaved_reverse_commit", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_interleave_db"
    sql "CREATE DATABASE test_table_stream_interleave_db"
    sql "USE test_table_stream_interleave_db"

    sql """
        CREATE TABLE base_interleave (
            id INT, src VARCHAR(8)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    // append_only stream consuming only post-creation increments.
    sql """
        CREATE STREAM s_interleave ON TABLE base_interleave
        PROPERTIES ("type" = "append_only", "show_initial_rows" = "false")
    """
    sql "sync"

    // Session A (default connection) opens its txn and writes a row, but keeps
    // the txn open (does NOT commit yet).
    sql "BEGIN"
    sql "INSERT INTO base_interleave VALUES (1, 'A')"

    // Session B (a separate connection) opens its own txn, writes a row and
    // commits FIRST, so its commit tso is smaller than A's.
    connect(context.config.jdbcUser, context.config.jdbcPassword, context.config.jdbcUrl) {
        sql "USE test_table_stream_interleave_db"
        sql "BEGIN"
        sql "INSERT INTO base_interleave VALUES (2, 'B')"
        sql "COMMIT"
    }

    // Now session A commits, with a strictly larger commit tso than B.
    sql "COMMIT"
    sql "sync"

    // Consume via the stream. Rows are ordered by commit tso: B (committed
    // first) must precede A (committed later).
    def rows = sql """
        SELECT src, __DORIS_STREAM_SEQUENCE_COL__
        FROM s_interleave
        ORDER BY __DORIS_STREAM_SEQUENCE_COL__
    """
    assertEquals(2, rows.size())
    assertEquals(["B", "A"], rows.collect { it[0] })

    long seqB = rows[0][1] as long
    long seqA = rows[1][1] as long
    assertTrue(seqB < seqA,
        "B committed first so seqB(${seqB}) must be < seqA(${seqA})")

    sql "DROP DATABASE IF EXISTS test_table_stream_interleave_db"
}
