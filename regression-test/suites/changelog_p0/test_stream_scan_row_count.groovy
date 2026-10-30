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

import org.awaitility.Awaitility

import static java.util.concurrent.TimeUnit.SECONDS

/**
 * The optimizer must estimate a stream read from what the read answers with, not at one row.
 *
 * <p>A stream read is planned over a lightweight wrapper of the base table. The wrapper holds no
 * partitions of its own, so a row count derived from its own partitions collapsed to zero and the
 * statistics calculator raised that to one: a stream read of a large table looked like a one-row
 * scan, and the optimizer could pick the real large side as a broadcast build.
 *
 * <p>Three reads answer different things and are estimated differently. A reset read answers with
 * the table as it is now, a snapshot read with the image at the stream's offset, and an
 * incremental read with the change rows written after the offset. The last two are recorded on the
 * stream when it advances its offset (the row counts of the changelog index and of the base index),
 * so the estimate of an incremental read is the change rows since then rather than the whole table.
 *
 * <p>This suite runs in both shared-nothing and cloud mode, and that matters: in cloud every read
 * of a stream wraps the base table once the cloud read states are installed, so even a reset read
 * goes through the wrapper; in shared-nothing a reset read scans the real table and only a
 * snapshot read uses a wrapper. Both must be estimated from the table.
 */
suite("test_stream_scan_row_count") {
    sql "DROP STREAM IF EXISTS stream_read_rowcount_stream FORCE"
    sql "DROP TABLE IF EXISTS stream_read_rowcount_sink"
    sql "DROP TABLE IF EXISTS stream_read_rowcount_tbl"
    sql """
        CREATE TABLE stream_read_rowcount_tbl (
            id BIGINT NOT NULL,
            v BIGINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "changelog.enable" = "true",
            "changelog.ttl_seconds" = "86400"
        )
    """
    sql """
        CREATE TABLE stream_read_rowcount_sink (
            id BIGINT NOT NULL,
            v BIGINT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    // Five rows exist when the stream is created; the last two are written afterwards, so the
    // snapshot image holds five rows while the table holds seven. The syncs make sure the rows are
    // published, so the stream records their offsets rather than an unset one.
    sql "INSERT INTO stream_read_rowcount_tbl VALUES (1,10),(2,20),(3,30),(4,40),(5,50)"
    sql "sync"

    // The estimated row count of the base table's scan in the physical plan.
    def scanRowCount = { String query ->
        def planLines = sql "explain physical plan ${query}"
        def scanLine = planLines.collect { it[0] }.find {
            it.contains("PhysicalOlapScan") && it.contains("table=stream_read_rowcount_tbl")
        }
        assertTrue(scanLine != null, "no scan of stream_read_rowcount_tbl in the plan of [${query}]: ${planLines}")
        def stats = (scanLine =~ /stats=([0-9]+)/)
        assertTrue(stats.find(), "no row count in the scan line: ${scanLine}")
        return stats.group(1) as long
    }

    // The counters a stream records when it advances are the row counts the frontend holds, and
    // those come from the tablet reports, which a backend sends periodically. Wait for them before
    // anything is recorded, otherwise the stream records a baseline of zero and every read after
    // it is over-estimated. SHOW TABLETS prints the same reported row counts the stream records.
    def awaitTabletRowCount = { long expected ->
        Awaitility.await().atMost(300, SECONDS).pollInterval(2, SECONDS).until({
            def tablets = sql_return_maparray "show tablets from stream_read_rowcount_tbl"
            // Both the base tablet and the changelog tablet hold the rows the table was loaded with.
            tablets.every { (it["RowCount"] as long) == expected }
        })
    }

    awaitTabletRowCount(5L)
    sql """
        CREATE STREAM stream_read_rowcount_stream ON TABLE stream_read_rowcount_tbl
        PROPERTIES ('show_initial_rows'='false')
    """
    sql "INSERT INTO stream_read_rowcount_tbl VALUES (6,60),(7,70)"
    sql "sync"
    awaitTabletRowCount(7L)

    // stream@reset() reads the table as it is now, in both modes.
    assertEquals(7L, scanRowCount("select * from stream_read_rowcount_stream@reset()"))

    // stream@snapshot() answers with the image at the stream's offset (five rows). A duplicate key
    // image only grows, so the current table is its upper bound -- but it must not collapse to one
    // row.
    assertEquals(7L, scanRowCount("select * from stream_read_rowcount_stream@snapshot()"))

    // Consuming the stream advances its offset, and the stream records there the row counts of the
    // changelog index and of the base index the next estimate is built from.
    sql "INSERT INTO stream_read_rowcount_sink SELECT id, v FROM stream_read_rowcount_stream"

    // Nothing was written after the offset: the read returns no row, so the plan keeps no scan at
    // all rather than one estimated at a single row.
    def emptyPlan = sql "explain physical plan select id, v from stream_read_rowcount_stream"
    assertTrue(emptyPlan.collect { it[0] }.any { it.contains("PhysicalEmptyRelation") },
            "the incremental read of a caught-up stream should be an empty relation: ${emptyPlan}")

    sql "INSERT INTO stream_read_rowcount_tbl VALUES (8,80),(9,90),(10,100)"
    sql "sync"
    awaitTabletRowCount(10L)

    // The three rows written after the offset -- not the ten rows the table holds, and not the
    // ten rows the changelog holds.
    assertEquals(3L, scanRowCount("select id, v from stream_read_rowcount_stream"))

    assertEquals(10L, scanRowCount("select * from stream_read_rowcount_stream@reset()"))
}
