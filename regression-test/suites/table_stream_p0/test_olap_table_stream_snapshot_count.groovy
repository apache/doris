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

// Regression for the storage-layer statistics fast path (VStatisticsIterator)
// ignoring the row-level commit-tso predicate that BE injects for stream
// snapshot reads. After compaction merges pre-snapshot and post-snapshot rows
// into one segment, a pushed-down COUNT(*) over @snapshot() must still honor
// commit_tso <= snapshot_tso, i.e. count only the pre-snapshot rows.
suite("test_olap_table_stream_snapshot_count", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_olap_table_stream_snapshot_count_db"
    sql "CREATE DATABASE test_olap_table_stream_snapshot_count_db"
    sql "USE test_olap_table_stream_snapshot_count_db"
    sql "SET show_hidden_columns=false"

    def waitVisible = {
        sql "sync"
        sleep(1200)
    }

    sql """
        CREATE TABLE snapshot_count_dup (
            id INT,
            v INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "disable_auto_compaction" = "true"
        )
    """

    // Pre-snapshot rows.
    sql "INSERT INTO snapshot_count_dup VALUES (1, 10), (2, 20)"
    waitVisible()

    // Stream creation fixes the snapshot tso. show_initial_rows=true so the
    // snapshot image is exactly the pre-snapshot rows above.
    sql """
        CREATE STREAM s_snapshot_count ON TABLE snapshot_count_dup
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """

    // Post-snapshot rows: written with a larger commit_tso than the snapshot.
    sql "INSERT INTO snapshot_count_dup VALUES (3, 30), (4, 40), (5, 50)"
    waitVisible()

    // Merge the pre/post-snapshot rowsets into a single segment. Now one segment
    // holds rows with mixed commit_tso, so version-level pruning alone cannot
    // exclude the post-snapshot rows; correctness relies on the row-level
    // commit_tso predicate.
    trigger_and_wait_compaction("snapshot_count_dup", "cumulative")

    // Row-level snapshot read: the correct pre-snapshot image (2 rows).
    def snapshotRows = sql "SELECT id, v FROM s_snapshot_count@snapshot() ORDER BY id"
    assertEquals([[1, 10], [2, 20]], snapshotRows)

    // COUNT(*) is pushed down as a storage-layer aggregate. It must return 2,
    // matching the row-level result. With the fast-path bug it returns 5 (the
    // raw segment num_rows including post-snapshot rows).
    def snapshotCount = sql "SELECT count(*) FROM s_snapshot_count@snapshot()"
    assertEquals(2L, snapshotCount[0][0])

    sql "DROP DATABASE IF EXISTS test_olap_table_stream_snapshot_count_db"
}
