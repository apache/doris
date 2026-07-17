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

// Verify that streams created on the same base table maintain independent
// offsets. Consuming one stream must not change the rows visible to another.
suite("test_table_stream_multiple_streams", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_table_stream_multiple_streams_db"
    sql "CREATE DATABASE test_table_stream_multiple_streams_db"
    sql "USE test_table_stream_multiple_streams_db"

    sql """
        CREATE TABLE multi_stream_base (
            id INT,
            value VARCHAR(16)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """

    sql """
        CREATE TABLE multi_stream_target_a (
            id INT,
            value VARCHAR(16)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE TABLE multi_stream_target_b (
            id INT,
            value VARCHAR(16)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql """
        CREATE STREAM multi_stream_a ON TABLE multi_stream_base
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql """
        CREATE STREAM multi_stream_b ON TABLE multi_stream_base
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """

    sql "INSERT INTO multi_stream_base VALUES (1, 'a'), (2, 'b')"
    sql "sync"
    sleep(1200)

    def normalizeRows = { rows ->
        rows.collect { row -> row.collect { value -> value == null ? "null" : value.toString() } }
    }
    def firstBatch = [["1", "a"], ["2", "b"]]
    assertEquals(firstBatch, normalizeRows(sql("SELECT id, value FROM multi_stream_a ORDER BY id")))
    assertEquals(firstBatch, normalizeRows(sql("SELECT id, value FROM multi_stream_b ORDER BY id")))

    sql "INSERT INTO multi_stream_target_a SELECT id, value FROM multi_stream_a"
    sql "sync"
    assertEquals(0, sql("SELECT id, value FROM multi_stream_a").size())
    assertEquals(firstBatch, normalizeRows(sql("SELECT id, value FROM multi_stream_b ORDER BY id")))

    sql "INSERT INTO multi_stream_base VALUES (3, 'c')"
    sql "sync"
    sleep(1200)

    assertEquals([["3", "c"]], normalizeRows(sql("SELECT id, value FROM multi_stream_a ORDER BY id")))
    assertEquals([["1", "a"], ["2", "b"], ["3", "c"]],
            normalizeRows(sql("SELECT id, value FROM multi_stream_b ORDER BY id")))

    sql "INSERT INTO multi_stream_target_b SELECT id, value FROM multi_stream_b"
    sql "sync"
    assertEquals(0, sql("SELECT id, value FROM multi_stream_b").size())
    assertEquals([["3", "c"]], normalizeRows(sql("SELECT id, value FROM multi_stream_a ORDER BY id")))

    sql "INSERT INTO multi_stream_target_a SELECT id, value FROM multi_stream_a"
    sql "sync"
    assertEquals(0, sql("SELECT id, value FROM multi_stream_a").size())

    assertEquals([["1", "a"], ["2", "b"], ["3", "c"]],
            normalizeRows(sql("SELECT id, value FROM multi_stream_target_a ORDER BY id")))
    assertEquals([["1", "a"], ["2", "b"], ["3", "c"]],
            normalizeRows(sql("SELECT id, value FROM multi_stream_target_b ORDER BY id")))
}
