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

suite("test_cloud_table_stream_multi_reference") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_cloud_table_stream_multi_reference_db FORCE"
    sql "CREATE DATABASE test_cloud_table_stream_multi_reference_db"
    sql "USE test_cloud_table_stream_multi_reference_db"

    // Consume two independent Streams in one transaction.
    sql """
        CREATE TABLE multi_source_a (
            id INT,
            value INT
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
        CREATE TABLE multi_source_b (
            id INT,
            value INT
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
        CREATE TABLE multi_sink (
            source_no INT,
            id INT,
            value INT
        )
        DUPLICATE KEY(source_no, id)
        DISTRIBUTED BY HASH(source_no, id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE STREAM multi_stream_a ON TABLE multi_source_a
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql """
        CREATE STREAM multi_stream_b ON TABLE multi_source_b
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO multi_source_a VALUES (1, 10)"
    sql "INSERT INTO multi_source_b VALUES (2, 20)"
    sql "sync"

    sql """
        INSERT INTO multi_sink
        SELECT 1, id, value FROM multi_stream_a
        UNION ALL
        SELECT 2, id, value FROM multi_stream_b
    """
    order_qt_two_streams_one_statement """
        SELECT source_no, id, value
        FROM multi_sink
        ORDER BY source_no, id
    """
    qt_multi_stream_a_consumed "SELECT count(*) FROM multi_stream_a"
    qt_multi_stream_b_consumed "SELECT count(*) FROM multi_stream_b"

    // Reusing one Stream through an alias and a CTE must install one
    // statement-level read snapshot and advance its Offset only once.
    sql """
        CREATE TABLE alias_source (
            id INT,
            value INT
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
        CREATE TABLE alias_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE STREAM alias_stream ON TABLE alias_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO alias_source VALUES (3, 30), (4, 40)"
    sql "sync"

    order_qt_alias_read """
        SELECT s.id, s.value
        FROM alias_stream AS s
        ORDER BY s.id
    """
    order_qt_cte_reused_read """
        WITH stream_rows AS (
            SELECT id, value FROM alias_stream
        )
        SELECT left_rows.id, left_rows.value
        FROM stream_rows AS left_rows
        JOIN stream_rows AS right_rows ON left_rows.id = right_rows.id
        ORDER BY left_rows.id
    """
    sql """
        INSERT INTO alias_sink
        WITH stream_rows AS (
            SELECT id, value FROM alias_stream
        )
        SELECT left_rows.id, left_rows.value
        FROM stream_rows AS left_rows
        JOIN stream_rows AS right_rows ON left_rows.id = right_rows.id
    """
    order_qt_alias_cte_sink "SELECT id, value FROM alias_sink ORDER BY id"
    qt_alias_stream_consumed "SELECT count(*) FROM alias_stream"

    // References to different partitions of the same Stream are merged into
    // one Stream update carrying both partition Offsets.
    sql """
        CREATE TABLE partition_source (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        PARTITION BY RANGE(id) (
            PARTITION p1 VALUES LESS THAN (10),
            PARTITION p2 VALUES LESS THAN (20)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE TABLE partition_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO partition_source VALUES (1, 10), (11, 110)"
    sql "sync"
    sql """
        CREATE STREAM partition_stream ON TABLE partition_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO partition_source VALUES (2, 20), (12, 120)"
    sql "sync"

    sql """
        INSERT INTO partition_sink
        SELECT id, value FROM partition_stream PARTITION(p1)
        UNION ALL
        SELECT id, value FROM partition_stream PARTITION(p2)
    """
    order_qt_partition_updates_merged "SELECT id, value FROM partition_sink ORDER BY id"
    qt_partition_stream_consumed "SELECT count(*) FROM partition_stream"
}
