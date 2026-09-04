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

suite("test_cloud_table_stream_initial_rows") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_cloud_table_stream_initial_rows_db FORCE"
    sql "CREATE DATABASE test_cloud_table_stream_initial_rows_db"
    sql "USE test_cloud_table_stream_initial_rows_db"

    // A non-empty source with show_initial_rows=true starts with the creation snapshot.
    sql """
        CREATE TABLE initial_true_source (
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
        CREATE TABLE initial_true_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO initial_true_source VALUES (1, 10), (2, 20)"
    sql "sync"
    sql """
        CREATE STREAM initial_true_stream ON TABLE initial_true_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "true"
        )
    """

    order_qt_initial_true_snapshot "SELECT id, value FROM initial_true_stream ORDER BY id"
    sql "INSERT INTO initial_true_sink SELECT id, value FROM initial_true_stream"
    qt_initial_true_empty_after_snapshot "SELECT count(*) FROM initial_true_stream"
    sql "INSERT INTO initial_true_source VALUES (3, 30)"
    sql "sync"
    order_qt_initial_true_incremental "SELECT id, value FROM initial_true_stream ORDER BY id"
    sql "INSERT INTO initial_true_sink SELECT id, value FROM initial_true_stream"
    order_qt_initial_true_sink "SELECT id, value FROM initial_true_sink ORDER BY id"
    qt_initial_true_empty_after_incremental "SELECT count(*) FROM initial_true_stream"

    // A non-empty source with show_initial_rows=false starts after the creation snapshot.
    sql """
        CREATE TABLE initial_false_source (
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
        CREATE TABLE initial_false_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO initial_false_source VALUES (1, 10), (2, 20)"
    sql "sync"
    sql """
        CREATE STREAM initial_false_stream ON TABLE initial_false_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """

    qt_initial_false_skips_snapshot "SELECT count(*) FROM initial_false_stream"
    sql "INSERT INTO initial_false_source VALUES (3, 30)"
    sql "sync"
    order_qt_initial_false_incremental "SELECT id, value FROM initial_false_stream ORDER BY id"
    sql "INSERT INTO initial_false_sink SELECT id, value FROM initial_false_stream"
    order_qt_initial_false_sink "SELECT id, value FROM initial_false_sink ORDER BY id"
    qt_initial_false_empty_after_consume "SELECT count(*) FROM initial_false_stream"

    // Creating a Stream on an empty table must establish a valid empty boundary.
    sql """
        CREATE TABLE empty_source (
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
        CREATE TABLE empty_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE STREAM empty_stream ON TABLE empty_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "true"
        )
    """

    qt_empty_source_initially_empty "SELECT count(*) FROM empty_stream"
    sql "INSERT INTO empty_source VALUES (1, 10)"
    sql "sync"
    order_qt_empty_source_first_incremental "SELECT id, value FROM empty_stream ORDER BY id"
    sql "INSERT INTO empty_sink SELECT id, value FROM empty_stream"
    sql "INSERT INTO empty_sink SELECT id, value FROM empty_stream"
    order_qt_empty_source_sink_once "SELECT id, value FROM empty_sink ORDER BY id"
    qt_empty_source_empty_after_consume "SELECT count(*) FROM empty_stream"

    // An existing empty Partition has its own empty boundary and starts consuming at its first write.
    sql """
        CREATE TABLE empty_partition_source (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        PARTITION BY RANGE(id) (
            PARTITION p1 VALUES LESS THAN (100),
            PARTITION p2 VALUES LESS THAN (200)
        )
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        )
    """
    sql """
        CREATE TABLE empty_partition_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO empty_partition_source VALUES (10, 100)"
    sql "sync"
    sql """
        CREATE STREAM empty_partition_stream ON TABLE empty_partition_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "true"
        )
    """

    order_qt_empty_partition_initial_snapshot """
        SELECT id, value FROM empty_partition_stream ORDER BY id
    """
    sql "INSERT INTO empty_partition_sink SELECT id, value FROM empty_partition_stream"
    qt_empty_partition_after_snapshot "SELECT count(*) FROM empty_partition_stream"

    sql "INSERT INTO empty_partition_source VALUES (110, 1100)"
    sql "sync"
    order_qt_empty_partition_first_write """
        SELECT id, value FROM empty_partition_stream PARTITION(p2) ORDER BY id
    """
    sql """
        INSERT INTO empty_partition_sink
        SELECT id, value FROM empty_partition_stream PARTITION(p2)
    """
    sql """
        INSERT INTO empty_partition_sink
        SELECT id, value FROM empty_partition_stream PARTITION(p2)
    """
    order_qt_empty_partition_sink_once "SELECT id, value FROM empty_partition_sink ORDER BY id"
    qt_empty_partition_empty_after_consume """
        SELECT count(*) FROM empty_partition_stream PARTITION(p2)
    """
}
