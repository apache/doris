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

suite("test_cloud_table_stream_partition_consume") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_cloud_table_stream_partition_consume_db FORCE"
    sql "CREATE DATABASE test_cloud_table_stream_partition_consume_db"
    sql "USE test_cloud_table_stream_partition_consume_db"

    sql """
        CREATE TABLE partition_source (
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
        CREATE TABLE partition_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO partition_source VALUES (10, 100), (110, 1100)"
    sql "sync"
    sql """
        CREATE STREAM partition_stream ON TABLE partition_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO partition_source VALUES (20, 200), (120, 1200)"
    sql "sync"

    order_qt_p1_before_consume """
        SELECT id, value FROM partition_stream PARTITION(p1) ORDER BY id
    """
    order_qt_p2_before_consume """
        SELECT id, value FROM partition_stream PARTITION(p2) ORDER BY id
    """

    sql """
        INSERT INTO partition_sink
        SELECT id, value FROM partition_stream PARTITION(p1)
    """
    qt_p1_empty_after_consume "SELECT count(*) FROM partition_stream PARTITION(p1)"
    order_qt_p2_unchanged_after_p1_consume """
        SELECT id, value FROM partition_stream PARTITION(p2) ORDER BY id
    """
    order_qt_sink_after_p1 "SELECT id, value FROM partition_sink ORDER BY id"

    sql """
        INSERT INTO partition_sink
        SELECT id, value FROM partition_stream PARTITION(p2)
    """
    qt_p2_empty_after_consume "SELECT count(*) FROM partition_stream PARTITION(p2)"
    order_qt_sink_after_p2 "SELECT id, value FROM partition_sink ORDER BY id"

    // ADD PARTITION does not initialize an Offset eagerly. Its UNKNOWN read state is an empty range.
    sql "ALTER TABLE partition_source ADD PARTITION p3 VALUES LESS THAN (300)"
    qt_new_partition_immediately_empty "SELECT count(*) FROM partition_stream PARTITION(p3)"

    sql "INSERT INTO partition_source VALUES (220, 2200)"
    sql "sync"
    order_qt_new_partition_first_write """
        SELECT id, value FROM partition_stream PARTITION(p3) ORDER BY id
    """
    sql """
        INSERT INTO partition_sink
        SELECT id, value FROM partition_stream PARTITION(p3)
    """
    qt_new_partition_empty_after_consume "SELECT count(*) FROM partition_stream PARTITION(p3)"

    // A second consumption observes the newly created local Offset and must not duplicate data.
    sql """
        INSERT INTO partition_sink
        SELECT id, value FROM partition_stream PARTITION(p3)
    """
    order_qt_sink_after_p3_consumed_once "SELECT id, value FROM partition_sink ORDER BY id"
}
