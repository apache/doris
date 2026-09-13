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

suite("test_cloud_table_stream_partition_lifecycle") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_cloud_table_stream_partition_lifecycle_db FORCE"
    sql "CREATE DATABASE test_cloud_table_stream_partition_lifecycle_db"
    sql "USE test_cloud_table_stream_partition_lifecycle_db"

    // DROP and recreate a same-named partition. The new partition has a new
    // partition ID and must start from UNKNOWN instead of inheriting the old Offset.
    sql """
        CREATE TABLE drop_source (
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
        CREATE TABLE drop_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO drop_source VALUES (1, 10), (11, 110)"
    sql "sync"
    sql """
        CREATE STREAM drop_stream ON TABLE drop_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO drop_source VALUES (2, 20), (12, 120)"
    sql "sync"
    sql "INSERT INTO drop_sink SELECT id, value FROM drop_stream"
    qt_drop_stream_consumed_before_ddl "SELECT count(*) FROM drop_stream"

    sql "ALTER TABLE drop_source DROP PARTITION p2 FORCE"
    sql "ALTER TABLE drop_source ADD PARTITION p2 VALUES LESS THAN (20)"
    qt_recreated_drop_partition_initially_empty "SELECT count(*) FROM drop_stream PARTITION(p2)"

    sql "INSERT INTO drop_source VALUES (3, 30), (13, 130)"
    sql "sync"
    order_qt_recreated_drop_partition_first_batch """
        SELECT id, value FROM drop_stream PARTITION(p2) ORDER BY id
    """
    sql "INSERT INTO drop_sink SELECT id, value FROM drop_stream PARTITION(p2)"
    order_qt_drop_other_partition_unchanged "SELECT id, value FROM drop_stream ORDER BY id"
    sql "INSERT INTO drop_sink SELECT id, value FROM drop_stream PARTITION(p1)"
    order_qt_drop_lifecycle_sink "SELECT id, value FROM drop_sink ORDER BY id"
    qt_drop_stream_consumed_after_ddl "SELECT count(*) FROM drop_stream"

    // TRUNCATE replaces the selected partition with a new partition ID. Other
    // partition Offsets stay unchanged and the new partition is consumed lazily.
    sql """
        CREATE TABLE truncate_source (
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
        CREATE TABLE truncate_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO truncate_source VALUES (1, 10), (11, 110)"
    sql "sync"
    sql """
        CREATE STREAM truncate_stream ON TABLE truncate_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO truncate_source VALUES (2, 20), (12, 120)"
    sql "sync"
    sql "INSERT INTO truncate_sink SELECT id, value FROM truncate_stream"
    qt_truncate_stream_consumed_before_ddl "SELECT count(*) FROM truncate_stream"

    sql "TRUNCATE TABLE truncate_source PARTITION(p2)"
    qt_truncated_partition_initially_empty "SELECT count(*) FROM truncate_stream PARTITION(p2)"

    sql "INSERT INTO truncate_source VALUES (3, 30), (13, 130)"
    sql "sync"
    order_qt_truncated_partition_first_batch """
        SELECT id, value FROM truncate_stream PARTITION(p2) ORDER BY id
    """
    sql "INSERT INTO truncate_sink SELECT id, value FROM truncate_stream PARTITION(p2)"
    order_qt_truncate_other_partition_unchanged "SELECT id, value FROM truncate_stream ORDER BY id"
    sql "INSERT INTO truncate_sink SELECT id, value FROM truncate_stream PARTITION(p1)"
    order_qt_truncate_lifecycle_sink "SELECT id, value FROM truncate_sink ORDER BY id"
    qt_truncate_stream_consumed_after_ddl "SELECT count(*) FROM truncate_stream"

    // REPLACE promotes a temporary partition with its own partition ID. The
    // Stream reads that partition from its beginning and then advances normally.
    sql """
        CREATE TABLE replace_source (
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
        CREATE TABLE replace_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql "INSERT INTO replace_source VALUES (1, 10), (11, 110)"
    sql "sync"
    sql """
        CREATE STREAM replace_stream ON TABLE replace_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """
    sql "INSERT INTO replace_source VALUES (2, 20), (12, 120)"
    sql "sync"
    sql "INSERT INTO replace_sink SELECT id, value FROM replace_stream"
    qt_replace_stream_consumed_before_ddl "SELECT count(*) FROM replace_stream"

    sql "ALTER TABLE replace_source ADD TEMPORARY PARTITION tp2 VALUES [(\"10\"), (\"20\"))"
    sql "INSERT INTO replace_source TEMPORARY PARTITION(tp2) VALUES (13, 130)"
    sql "sync"
    sql """
        ALTER TABLE replace_source
        REPLACE PARTITION (p2) WITH TEMPORARY PARTITION (tp2)
        PROPERTIES("use_temp_partition_name" = "false")
    """
    order_qt_replaced_partition_first_batch """
        SELECT id, value FROM replace_stream PARTITION(p2) ORDER BY id
    """
    sql "INSERT INTO replace_sink SELECT id, value FROM replace_stream PARTITION(p2)"
    qt_replaced_partition_consumed "SELECT count(*) FROM replace_stream PARTITION(p2)"

    sql "INSERT INTO replace_source VALUES (14, 140)"
    sql "sync"
    order_qt_replaced_partition_second_batch """
        SELECT id, value FROM replace_stream PARTITION(p2) ORDER BY id
    """
    sql "INSERT INTO replace_sink SELECT id, value FROM replace_stream PARTITION(p2)"
    order_qt_replace_lifecycle_sink "SELECT id, value FROM replace_sink ORDER BY id"
    qt_replace_stream_consumed_after_ddl "SELECT count(*) FROM replace_stream"
}
