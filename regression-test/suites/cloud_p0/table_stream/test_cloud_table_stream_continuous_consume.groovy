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

suite("test_cloud_table_stream_continuous_consume") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_cloud_table_stream_continuous_consume_db FORCE"
    sql "CREATE DATABASE test_cloud_table_stream_continuous_consume_db"
    sql "USE test_cloud_table_stream_continuous_consume_db"

    sql """
        CREATE TABLE continuous_source (
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
        CREATE TABLE continuous_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE STREAM continuous_stream ON TABLE continuous_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """

    // Round 1: repeated reads observe the same batch because SELECT does not advance the Offset.
    sql "INSERT INTO continuous_source VALUES (1, 10), (2, 20)"
    sql "sync"
    order_qt_round_1_first_read "SELECT id, value FROM continuous_stream ORDER BY id"
    order_qt_round_1_second_read "SELECT id, value FROM continuous_stream ORDER BY id"
    sql "INSERT INTO continuous_sink SELECT id, value FROM continuous_stream"
    order_qt_sink_after_round_1 "SELECT id, value FROM continuous_sink ORDER BY id"
    qt_stream_empty_after_round_1 "SELECT count(*) FROM continuous_stream"

    // Round 2: a later statement extends its upper bound to include a write made after a pure read.
    sql "INSERT INTO continuous_source VALUES (3, 30)"
    sql "sync"
    order_qt_round_2_before_more_input "SELECT id, value FROM continuous_stream ORDER BY id"
    sql "INSERT INTO continuous_source VALUES (4, 40)"
    sql "sync"
    order_qt_round_2_after_more_input "SELECT id, value FROM continuous_stream ORDER BY id"
    sql "INSERT INTO continuous_sink SELECT id, value FROM continuous_stream"
    order_qt_sink_after_round_2 "SELECT id, value FROM continuous_sink ORDER BY id"
    qt_stream_empty_after_round_2 "SELECT count(*) FROM continuous_stream"

    // Round 3: consume another independent batch and verify it cannot be consumed twice.
    sql "INSERT INTO continuous_source VALUES (5, 50), (6, 60)"
    sql "sync"
    order_qt_round_3_before_consume "SELECT id, value FROM continuous_stream ORDER BY id"
    sql "INSERT INTO continuous_sink SELECT id, value FROM continuous_stream"
    sql "INSERT INTO continuous_sink SELECT id, value FROM continuous_stream"
    order_qt_sink_after_round_3 "SELECT id, value FROM continuous_sink ORDER BY id"
    qt_stream_empty_after_round_3 "SELECT count(*) FROM continuous_stream"

    // Round 4 consumes without a preceding read to cover the direct consumer path.
    sql "INSERT INTO continuous_source VALUES (7, 70)"
    sql "sync"
    sql "INSERT INTO continuous_sink SELECT id, value FROM continuous_stream"

    order_qt_final_source "SELECT id, value FROM continuous_source ORDER BY id"
    order_qt_final_sink "SELECT id, value FROM continuous_sink ORDER BY id"
    qt_final_stream_empty "SELECT count(*) FROM continuous_stream"
}
