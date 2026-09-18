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

suite("test_cloud_table_stream_e2e") {
    if (!isCloudMode()) {
        return
    }

    sql "DROP DATABASE IF EXISTS test_cloud_table_stream_e2e_db FORCE"
    sql "CREATE DATABASE test_cloud_table_stream_e2e_db"
    sql "USE test_cloud_table_stream_e2e_db"

    sql """
        CREATE TABLE stream_source (
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
        CREATE TABLE stream_sink (
            id INT,
            value INT
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO stream_source VALUES (1, 10), (2, 20)"
    sql "sync"
    sql """
        CREATE STREAM cloud_stream ON TABLE stream_source
        PROPERTIES (
            "type" = "append_only",
            "show_initial_rows" = "false"
        )
    """

    sql "INSERT INTO stream_source VALUES (3, 30), (4, 40)"
    sql "sync"

    order_qt_incremental_before_consume """
        SELECT id, value
        FROM cloud_stream
        ORDER BY id
    """
    order_qt_select_does_not_advance_offset """
        SELECT id, value
        FROM cloud_stream
        ORDER BY id
    """
    order_qt_snapshot_at_initial_offset """
        SELECT id, value
        FROM cloud_stream@snapshot()
        ORDER BY id
    """
    order_qt_reset_to_current_base_table """
        SELECT id, value
        FROM cloud_stream@reset()
        ORDER BY id
    """
    order_qt_consumption_visible_before_first_insert """
        SELECT STREAM_NAME,
               UNIT,
               CONSUMPTION_STATUS <> 'N/A',
               LAST_CONSUMPTION_TIME = -1
        FROM information_schema.table_stream_consumption
        WHERE DB_NAME = 'test_cloud_table_stream_e2e_db'
          AND STREAM_NAME = 'cloud_stream'
        ORDER BY UNIT
    """

    sql "INSERT INTO stream_sink SELECT id, value FROM cloud_stream"
    order_qt_first_consumption_target "SELECT id, value FROM stream_sink ORDER BY id"
    qt_stream_empty_after_first_consumption "SELECT count(*) FROM cloud_stream"

    sql "INSERT INTO stream_source VALUES (5, 50)"
    sql "sync"
    order_qt_second_incremental_batch "SELECT id, value FROM cloud_stream ORDER BY id"

    sql "INSERT INTO stream_sink SELECT id, value FROM cloud_stream"
    order_qt_second_consumption_target "SELECT id, value FROM stream_sink ORDER BY id"
    qt_stream_empty_after_second_consumption "SELECT count(*) FROM cloud_stream"
    order_qt_consumption_visible_after_insert """
        SELECT STREAM_NAME,
               UNIT,
               CONSUMPTION_STATUS <> 'N/A',
               LAG = '0',
               LAST_CONSUMPTION_TIME > 0
        FROM information_schema.table_stream_consumption
        WHERE DB_NAME = 'test_cloud_table_stream_e2e_db'
          AND STREAM_NAME = 'cloud_stream'
        ORDER BY UNIT
    """

    sql "DROP STREAM cloud_stream FORCE"
    qt_stream_removed_from_catalog """
        SELECT count(*)
        FROM information_schema.table_streams
        WHERE DB_NAME = 'test_cloud_table_stream_e2e_db'
          AND STREAM_NAME = 'cloud_stream'
    """
    test {
        sql "SELECT id, value FROM cloud_stream"
        exception "does not exist in database"
    }
}
