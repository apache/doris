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

suite("test_ivm_hidden_columns") {
    // IVM target with row-store hidden column, matching the DORIS-27664 failure mode.
    sql """drop materialized view if exists ivm_hidden_row_store_mv"""
    sql """drop table if exists ivm_hidden_row_store_base"""

    sql """
        CREATE TABLE ivm_hidden_row_store_base (
            id INT,
            value INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true"
        )
    """

    sql """
        INSERT INTO ivm_hidden_row_store_base VALUES
            (1, 10),
            (2, 20),
            (3, 30)
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_hidden_row_store_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true",
            "light_schema_change" = "true",
            "store_row_column" = "true"
        )
        AS SELECT id, value FROM ivm_hidden_row_store_base
    """

    sql """REFRESH MATERIALIZED VIEW ivm_hidden_row_store_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_hidden_row_store_mv")
    order_qt_row_store_after_complete_initial """
        SELECT id, value FROM ivm_hidden_row_store_mv
    """

    sql """
        INSERT INTO ivm_hidden_row_store_base VALUES (4, 40)
    """
    sql """REFRESH MATERIALIZED VIEW ivm_hidden_row_store_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_hidden_row_store_mv")
    order_qt_row_store_after_incremental """
        SELECT id, value FROM ivm_hidden_row_store_mv
    """

    sql """REFRESH MATERIALIZED VIEW ivm_hidden_row_store_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_hidden_row_store_mv")
    order_qt_row_store_after_complete """
        SELECT id, value FROM ivm_hidden_row_store_mv
    """

    // IVM target with the MOW skip-bitmap hidden column.
    sql """drop materialized view if exists ivm_hidden_skip_bitmap_mv"""
    sql """drop table if exists ivm_hidden_skip_bitmap_base"""

    sql """
        CREATE TABLE ivm_hidden_skip_bitmap_base (
            id INT,
            value INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """
        INSERT INTO ivm_hidden_skip_bitmap_base VALUES
            (1, 100),
            (2, 200),
            (3, 300)
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_hidden_skip_bitmap_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_skip_bitmap_column" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
        AS SELECT id, value FROM ivm_hidden_skip_bitmap_base
    """

    sql """REFRESH MATERIALIZED VIEW ivm_hidden_skip_bitmap_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_hidden_skip_bitmap_mv")
    order_qt_skip_bitmap_after_complete_initial """
        SELECT id, value FROM ivm_hidden_skip_bitmap_mv
    """

    sql """
        INSERT INTO ivm_hidden_skip_bitmap_base VALUES (4, 400)
    """
    sql """REFRESH MATERIALIZED VIEW ivm_hidden_skip_bitmap_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_hidden_skip_bitmap_mv")
    order_qt_skip_bitmap_after_incremental """
        SELECT id, value FROM ivm_hidden_skip_bitmap_mv
    """

    sql """REFRESH MATERIALIZED VIEW ivm_hidden_skip_bitmap_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_hidden_skip_bitmap_mv")
    order_qt_skip_bitmap_after_complete """
        SELECT id, value FROM ivm_hidden_skip_bitmap_mv
    """
}
