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

suite("test_ivm_drop_base_table_stream_state") {
    if (isCloudMode()) {
        return
    }

    sql "DROP STREAM IF EXISTS ivm_drop_base_stream_state_stream"
    sql "DROP TABLE IF EXISTS ivm_drop_base_stream_state_base"

    sql """
        CREATE TABLE ivm_drop_base_stream_state_base (
            id BIGINT NOT NULL,
            value BIGINT
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO ivm_drop_base_stream_state_base VALUES (1, 10), (2, 20)"

    sql """
        CREATE STREAM ivm_drop_base_stream_state_stream
        ON TABLE ivm_drop_base_stream_state_base
        PROPERTIES ("show_initial_rows" = "true")
    """

    order_qt_initial_mv_rows """
        SELECT id, value
        FROM ivm_drop_base_stream_state_stream
        ORDER BY id
    """

    order_qt_stream_before_drop """
        SELECT BASE_TABLE_CTL, BASE_TABLE_DB, BASE_TABLE_NAME, BASE_TABLE_TYPE,
               ENABLED, IS_STALE, STALE_REASON
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND BASE_TABLE_NAME = 'ivm_drop_base_stream_state_base'
          AND STREAM_NAME = 'ivm_drop_base_stream_state_stream'
        ORDER BY STREAM_NAME
    """

    sql "DROP TABLE ivm_drop_base_stream_state_base"

    order_qt_stream_after_drop """
        SELECT BASE_TABLE_CTL, BASE_TABLE_DB, BASE_TABLE_NAME, BASE_TABLE_TYPE,
               ENABLED, IS_STALE, STALE_REASON
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND BASE_TABLE_NAME = 'ivm_drop_base_stream_state_base'
          AND STREAM_NAME = 'ivm_drop_base_stream_state_stream'
        ORDER BY STREAM_NAME
    """

    sql "RECOVER TABLE ivm_drop_base_stream_state_base"

    order_qt_stream_after_recover """
        SELECT BASE_TABLE_CTL, BASE_TABLE_DB, BASE_TABLE_NAME, BASE_TABLE_TYPE,
               ENABLED, IS_STALE, STALE_REASON
        FROM information_schema.table_streams
        WHERE DB_NAME = '${context.dbName}'
          AND BASE_TABLE_NAME = 'ivm_drop_base_stream_state_base'
          AND STREAM_NAME = 'ivm_drop_base_stream_state_stream'
        ORDER BY STREAM_NAME
    """

    sql "INSERT INTO ivm_drop_base_stream_state_base VALUES (3, 30)"
    sql "sync"

    order_qt_mv_rows_after_recover """
        SELECT id, value
        FROM ivm_drop_base_stream_state_stream
        ORDER BY id
    """
}
