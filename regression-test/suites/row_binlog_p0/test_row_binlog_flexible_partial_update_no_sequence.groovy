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

suite("test_row_binlog_flexible_partial_update_no_sequence", "nonConcurrent") {
    sql "DROP TABLE IF EXISTS test_row_binlog_flexible_partial_update_no_sequence FORCE"

    sql """
        CREATE TABLE test_row_binlog_flexible_partial_update_no_sequence (
            id INT,
            v1 INT NOT NULL DEFAULT "0",
            v2 VARCHAR(16) NULL DEFAULT "default",
            v3 INT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """
        INSERT INTO test_row_binlog_flexible_partial_update_no_sequence VALUES
            (1, 10, 'a', 101),
            (2, 20, 'b', 102),
            (4, 40, 'd', 104),
            (5, 50, 'e', 105)
    """

    // Cover duplicate-key aggregation, history fill, explicit NULL, a new key,
    // DELETE, and DELETE -> INSERT without relying on a sequence column.
    def flexibleRows = """
        {"id":1,"v1":11}
        {"id":1,"v2":"aa"}
        {"id":2,"v2":null}
        {"id":3,"v1":30}
        {"id":4,"__DORIS_DELETE_SIGN__":1}
        {"id":5,"__DORIS_DELETE_SIGN__":1}
        {"id":5}
    """.stripIndent().trim()
    streamLoad {
        table "test_row_binlog_flexible_partial_update_no_sequence"
        set "format", "json"
        set "read_json_by_line", "true"
        set "strict_mode", "false"
        set "unique_key_update_mode", "UPDATE_FLEXIBLE_COLUMNS"
        inputStream new ByteArrayInputStream(flexibleRows.getBytes())
        time 20000
    }
    sql "sync"

    order_qt_no_sequence_visible """
        SELECT id, v1, v2, v3
        FROM test_row_binlog_flexible_partial_update_no_sequence
        ORDER BY id
    """

    qt_no_sequence_raw_binlog """
        SELECT __DORIS_BINLOG_OP__, id, v1, v2, v3,
               __BEFORE__v1__, __BEFORE__v2__, __BEFORE__v3__
        FROM binlog("table" = "test_row_binlog_flexible_partial_update_no_sequence")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """

    qt_no_sequence_detail """
        SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
        FROM test_row_binlog_flexible_partial_update_no_sequence@incr("incrementType" = "DETAIL")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__, __DORIS_BINLOG_OP__
    """

    order_qt_no_sequence_min_delta """
        SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
        FROM test_row_binlog_flexible_partial_update_no_sequence@incr("incrementType" = "MIN_DELTA")
        ORDER BY id, __DORIS_BINLOG_OP__
    """

    qt_no_sequence_append_only """
        SELECT id, v1, v2, v3, __DORIS_BINLOG_OP__
        FROM test_row_binlog_flexible_partial_update_no_sequence@incr("incrementType" = "APPEND_ONLY")
        ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
    """
}
