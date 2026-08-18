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

suite("test_row_binlog_flexible_partial_update") {
    def tableName = "test_row_binlog_flexible_partial_update"
    sql "DROP TABLE IF EXISTS ${tableName} FORCE"
    try {
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                v1 INT NOT NULL DEFAULT "0",
                v2 VARCHAR(16) NULL DEFAULT "default",
                seq INT
            )
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "light_schema_change" = "true",
                "enable_unique_key_skip_bitmap_column" = "true",
                "function_column.sequence_col" = "seq",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """

        sql """
            INSERT INTO ${tableName} VALUES
                (1, 10, 'a', 100),
                (2, 20, 'b', 100),
                (4, 40, 'd', 100)
        """

        def flexibleRows = """
            {"id":1,"v1":11,"seq":110}
            {"id":1,"v2":"aa"}
            {"id":2,"v2":null,"seq":120}
            {"id":3,"v1":30,"seq":130}
            {"id":4,"__DORIS_DELETE_SIGN__":1,"seq":140}
        """.stripIndent().trim()
        streamLoad {
            table tableName
            set "format", "json"
            set "read_json_by_line", "true"
            set "strict_mode", "false"
            set "unique_key_update_mode", "UPDATE_FLEXIBLE_COLUMNS"
            inputStream new ByteArrayInputStream(flexibleRows.getBytes())
            time 20000
        }
        sql "sync"

        order_qt_flexible_visible """
            SELECT id, v1, v2, seq
            FROM ${tableName}
            ORDER BY id
        """

        qt_flexible_raw_binlog """
            SELECT __DORIS_BINLOG_OP__, id, v1, v2, seq,
                   __BEFORE__v1__, __BEFORE__v2__, __BEFORE__seq__
            FROM binlog("table" = "${tableName}")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """

        qt_flexible_detail """
            SELECT id, v1, v2, seq, __DORIS_BINLOG_OP__
            FROM ${tableName}@incr("incrementType" = "DETAIL")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__, __DORIS_BINLOG_OP__
        """

        order_qt_flexible_min_delta """
            SELECT id, v1, v2, seq, __DORIS_BINLOG_OP__
            FROM ${tableName}@incr("incrementType" = "MIN_DELTA")
            ORDER BY id, __DORIS_BINLOG_OP__
        """

        qt_flexible_append_only """
            SELECT id, v1, v2, seq, __DORIS_BINLOG_OP__
            FROM ${tableName}@incr("incrementType" = "APPEND_ONLY")
            ORDER BY __DORIS_BINLOG_TSO__, __DORIS_BINLOG_LSN__
        """
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
    }
}
