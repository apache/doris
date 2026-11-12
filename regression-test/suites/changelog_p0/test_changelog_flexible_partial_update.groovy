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

suite("test_changelog_flexible_partial_update") {
    def tableName = "test_changelog_flexible_partial_update"
    sql "DROP TABLE IF EXISTS ${tableName} FORCE"
    try {
        sql """
            CREATE TABLE ${tableName} (
                id INT,
                v1 INT NOT NULL DEFAULT "0",
                v2 VARCHAR(16) NULL DEFAULT "default",
                v3 INT NULL,
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
                "changelog.enable" = "true",
                "changelog.need_historical_value" = "true"
            )
        """

        sql """
            INSERT INTO ${tableName} VALUES
                (1, 10, 'a', 101, 100),
                (2, 20, 'b', 102, 100),
                (4, 40, 'd', 104, 100),
                (5, 50, 'e', 105, 500),
                (6, 60, 'f', 106, 500),
                (7, 70, 'g', 107, 100)
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

        // A single lower-sequence update loses to the visible row. It must neither change the
        // base table nor produce a visible changelog entry.
        streamLoad {
            table tableName
            set "format", "json"
            set "read_json_by_line", "true"
            set "strict_mode", "false"
            set "unique_key_update_mode", "UPDATE_FLEXIBLE_COLUMNS"
            inputStream new ByteArrayInputStream(
                    '{"id":5,"v1":5000,"seq":400}'.getBytes())
            time 20000
        }

        // The same rule applies to a lower-sequence delete.
        streamLoad {
            table tableName
            set "format", "json"
            set "read_json_by_line", "true"
            set "strict_mode", "false"
            set "unique_key_update_mode", "UPDATE_FLEXIBLE_COLUMNS"
            inputStream new ByteArrayInputStream(
                    '{"id":6,"__DORIS_DELETE_SIGN__":1,"seq":400}'.getBytes())
            time 20000
        }

        // DELETE followed by INSERT for an existing key in one load keeps the old row as BEFORE,
        // but omitted AFTER columns use default/null instead of resurrecting historical values.
        def deleteThenInsertRows = """
            {"id":7,"__DORIS_DELETE_SIGN__":1,"seq":150}
            {"id":7,"seq":160}
        """.stripIndent().trim()
        streamLoad {
            table tableName
            set "format", "json"
            set "read_json_by_line", "true"
            set "strict_mode", "false"
            set "unique_key_update_mode", "UPDATE_FLEXIBLE_COLUMNS"
            inputStream new ByteArrayInputStream(deleteThenInsertRows.getBytes())
            time 20000
        }
        sql "sync"

        order_qt_flexible_rejected_sequence_visible """
            SELECT id, v1, v2, v3, seq
            FROM ${tableName}
            WHERE id IN (5, 6)
            ORDER BY id
        """

        qt_flexible_rejected_sequence_changelog """
            SELECT id, v1, v2, v3, seq, __DORIS_CHANGELOG_OP__
            FROM changelog("table" = "${tableName}")
            WHERE id IN (5, 6)
            ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
        """

        order_qt_flexible_delete_then_insert_visible """
            SELECT id, v1, v2, v3, seq
            FROM ${tableName}
            WHERE id = 7
        """

        qt_flexible_delete_then_insert_changelog """
            SELECT __DORIS_CHANGELOG_OP__, id, v1, v2, v3, seq,
                   __DORIS_CHANGELOG_BEFORE_v1__, __DORIS_CHANGELOG_BEFORE_v2__, __DORIS_CHANGELOG_BEFORE_v3__, __DORIS_CHANGELOG_BEFORE_seq__
            FROM changelog("table" = "${tableName}")
            WHERE id = 7
            ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
        """

        order_qt_flexible_visible """
            SELECT id, v1, v2, seq
            FROM ${tableName}
            ORDER BY id
        """

        qt_flexible_raw_changelog """
            SELECT __DORIS_CHANGELOG_OP__, id, v1, v2, seq,
                   __DORIS_CHANGELOG_BEFORE_v1__, __DORIS_CHANGELOG_BEFORE_v2__, __DORIS_CHANGELOG_BEFORE_seq__
            FROM changelog("table" = "${tableName}")
            ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
        """

        if (!isCloudMode()) {
            qt_flexible_detail """
                SELECT id, v1, v2, seq, __DORIS_CHANGELOG_OP__
                FROM ${tableName}@incr("incrementType" = "DETAIL")
                ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__, __DORIS_CHANGELOG_OP__
            """

            order_qt_flexible_min_delta """
                SELECT id, v1, v2, seq, __DORIS_CHANGELOG_OP__
                FROM ${tableName}@incr("incrementType" = "MIN_DELTA")
                ORDER BY id, __DORIS_CHANGELOG_OP__
            """

            qt_flexible_append_only """
                SELECT id, v1, v2, seq, __DORIS_CHANGELOG_OP__
                FROM ${tableName}@incr("incrementType" = "APPEND_ONLY")
                ORDER BY __DORIS_CHANGELOG_TSO__, __DORIS_CHANGELOG_LSN__
            """
        }
    } finally {
        sql "DROP TABLE IF EXISTS ${tableName} FORCE"
    }
}
