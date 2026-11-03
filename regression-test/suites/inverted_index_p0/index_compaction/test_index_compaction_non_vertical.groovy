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

suite("test_index_compaction_non_vertical", "nonConcurrent") {
    def isCloudMode = isCloudMode()

    def get_index_sizes = { String tableName ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        assertEquals(1, tablets.size())
        return sql_return_maparray("""
            select INDEX_DISK_SIZE, DATA_DISK_SIZE from information_schema.rowsets
            where TABLET_ID = ${tablets[0].TabletId} and ROWSET_NUM_ROWS > 0;
        """)
    }

    // The index file is sealed only after index compaction merged the string postings, so every
    // MATCH must still be answered by the index. With this variable off, a lost index is an error
    // rather than a silent fallback to reading the column.
    sql """ set enable_match_without_inverted_index = false """
    sql """ set enable_common_expr_pushdown = true """
    sql """ set enable_sql_cache = false """

    setBeConfigTemporary([
            "enable_vertical_compaction": "false",
            "inverted_index_compaction_enable": "true"
    ]) {
        // Only string indexes: before the fix the compound file was closed empty at flush time.
        sql """ DROP TABLE IF EXISTS test_index_compaction_non_vertical_string """
        sql """
            CREATE TABLE test_index_compaction_non_vertical_string (
                `ts` datetime NOT NULL,
                `trace_id` text NULL,
                `msg` text NULL,
                INDEX idx_trace_id (`trace_id`) USING INVERTED,
                INDEX idx_msg (`msg`) USING INVERTED PROPERTIES("parser" = "unicode", "support_phrase" = "true")
            ) ENGINE=OLAP
            DUPLICATE KEY(`ts`)
            DISTRIBUTED BY HASH(`ts`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V2"
            );
        """
        for (int i = 1; i <= 6; i++) {
            sql """ INSERT INTO test_index_compaction_non_vertical_string VALUES
                    ('2026-01-01 00:00:0${i}', 'trace-${i}', 'request number ${i} finished'); """
        }
        order_qt_string_before """ select * from test_index_compaction_non_vertical_string where trace_id match 'trace-3' """

        trigger_and_wait_compaction("test_index_compaction_non_vertical_string", "full")

        def stringRowsets = get_index_sizes("test_index_compaction_non_vertical_string")
        assertEquals(1, stringRowsets.size())
        assertTrue(stringRowsets[0].INDEX_DISK_SIZE as long > 0)

        order_qt_string_exact """ select * from test_index_compaction_non_vertical_string where trace_id match 'trace-3' """
        order_qt_string_token """ select * from test_index_compaction_non_vertical_string where msg match 'finished' """
        order_qt_string_equal """ select * from test_index_compaction_non_vertical_string where trace_id = 'trace-5' """

        // A numeric index is built while the segment is flushed and the string one is merged
        // afterwards. Before the fix the file was sealed with the numeric index alone, so its size
        // stayed above zero while the string index was gone.
        sql """ DROP TABLE IF EXISTS test_index_compaction_non_vertical_mixed """
        sql """
            CREATE TABLE test_index_compaction_non_vertical_mixed (
                `ts` datetime NOT NULL,
                `code` int NULL,
                `trace_id` text NULL,
                INDEX idx_code (`code`) USING INVERTED,
                INDEX idx_trace_id (`trace_id`) USING INVERTED
            ) ENGINE=OLAP
            DUPLICATE KEY(`ts`)
            DISTRIBUTED BY HASH(`ts`) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "inverted_index_storage_format" = "V2"
            );
        """
        for (int i = 1; i <= 6; i++) {
            sql """ INSERT INTO test_index_compaction_non_vertical_mixed VALUES
                    ('2026-01-01 00:00:0${i}', ${i}, 'trace-${i}'); """
        }

        trigger_and_wait_compaction("test_index_compaction_non_vertical_mixed", "full")

        def mixedRowsets = get_index_sizes("test_index_compaction_non_vertical_mixed")
        assertEquals(1, mixedRowsets.size())
        assertTrue(mixedRowsets[0].INDEX_DISK_SIZE as long > 0)

        order_qt_mixed_string """ select * from test_index_compaction_non_vertical_mixed where trace_id match 'trace-3' """
        order_qt_mixed_numeric """ select * from test_index_compaction_non_vertical_mixed where code = 4 """

        // The merged indexes must survive the next compaction of the same rowset.
        for (int i = 7; i <= 9; i++) {
            sql """ INSERT INTO test_index_compaction_non_vertical_mixed VALUES
                    ('2026-01-01 00:00:0${i}', ${i}, 'trace-${i}'); """
        }
        trigger_and_wait_compaction("test_index_compaction_non_vertical_mixed", "full")
        order_qt_mixed_second_round """ select * from test_index_compaction_non_vertical_mixed where trace_id match 'trace-8' """
    }
}
