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

suite("test_min_delta_op_filter_correctness", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    sql "DROP DATABASE IF EXISTS test_min_delta_op_filter_correctness_db"
    sql "CREATE DATABASE test_min_delta_op_filter_correctness_db"
    sql "USE test_min_delta_op_filter_correctness_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    try {
        sql "DROP STREAM IF EXISTS s"
        sql "DROP TABLE IF EXISTS src"
        sql "DROP TABLE IF EXISTS audit"

        // MoW UNIQUE KEY with a user-defined sequence column, consumed by a
        // MIN_DELTA stream. Consumption is materialized via INSERT INTO audit
        // SELECT ... FROM s, which commits and advances the stream offset.
        // A bare SELECT over the stream rolls back (does NOT advance) the
        // offset, so after a DELETE the two identical bare SELECTs must return
        // the same DELETE rows.
        sql """
            CREATE TABLE src (
                id BIGINT NOT NULL,
                version_no BIGINT NOT NULL,
                payload VARCHAR(32) NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "function_column.sequence_col" = "version_no",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """

        sql """
            CREATE TABLE audit (
                id BIGINT,
                version_no BIGINT,
                payload VARCHAR(32),
                change_type VARCHAR(32)
            ) ENGINE=OLAP
            DUPLICATE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1"
            )
        """

        sql """
            CREATE STREAM s ON TABLE src
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """

        // Round 1: three fresh inserts -> APPEND, consumed into audit.
        sql "INSERT INTO src VALUES (1, 1, 'a1'), (2, 1, 'a2'), (3, 1, 'a3')"
        sql "sync"

        order_qt_stream_append_filtered_1 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('APPEND')
            ORDER BY id
        """

        order_qt_stream_append_filtered_2 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            ORDER BY id
        """

        order_qt_stream_append_filtered_3 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('APPEND', 'UPDATE_BEFORE', 'UPDATE_AFTER', 'DELETE')
            ORDER BY id
        """

        sql """
            INSERT INTO audit
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('APPEND', 'INSERT', 'UPDATE_AFTER', 'DELETE')
        """

        order_qt_audit_round1 """
            SELECT change_type, count(*) FROM audit GROUP BY change_type ORDER BY change_type
        """

        // Round 2: bump the sequence column for the same keys -> UPDATE, only
        // the UPDATE_AFTER image survives the WHERE filter and is consumed.
        sql "INSERT INTO src VALUES (1, 2, 'b1'), (2, 2, 'b2'), (3, 2, 'b3')"
        sql "sync"

        order_qt_stream_update_filtered_1 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('UPDATE_BEFORE')
            ORDER BY id
        """

        order_qt_stream_update_filtered_2 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('UPDATE_AFTER')
            ORDER BY id
        """

        order_qt_stream_update_filtered_3 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('APPEND', 'UPDATE_BEFORE', 'UPDATE_AFTER', 'DELETE')
            ORDER BY id
        """

        order_qt_stream_update_filtered_4 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            ORDER BY id
        """

        sql """
            INSERT INTO audit
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('APPEND', 'UPDATE_BEFORE', 'UPDATE_AFTER', 'DELETE')
        """

        order_qt_audit_round2 """
            SELECT change_type, count(*) FROM audit GROUP BY change_type ORDER BY change_type
        """

        // Round 3: delete two keys, then read the stream twice with bare
        // SELECTs. Because bare SELECT rolls back the offset, both reads must
        // observe the same DELETE rows carrying the pre-delete snapshot.
        sql "DELETE FROM src WHERE id <= 2"
        sql "sync"

        order_qt_stream_delete_filtered_1 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            ORDER BY id
        """

        order_qt_stream_delete_filtered_2 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('DELETE')
            ORDER BY id
        """

        order_qt_stream_delete_filtered_3 """
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('APPEND', 'UPDATE_BEFORE', 'UPDATE_AFTER', 'DELETE')
            ORDER BY id
        """
        sql """
            INSERT INTO audit
            SELECT id, version_no, payload, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM s
            WHERE __DORIS_STREAM_CHANGE_TYPE_COL__ IN ('APPEND', 'UPDATE_BEFORE', 'UPDATE_AFTER', 'DELETE')
        """

        order_qt_audit_round3 """
            SELECT change_type, count(*) FROM audit GROUP BY change_type ORDER BY change_type
        """

    } finally {
        sql "DROP DATABASE IF EXISTS test_min_delta_op_filter_correctness_db"
    }
}
