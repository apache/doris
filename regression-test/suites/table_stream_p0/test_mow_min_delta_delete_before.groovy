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

suite("test_mow_min_delta_delete_before", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }
    sql "DROP DATABASE IF EXISTS test_mow_min_delta_delete_before_db"
    sql "CREATE DATABASE test_mow_min_delta_delete_before_db"
    sql "USE test_mow_min_delta_delete_before_db"
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    try {
        sql "DROP STREAM IF EXISTS mow_min_delta_bug_stream"
        sql "DROP TABLE IF EXISTS mow_min_delta_bug"

        // MoW UNIQUE KEY + MIN_DELTA: when an UPDATE and a DELETE on the same key
        // are folded into a single DELETE, the emitted DELETE row must carry the
        // pre-delete snapshot (the UPDATE_BEFORE old value), not the UPDATE_AFTER
        // new value nor the DELETE tombstone value.
        sql """
            CREATE TABLE mow_min_delta_bug (
                id BIGINT,
                v1 INT
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        // Seed old value before the stream starts so the in-window change is an
        // UPDATE (not a fresh APPEND).
        sql "INSERT INTO mow_min_delta_bug VALUES (1, 10)"
        sql """
            CREATE STREAM mow_min_delta_bug_stream
            ON TABLE mow_min_delta_bug
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "sync"
        sql "INSERT INTO mow_min_delta_bug VALUES (1, 11)"
        sql "DELETE FROM mow_min_delta_bug WHERE id = 1"
        sql "sync"
        sleep(1200)

        // Expect a single DELETE row keeping the old value 10.
        order_qt_mow_min_delta_delete_before """
            SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM mow_min_delta_bug_stream
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """

        sql "DROP STREAM IF EXISTS mow_min_delta_del_ins_del_stream"
        sql "DROP TABLE IF EXISTS mow_min_delta_del_ins_del"

        // MoW UNIQUE KEY + MIN_DELTA: when a DELETE, a re-INSERT and another DELETE
        // on the same key are folded into a single DELETE, the first op is a DELETE,
        // which means the key already existed before the window. The emitted DELETE
        // row must carry the first op's pre-delete snapshot (the original value 20),
        // not the re-inserted value 21.
        sql """
            CREATE TABLE mow_min_delta_del_ins_del (
                id BIGINT,
                v1 INT
            ) ENGINE=OLAP
            UNIQUE KEY(id)
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES (
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "binlog.enable" = "true",
                "binlog.format" = "ROW",
                "binlog.need_historical_value" = "true"
            )
        """
        // Seed old value before the stream starts so the first in-window DELETE
        // refers to a pre-existing row.
        sql "INSERT INTO mow_min_delta_del_ins_del VALUES (2, 20)"
        sql """
            CREATE STREAM mow_min_delta_del_ins_del_stream
            ON TABLE mow_min_delta_del_ins_del
            PROPERTIES (
                "type" = "min_delta",
                "show_initial_rows" = "false"
            )
        """
        sql "sync"
        sql "DELETE FROM mow_min_delta_del_ins_del WHERE id = 2"
        sql "INSERT INTO mow_min_delta_del_ins_del VALUES (2, 21)"
        sql "DELETE FROM mow_min_delta_del_ins_del WHERE id = 2"
        sql "sync"
        sleep(1200)

        // Expect a single DELETE row keeping the old value 20.
        order_qt_mow_min_delta_del_ins_del """
            SELECT id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
            FROM mow_min_delta_del_ins_del_stream
            ORDER BY id, v1, __DORIS_STREAM_CHANGE_TYPE_COL__
        """
    } finally {
        sql "DROP DATABASE IF EXISTS test_mow_min_delta_delete_before_db"
    }
}
