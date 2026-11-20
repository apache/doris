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

suite("test_olap_table_stream_snapshot_mow_predicate", "nonConcurrent") {
    if (isCloudMode()) {
        return
    }

    sql "DROP STREAM IF EXISTS snapshot_mow_predicate_stream"
    sql "DROP TABLE IF EXISTS snapshot_mow_predicate_base"
    sql """
        CREATE TABLE snapshot_mow_predicate_base (
            id INT NOT NULL,
            amount INT NOT NULL,
            enabled BOOLEAN NOT NULL
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "changelog.enable" = "true",
            "changelog.need_historical_value" = "true"
        )
    """
    sql "INSERT INTO snapshot_mow_predicate_base VALUES (1, 10, true), (2, 20, true), (3, 30, true)"
    sql "SYNC"
    sleep(1200)
    sql """
        CREATE STREAM snapshot_mow_predicate_stream ON TABLE snapshot_mow_predicate_base
        PROPERTIES ("type" = "append_only", "show_initial_rows" = "false")
    """

    // Newer data makes the snapshot rebuild the original three rows through a base-table wrapper.
    sql "INSERT INTO snapshot_mow_predicate_base VALUES (4, 40, true)"
    sql "SYNC"
    sleep(1200)
    sql "SET enable_no_need_read_data_opt = true"
    order_qt_base """
        SELECT id, amount FROM snapshot_mow_predicate_base WHERE enabled = true ORDER BY id
    """
    // The filter column must retain its real value if a residual predicate still needs it.
    order_qt_snapshot_filter """
        SELECT id, amount FROM snapshot_mow_predicate_stream@snapshot()
        WHERE enabled = true ORDER BY id
    """
    order_qt_snapshot_project_enabled """
        SELECT id, amount, enabled FROM snapshot_mow_predicate_stream@snapshot()
        WHERE enabled = true ORDER BY id
    """
    sql "SET enable_no_need_read_data_opt = false"
    order_qt_snapshot_without_skip_read """
        SELECT id, amount FROM snapshot_mow_predicate_stream@snapshot()
        WHERE enabled = true ORDER BY id
    """
}
