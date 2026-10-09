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

suite("test_ivm_union_delete_same_base") {
    sql """DROP MATERIALIZED VIEW IF EXISTS ivm_udsb_mv"""
    sql """DROP TABLE IF EXISTS ivm_udsb_events"""

    sql """
        CREATE TABLE ivm_udsb_events (
            id BIGINT NOT NULL,
            region VARCHAR(16),
            category VARCHAR(16),
            amount BIGINT NOT NULL,
            enabled BOOLEAN NOT NULL
        ) UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """
        INSERT INTO ivm_udsb_events VALUES
            (1, 'east', 'book', 10, true),
            (2, 'east', 'food', 20, true),
            (3, 'west', 'book', 30, true)
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_udsb_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT id, region, category, amount, 'all' AS arm_name
        FROM ivm_udsb_events
        WHERE enabled = true
        UNION ALL
        SELECT id, region, category, amount, 'large' AS arm_name
        FROM ivm_udsb_events
        WHERE enabled = true AND amount >= 30
    """

    sql """REFRESH MATERIALIZED VIEW ivm_udsb_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_udsb_mv")
    order_qt_union_delete_same_base_after_complete """
        SELECT id, region, category, amount, arm_name
        FROM ivm_udsb_mv
        ORDER BY id, arm_name
    """

    sql """DELETE FROM ivm_udsb_events WHERE id = 3"""
    sql """REFRESH MATERIALIZED VIEW ivm_udsb_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_udsb_mv")

    order_qt_union_delete_same_base_direct """
        SELECT id, region, category, amount, arm_name
        FROM (
            SELECT id, region, category, amount, 'all' AS arm_name
            FROM ivm_udsb_events
            WHERE enabled = true
            UNION ALL
            SELECT id, region, category, amount, 'large' AS arm_name
            FROM ivm_udsb_events
            WHERE enabled = true AND amount >= 30
        ) u
        ORDER BY id, arm_name
    """

    order_qt_union_delete_same_base_after_incremental """
        SELECT id, region, category, amount, arm_name
        FROM ivm_udsb_mv
        ORDER BY id, arm_name
    """
}
