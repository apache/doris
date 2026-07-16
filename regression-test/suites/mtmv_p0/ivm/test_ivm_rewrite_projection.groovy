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

suite("test_ivm_rewrite_projection", "nonConcurrent") {
    sql """drop materialized view if exists rewrite_projection_ivm;"""
    sql """drop table if exists rewrite_projection_base;"""

    sql """
        CREATE TABLE rewrite_projection_base (
            id BIGINT NOT NULL,
            category VARCHAR(20) NOT NULL,
            amount DECIMAL(12, 2) NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """

    sql """
        INSERT INTO rewrite_projection_base VALUES
            (1, 'book', 10.00),
            (2, 'food', 20.00),
            (3, 'toy', 5.00);
    """

    sql """
        CREATE MATERIALIZED VIEW rewrite_projection_ivm
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1"
        )
        AS
        SELECT id, category, amount
        FROM rewrite_projection_base
        WHERE amount >= 10;
    """

    sql """REFRESH MATERIALIZED VIEW rewrite_projection_ivm COMPLETE"""
    waitingMTMVTaskFinishedByMvName("rewrite_projection_ivm")
    advance_ivm_stream_offset("rewrite_projection_ivm")

    mv_rewrite_success_without_check_chosen("""
        SELECT id, category, amount
        FROM rewrite_projection_base
        WHERE amount >= 10
        ORDER BY id
    """, "rewrite_projection_ivm")

    order_qt_rewrite_projection_base """
        SELECT id, category, amount
        FROM rewrite_projection_base
        WHERE amount >= 10
        ORDER BY id
    """

    order_qt_rewrite_projection_mv """
        SELECT id, category, amount
        FROM rewrite_projection_ivm
        ORDER BY id
    """
}
