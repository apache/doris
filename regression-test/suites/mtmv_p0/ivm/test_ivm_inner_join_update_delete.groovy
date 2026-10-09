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

suite("test_ivm_inner_join_update_delete") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"
    sql "SET enable_materialized_view_rewrite = false"
    sql "SET enable_nereids_timeout = false"

    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_inner_join_update_delete_mv"
    sql "DROP TABLE IF EXISTS ivm_inner_join_update_delete_fact"
    sql "DROP TABLE IF EXISTS ivm_inner_join_update_delete_dim"

    sql """
        CREATE TABLE ivm_inner_join_update_delete_dim (
            customer_id BIGINT NOT NULL,
            segment VARCHAR(32) NOT NULL,
            version_no INT NOT NULL
        )
        UNIQUE KEY(customer_id)
        DISTRIBUTED BY HASH(customer_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """
        CREATE TABLE ivm_inner_join_update_delete_fact (
            fact_id BIGINT NOT NULL,
            customer_id BIGINT NOT NULL,
            quantity BIGINT NOT NULL,
            amount DECIMAL(18, 2) NOT NULL
        )
        UNIQUE KEY(fact_id)
        DISTRIBUTED BY HASH(fact_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "binlog.enable" = "true",
            "binlog.format" = "ROW",
            "binlog.need_historical_value" = "true"
        )
    """

    sql """
        INSERT INTO ivm_inner_join_update_delete_dim VALUES
            (1, 'old', 1), (2, 'keep', 1)
    """
    sql """
        INSERT INTO ivm_inner_join_update_delete_fact VALUES
            (1, 1, 10, 100.00),
            (2, 1, 20, 200.00),
            (3, 2, 30, 300.00)
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_inner_join_update_delete_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        KEY(segment)
        DISTRIBUTED BY HASH(segment) BUCKETS 2
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT d.segment,
               COUNT(*) AS row_count,
               SUM(f.quantity) AS total_quantity,
               SUM(f.amount) AS total_amount
        FROM ivm_inner_join_update_delete_fact f
        INNER JOIN ivm_inner_join_update_delete_dim d
            ON f.customer_id = d.customer_id
        GROUP BY d.segment
    """

    sql "REFRESH MATERIALIZED VIEW ivm_inner_join_update_delete_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_inner_join_update_delete_mv")

    sql """
        UPDATE ivm_inner_join_update_delete_dim
        SET segment = 'new', version_no = 2
        WHERE customer_id = 1
    """
    sql """
        DELETE FROM ivm_inner_join_update_delete_dim
        WHERE customer_id = 1
    """

    sql "REFRESH MATERIALIZED VIEW ivm_inner_join_update_delete_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_inner_join_update_delete_mv")

    order_qt_ivm_inner_join_update_delete """
        SELECT segment, row_count, total_quantity, total_amount
        FROM ivm_inner_join_update_delete_mv
        ORDER BY segment
    """
}
