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

suite("test_ivm_agg_join_3") {

    // =========================================================
    // Part 11: Root agg over DUP x MOW join with delete on MOW side
    // =========================================================
    sql """drop materialized view if exists ivm_aj3_p11_mv;"""
    sql """drop table if exists ivm_aj3_p11_fact;"""
    sql """drop table if exists ivm_aj3_p11_dim;"""

    sql """
        CREATE TABLE ivm_aj3_p11_fact (
            fact_id INT,
            dim_id INT,
            amount INT
        )
        DUPLICATE KEY(fact_id)
        DISTRIBUTED BY HASH(fact_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW"
        );
    """

    sql """
        CREATE TABLE ivm_aj3_p11_dim (
            dim_id INT,
            category VARCHAR(32)
        )
        UNIQUE KEY(dim_id)
        DISTRIBUTED BY HASH(dim_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj3_p11_fact VALUES (1,1,10),(2,1,20);"""
    sql """INSERT INTO ivm_aj3_p11_dim VALUES (1,'A');"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj3_p11_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj3_p11_dim.category AS category,
               COUNT(*) AS fact_cnt,
               SUM(ivm_aj3_p11_fact.amount) AS total_amount
        FROM ivm_aj3_p11_fact
        INNER JOIN ivm_aj3_p11_dim
            ON ivm_aj3_p11_fact.dim_id = ivm_aj3_p11_dim.dim_id
        GROUP BY ivm_aj3_p11_dim.category;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj3_p11_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj3_p11_mv")
    order_qt_p11_complete """SELECT category, fact_cnt, total_amount FROM ivm_aj3_p11_mv"""

    // Mock binlog scans the full base table as delta. Keep the joined key as delete and add a
    // non-joined insert row to trigger incremental refresh.
    sql """INSERT INTO ivm_aj3_p11_dim VALUES (3,'B');"""
    sql """DELETE FROM ivm_aj3_p11_dim WHERE dim_id = 1;"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj3_p11_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj3_p11_mv")
    // This output follows the current mock binlog behavior. Refresh it with real binlog data after
    // delete/retract support is available.
    order_qt_p11_after_unique_delete """SELECT category, fact_cnt, total_amount FROM ivm_aj3_p11_mv"""
}
