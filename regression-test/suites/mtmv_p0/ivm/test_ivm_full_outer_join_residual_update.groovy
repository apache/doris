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

suite("test_ivm_full_outer_join_residual_update") {
    sql "SET disable_join_reorder = true"

    sql "DROP MATERIALIZED VIEW IF EXISTS ivm_27559_full_outer_join_mv"
    sql "DROP TABLE IF EXISTS ivm_27559_full_outer_join_l"
    sql "DROP TABLE IF EXISTS ivm_27559_full_outer_join_r"

    sql """
        CREATE TABLE ivm_27559_full_outer_join_l (
            id BIGINT NOT NULL,
            join_key INT,
            payload VARCHAR(32),
            score INT NOT NULL
        )
        UNIQUE KEY(id)
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
        CREATE TABLE ivm_27559_full_outer_join_r (
            id BIGINT NOT NULL,
            join_key INT,
            payload VARCHAR(32),
            score INT NOT NULL
        )
        UNIQUE KEY(id)
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
        INSERT INTO ivm_27559_full_outer_join_l VALUES
            (1, 10, 'l10', 20),
            (2, 20, 'l20', 5)
    """
    sql """
        INSERT INTO ivm_27559_full_outer_join_r VALUES
            (101, 10, 'r10', 10),
            (102, 20, 'r20', 10)
    """

    sql """
        CREATE MATERIALIZED VIEW ivm_27559_full_outer_join_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        PROPERTIES ("replication_num" = "1")
        AS
        SELECT
            l.id AS l_id,
            r.id AS r_id,
            l.score AS l_score,
            r.score AS r_score
        FROM ivm_27559_full_outer_join_l l
        FULL OUTER JOIN ivm_27559_full_outer_join_r r
            ON l.join_key = r.join_key AND l.score > r.score
    """

    sql "REFRESH MATERIALIZED VIEW ivm_27559_full_outer_join_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_27559_full_outer_join_mv")

    sql """
        INSERT INTO ivm_27559_full_outer_join_l VALUES
            (2, 20, 'l20-now-match', 15),
            (3, 30, 'l30', 50)
    """
    sql "INSERT INTO ivm_27559_full_outer_join_r VALUES (103, 30, 'r30', 40)"

    sql "REFRESH MATERIALIZED VIEW ivm_27559_full_outer_join_mv INCREMENTAL"
    waitingMTMVTaskFinishedByMvName("ivm_27559_full_outer_join_mv")

    order_qt_ivm_full_outer_join_residual_update """
        SELECT l_id, r_id, l_score, r_score
        FROM ivm_27559_full_outer_join_mv
        ORDER BY r_id, l_id
    """
}
