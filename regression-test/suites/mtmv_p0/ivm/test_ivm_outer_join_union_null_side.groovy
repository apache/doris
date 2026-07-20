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

suite("test_ivm_outer_join_union_null_side") {
    sql """drop materialized view if exists ivm_oj_un_mv;"""
    sql """drop table if exists ivm_oj_un_l;"""
    sql """drop table if exists ivm_oj_un_b;"""
    sql """drop table if exists ivm_oj_un_c;"""

    sql """
        CREATE TABLE ivm_oj_un_l (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_oj_un_b (
            k1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_oj_un_c (
            k1 INT,
            v2 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_oj_un_l VALUES (1, 10), (2, 20), (3, 30);"""
    sql """INSERT INTO ivm_oj_un_b VALUES (1, 100);"""
    sql """INSERT INTO ivm_oj_un_c VALUES (3, 300);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_oj_un_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT l.k1, l.v1, r.v2
        FROM ivm_oj_un_l l
        LEFT OUTER JOIN (
            SELECT k1, v2 FROM ivm_oj_un_b
            UNION ALL
            SELECT k1, v2 FROM ivm_oj_un_c
        ) r ON l.k1 = r.k1;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_oj_un_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_oj_un_mv")
    order_qt_after_complete """
        SELECT k1, v1, v2 FROM ivm_oj_un_mv ORDER BY k1, v1, v2
    """

    sql """INSERT INTO ivm_oj_un_b VALUES (2, 200);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj_un_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj_un_mv")
    order_qt_after_b_insert """
        SELECT k1, v1, v2 FROM ivm_oj_un_mv ORDER BY k1, v1, v2
    """

    sql """INSERT INTO ivm_oj_un_c VALUES (2, 201);"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj_un_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj_un_mv")
    order_qt_after_c_insert """
        SELECT k1, v1, v2 FROM ivm_oj_un_mv ORDER BY k1, v1, v2
    """

    sql """DELETE FROM ivm_oj_un_b WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj_un_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj_un_mv")
    order_qt_after_b_delete_with_c_match """
        SELECT k1, v1, v2 FROM ivm_oj_un_mv ORDER BY k1, v1, v2
    """

    sql """DELETE FROM ivm_oj_un_c WHERE k1 = 2;"""
    sql """REFRESH MATERIALIZED VIEW ivm_oj_un_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_oj_un_mv")
    order_qt_after_c_delete """
        SELECT k1, v1, v2 FROM ivm_oj_un_mv ORDER BY k1, v1, v2
    """
}
