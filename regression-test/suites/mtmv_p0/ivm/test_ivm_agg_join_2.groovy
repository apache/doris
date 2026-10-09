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

import org.awaitility.Awaitility
import static java.util.concurrent.TimeUnit.SECONDS

suite("test_ivm_agg_join_2") {

    // =========================================================
    // Part 6: One-to-many join multiplicity in aggregation
    // =========================================================
    sql """drop materialized view if exists ivm_aj2_p6_mv;"""
    sql """drop table if exists ivm_aj2_p6_parent;"""
    sql """drop table if exists ivm_aj2_p6_child;"""

    sql """
        CREATE TABLE ivm_aj2_p6_parent (
            pid INT,
            pname VARCHAR(32)
        )
        UNIQUE KEY(pid)
        DISTRIBUTED BY HASH(pid) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj2_p6_child (
            cid INT,
            pid_ref INT,
            val INT
        )
        UNIQUE KEY(cid)
        DISTRIBUTED BY HASH(cid) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj2_p6_parent VALUES (1,'P1'),(2,'P2');"""
    sql """INSERT INTO ivm_aj2_p6_child VALUES (1,1,10),(2,1,20),(3,1,30),(4,2,40);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj2_p6_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj2_p6_parent.pname AS pname,
               COUNT(*) AS child_cnt,
               SUM(ivm_aj2_p6_child.val) AS child_total
        FROM ivm_aj2_p6_child
        INNER JOIN ivm_aj2_p6_parent
            ON ivm_aj2_p6_child.pid_ref = ivm_aj2_p6_parent.pid
        GROUP BY ivm_aj2_p6_parent.pname;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p6_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p6_mv")
    order_qt_p6_complete """SELECT pname, child_cnt, child_total FROM ivm_aj2_p6_mv"""

    sql """INSERT INTO ivm_aj2_p6_child VALUES (5,1,50);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p6_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p6_mv")
    order_qt_p6_incr """SELECT pname, child_cnt, child_total FROM ivm_aj2_p6_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p6_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p6_mv")
    order_qt_p6_complete2 """SELECT pname, child_cnt, child_total FROM ivm_aj2_p6_mv"""

    // =========================================================
    // Part 7: NULL values in join key and agg column
    // =========================================================
    sql """drop materialized view if exists ivm_aj2_p7_mv;"""
    sql """drop table if exists ivm_aj2_p7_t1;"""
    sql """drop table if exists ivm_aj2_p7_t2;"""

    sql """
        CREATE TABLE ivm_aj2_p7_t1 (
            k1 INT,
            jk INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj2_p7_t2 (
            k2 INT,
            jk2 INT,
            v2 INT
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // NULL in join key → excluded by inner join; NULL in v2 → SUM ignores
    sql """INSERT INTO ivm_aj2_p7_t1 VALUES (1,1,10),(2,2,20),(3,NULL,30);"""
    sql """INSERT INTO ivm_aj2_p7_t2 VALUES (1,1,100),(2,2,NULL),(3,NULL,300);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj2_p7_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj2_p7_t1.jk AS jk,
               COUNT(*) AS cnt,
               SUM(ivm_aj2_p7_t2.v2) AS total
        FROM ivm_aj2_p7_t1
        INNER JOIN ivm_aj2_p7_t2
            ON ivm_aj2_p7_t1.jk = ivm_aj2_p7_t2.jk2
        GROUP BY ivm_aj2_p7_t1.jk;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p7_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p7_mv")
    order_qt_p7_complete """SELECT jk, cnt, total FROM ivm_aj2_p7_mv"""

    sql """INSERT INTO ivm_aj2_p7_t1 VALUES (4,1,40);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p7_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p7_mv")
    order_qt_p7_incr """SELECT jk, cnt, total FROM ivm_aj2_p7_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p7_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p7_mv")
    order_qt_p7_complete2 """SELECT jk, cnt, total FROM ivm_aj2_p7_mv"""

    // =========================================================
    // Part 8: WHERE filter + expression in agg function
    // =========================================================
    sql """drop materialized view if exists ivm_aj2_p8_mv;"""
    sql """drop table if exists ivm_aj2_p8_t1;"""
    sql """drop table if exists ivm_aj2_p8_t2;"""

    sql """
        CREATE TABLE ivm_aj2_p8_t1 (
            k1 INT,
            grp VARCHAR(32),
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj2_p8_t2 (
            k2 INT,
            k1_ref INT,
            v2 INT
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj2_p8_t1 VALUES (1,'X',10),(2,'Y',20),(3,'X',3);"""
    sql """INSERT INTO ivm_aj2_p8_t2 VALUES (1,1,100),(2,2,200),(3,3,300);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj2_p8_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj2_p8_t1.grp AS grp,
               COUNT(*) AS cnt,
               SUM(ivm_aj2_p8_t1.v1 + ivm_aj2_p8_t2.v2) AS total
        FROM ivm_aj2_p8_t1
        INNER JOIN ivm_aj2_p8_t2
            ON ivm_aj2_p8_t1.k1 = ivm_aj2_p8_t2.k1_ref
        WHERE ivm_aj2_p8_t1.v1 > 5
        GROUP BY ivm_aj2_p8_t1.grp;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p8_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p8_mv")
    order_qt_p8_complete """SELECT grp, cnt, total FROM ivm_aj2_p8_mv"""

    sql """INSERT INTO ivm_aj2_p8_t2 VALUES (4,1,400);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p8_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p8_mv")
    order_qt_p8_incr """SELECT grp, cnt, total FROM ivm_aj2_p8_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p8_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p8_mv")
    order_qt_p8_complete2 """SELECT grp, cnt, total FROM ivm_aj2_p8_mv"""

    // =========================================================
    // Part 9: 3-table nested inner join with aggregation
    // =========================================================
    sql """drop materialized view if exists ivm_aj2_p9_mv;"""
    sql """drop table if exists ivm_aj2_p9_t1;"""
    sql """drop table if exists ivm_aj2_p9_t2;"""
    sql """drop table if exists ivm_aj2_p9_t3;"""

    sql """
        CREATE TABLE ivm_aj2_p9_t1 (
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj2_p9_t2 (
            k2 INT,
            k1_ref INT,
            v2 INT
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj2_p9_t3 (
            k3 INT,
            k2_ref INT,
            v3 INT
        )
        UNIQUE KEY(k3)
        DISTRIBUTED BY HASH(k3) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj2_p9_t1 VALUES (1,10),(2,20);"""
    sql """INSERT INTO ivm_aj2_p9_t2 VALUES (1,1,100),(2,1,200),(3,2,300);"""
    sql """INSERT INTO ivm_aj2_p9_t3 VALUES (1,1,1000),(2,2,2000),(3,3,3000);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj2_p9_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj2_p9_t1.k1 AS k1,
               SUM(ivm_aj2_p9_t3.v3) AS s3,
               COUNT(*) AS cnt
        FROM ivm_aj2_p9_t1
        INNER JOIN ivm_aj2_p9_t2
            ON ivm_aj2_p9_t1.k1 = ivm_aj2_p9_t2.k1_ref
        INNER JOIN ivm_aj2_p9_t3
            ON ivm_aj2_p9_t2.k2 = ivm_aj2_p9_t3.k2_ref
        GROUP BY ivm_aj2_p9_t1.k1;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p9_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p9_mv")
    order_qt_p9_complete """SELECT k1, s3, cnt FROM ivm_aj2_p9_mv"""

    // Insert into t3 to verify delta propagation through 3-table join
    sql """INSERT INTO ivm_aj2_p9_t3 VALUES (4,1,4000);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p9_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p9_mv")
    order_qt_p9_incr """SELECT k1, s3, cnt FROM ivm_aj2_p9_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p9_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p9_mv")
    order_qt_p9_complete2 """SELECT k1, s3, cnt FROM ivm_aj2_p9_mv"""

    // =========================================================
    // Part 10: Successive incremental updates to both tables
    // =========================================================
    sql """drop materialized view if exists ivm_aj2_p10_mv;"""
    sql """drop table if exists ivm_aj2_p10_t1;"""
    sql """drop table if exists ivm_aj2_p10_t2;"""

    sql """
        CREATE TABLE ivm_aj2_p10_t1 (
            k1 INT,
            grp VARCHAR(32),
            v1 INT
        )
        UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj2_p10_t2 (
            k2 INT,
            k1_ref INT,
            v2 INT
        )
        UNIQUE KEY(k2)
        DISTRIBUTED BY HASH(k2) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj2_p10_t1 VALUES (1,'X',10),(2,'Y',20);"""
    sql """INSERT INTO ivm_aj2_p10_t2 VALUES (1,1,100),(2,2,200);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj2_p10_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT ivm_aj2_p10_t1.grp AS grp,
               COUNT(*) AS cnt,
               SUM(ivm_aj2_p10_t1.v1 + ivm_aj2_p10_t2.v2) AS total
        FROM ivm_aj2_p10_t1
        INNER JOIN ivm_aj2_p10_t2
            ON ivm_aj2_p10_t1.k1 = ivm_aj2_p10_t2.k1_ref
        GROUP BY ivm_aj2_p10_t1.grp;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p10_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p10_mv")
    order_qt_p10_complete """SELECT grp, cnt, total FROM ivm_aj2_p10_mv"""

    // First incremental: insert into right table only
    sql """INSERT INTO ivm_aj2_p10_t2 VALUES (3,1,300);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p10_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p10_mv")
    order_qt_p10_incr1 """SELECT grp, cnt, total FROM ivm_aj2_p10_mv"""

    // Second incremental: insert into BOTH tables before one refresh (multi-bundle)
    sql """INSERT INTO ivm_aj2_p10_t1 VALUES (3,'X',30);"""
    sql """INSERT INTO ivm_aj2_p10_t2 VALUES (4,3,400);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p10_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p10_mv")
    order_qt_p10_incr2 """SELECT grp, cnt, total FROM ivm_aj2_p10_mv"""

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p10_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p10_mv")
    order_qt_p10_complete2 """SELECT grp, cnt, total FROM ivm_aj2_p10_mv"""

    // =========================================================
    // Part 11: Aggregate join with aliases and deltas from both inputs
    // =========================================================
    sql """drop materialized view if exists ivm_aj2_p11_mv;"""
    sql """drop table if exists ivm_aj2_p11_left;"""
    sql """drop table if exists ivm_aj2_p11_right;"""

    sql """
        CREATE TABLE ivm_aj2_p11_left (
            left_id INT,
            group_key VARCHAR(32),
            amount INT
        )
        UNIQUE KEY(left_id)
        DISTRIBUTED BY HASH(left_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ivm_aj2_p11_right (
            right_id INT,
            left_id_ref INT,
            bonus INT
        )
        UNIQUE KEY(right_id)
        DISTRIBUTED BY HASH(right_id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """INSERT INTO ivm_aj2_p11_left VALUES (1, 'A', 10);"""
    sql """INSERT INTO ivm_aj2_p11_right VALUES (1, 1, 100);"""

    sql """
        CREATE MATERIALIZED VIEW ivm_aj2_p11_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES ('replication_num' = '1')
        AS
        SELECT l.group_key AS group_key,
               COUNT(*) AS cnt,
               SUM(l.amount + r.bonus) AS total
        FROM (
            SELECT left_id, group_key, amount
            FROM ivm_aj2_p11_left
        ) l
        INNER JOIN (
            SELECT right_id, left_id_ref, bonus
            FROM ivm_aj2_p11_right
        ) r
            ON l.left_id = r.left_id_ref
        GROUP BY l.group_key;
    """

    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p11_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p11_mv")
    order_qt_p11_initial_incr """SELECT group_key, cnt, total FROM ivm_aj2_p11_mv"""

    // Both branches have pending deltas when the aggregate is rewritten.
    sql """INSERT INTO ivm_aj2_p11_left VALUES (2, 'A', 20);"""
    sql """INSERT INTO ivm_aj2_p11_right VALUES (2, 2, 200);"""
    sql """REFRESH MATERIALIZED VIEW ivm_aj2_p11_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("ivm_aj2_p11_mv")
    order_qt_p11_incr """SELECT group_key, cnt, total FROM ivm_aj2_p11_mv"""
}
