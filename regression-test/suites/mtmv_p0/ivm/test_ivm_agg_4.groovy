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

suite("test_ivm_agg_4") {

    // =========================================================
    // Part 15: Expressions in agg arguments — SUM(v1 + v2), MIN(v1 * 2)
    //          IVM supports complex expressions inside agg functions.
    //          Verify that INCREMENTAL refresh produces correct results
    //          for SUM(expr), MIN(expr), MAX(expr), COUNT(expr), AVG(expr).
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_4_expr_mv;"""
    sql """drop table if exists test_ivm_agg_4_expr_base;"""

    sql """
        CREATE TABLE test_ivm_agg_4_expr_base (
            k1 INT,
            grp INT,
            v1 INT,
            v2 INT
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

    // Initial data
    sql """
        INSERT INTO test_ivm_agg_4_expr_base VALUES
            (1, 1, 10, 20),
            (2, 1, 30, 40),
            (3, 2, 50, 60);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_4_expr_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT grp,
                  SUM(v1 + v2) AS sum_add,
                  MIN(v1 * 2) AS min_double,
                  MAX(v1 + v2) AS max_add,
                  COUNT(v1 + v2) AS cnt_add,
                  AVG(v1 + v2) AS avg_add,
                  COUNT(*) AS cnt
           FROM test_ivm_agg_4_expr_base
           GROUP BY grp;
    """

    // Initial INCREMENTAL refresh establishes the stream offset.
    // grp=1: sum_add=SUM(30+70)=100, min_double=MIN(20,60)=20, max_add=MAX(30,70)=70,
    // cnt_add=2, avg_add=50, cnt=2
    // grp=2: sum_add=110, min_double=100, max_add=110, cnt_add=1, avg_add=110, cnt=1
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_expr_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_expr_mv")

    order_qt_expr_complete """
        SELECT grp, sum_add, min_double, max_add, cnt_add, avg_add, cnt
        FROM test_ivm_agg_4_expr_mv ORDER BY grp
    """

    // Insert rows and trigger INCREMENTAL
    sql """INSERT INTO test_ivm_agg_4_expr_base VALUES (4, 1, 100, 200);"""
    sql """INSERT INTO test_ivm_agg_4_expr_base VALUES (5, 2, 5, 1);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_expr_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_expr_mv")

    // After INCREMENTAL:
    // grp=1: +row(100,200): sum_add=100+300=400, min_double=MIN(20,60,200)=20, max_add=MAX(30,70,300)=300,
    //        cnt_add=3, avg_add=400/3≈133.333, cnt=3
    // grp=2: +row(5,1): sum_add=110+6=116, min_double=MIN(100,10)=10, max_add=MAX(110,6)=110,
    //        cnt_add=2, avg_add=116/2=58, cnt=2
    order_qt_expr_after_incr """
        SELECT grp, sum_add, min_double, max_add, cnt_add, avg_add, cnt
        FROM test_ivm_agg_4_expr_mv ORDER BY grp
    """

    // COMPLETE ground truth
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_expr_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_expr_mv")

    order_qt_expr_complete_verify """
        SELECT grp, sum_add, min_double, max_add, cnt_add, avg_add, cnt
        FROM test_ivm_agg_4_expr_mv ORDER BY grp
    """

    // Delete a row (non-boundary for MIN/MAX) and verify INCREMENTAL
    // Delete row k1=2 (grp=1, v1=30, v2=40):
    //   v1*2=60 → current MIN is 20, so 60 > 20 → NOT min boundary
    //   v1+v2=70 → current MAX is 300, so 70 < 300 → NOT max boundary
    sql """DELETE FROM test_ivm_agg_4_expr_base WHERE k1 = 2;"""
    // Dirty partition
    sql """INSERT INTO test_ivm_agg_4_expr_base VALUES (6, 2, 7, 3);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_expr_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_expr_mv")

    // After delete+insert INCREMENTAL:
    // grp=1: removed row(30,40): sum_add=400-70=330, min_double=MIN(20,200)=20, max_add=MAX(30,300)=300,
    //        cnt_add=2, avg_add=330/2=165, cnt=2
    // grp=2: +row(7,3): sum_add=116+10=126, min_double=MIN(10,100,14)=10, max_add=MAX(110,6,10)=110,
    //        cnt_add=3, avg_add=126/3=42, cnt=3
    order_qt_expr_after_delete """
        SELECT grp, sum_add, min_double, max_add, cnt_add, avg_add, cnt
        FROM test_ivm_agg_4_expr_mv ORDER BY grp
    """

    // COMPLETE ground truth after delete
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_expr_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_expr_mv")

    order_qt_expr_complete_after_delete """
        SELECT grp, sum_add, min_double, max_add, cnt_add, avg_add, cnt
        FROM test_ivm_agg_4_expr_mv ORDER BY grp
    """

    // =========================================================
    // Part 16: NULL group keys — single nullable group key
    //          Verifies null-safe row_id hash: groups with NULL key
    //          must not collide with non-NULL key groups.
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_4_nullkey1_mv;"""
    sql """drop table if exists test_ivm_agg_4_nullkey1_base;"""

    sql """
        CREATE TABLE test_ivm_agg_4_nullkey1_base (
            id INT,
            k1 INT,
            v1 INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Initial data: 3 groups — k1=NULL, k1=1, k1=2
    sql """
        INSERT INTO test_ivm_agg_4_nullkey1_base VALUES
            (1, NULL, 10),
            (2, NULL, 20),
            (3, 1, 30),
            (4, 2, 40);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_4_nullkey1_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM test_ivm_agg_4_nullkey1_base
           GROUP BY k1;
    """

    // Initial INCREMENTAL refresh establishes the stream offset.
    // k1=NULL: cnt=2, sum=30; k1=1: cnt=1, sum=30; k1=2: cnt=1, sum=40
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey1_mv")

    order_qt_nullkey1_complete """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullkey1_mv ORDER BY k1
    """

    // Insert: add to NULL group and k1=1 group
    sql """INSERT INTO test_ivm_agg_4_nullkey1_base VALUES (5, NULL, 50);"""
    sql """INSERT INTO test_ivm_agg_4_nullkey1_base VALUES (6, 1, 60);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey1_mv")

    // After INCREMENTAL — verify via query
    order_qt_nullkey1_after_incr """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullkey1_mv ORDER BY k1
    """

    // COMPLETE to get ground truth
    // k1=NULL: ids 1(10),2(20),5(50) → cnt=3, sum=80
    // k1=1: ids 3(30),6(60) → cnt=2, sum=90
    // k1=2: id 4(40) → cnt=1, sum=40
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey1_mv")

    order_qt_nullkey1_complete_verify """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullkey1_mv ORDER BY k1
    """

    // Delete from k1=1 group via binlog_op=1
    sql """DELETE FROM test_ivm_agg_4_nullkey1_base WHERE k1 = 1;"""
    // Dirty partition with another insert
    sql """INSERT INTO test_ivm_agg_4_nullkey1_base VALUES (7, 2, 70);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey1_mv")

    order_qt_nullkey1_after_delete_incr """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullkey1_mv ORDER BY k1
    """

    // COMPLETE ground truth after delete
    // Current base-table snapshot excludes deleted k1=1 rows.
    // k1=NULL: ids 1(10),2(20),5(50) → cnt=3, sum=80
    // k1=2: ids 4(40),7(70) → cnt=2, sum=110
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey1_mv")

    order_qt_nullkey1_complete_after_delete """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullkey1_mv ORDER BY k1
    """

    // Delete from NULL group via binlog_op=1 and verify NULL-key group updates correctly.
    sql """DELETE FROM test_ivm_agg_4_nullkey1_base WHERE id = 1;"""
    // Dirty partition and resurrect k1=1 with a fresh row.
    sql """INSERT INTO test_ivm_agg_4_nullkey1_base VALUES (8, 1, 80);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey1_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey1_mv")

    order_qt_nullkey1_after_null_delete_incr """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullkey1_mv ORDER BY k1
    """

    // COMPLETE ground truth after deleting one NULL-key row and inserting k1=1.
    // k1=NULL: ids 2(20),5(50) → cnt=2, sum=70
    // k1=1: id 8(80) → cnt=1, sum=80
    // k1=2: ids 4(40),7(70) → cnt=2, sum=110
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey1_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey1_mv")

    order_qt_nullkey1_complete_after_null_delete """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullkey1_mv ORDER BY k1
    """

    // =========================================================
    // Part 17: NULL group keys — multiple nullable group keys
    //          Verifies (NULL,'x'), ('x',NULL), (NULL,NULL) produce
    //          distinct row_ids and don't collide.
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_4_nullkey2_mv;"""
    sql """drop table if exists test_ivm_agg_4_nullkey2_base;"""

    sql """
        CREATE TABLE test_ivm_agg_4_nullkey2_base (
            id INT,
            k1 VARCHAR(32),
            k2 VARCHAR(32),
            v1 INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Groups:
    // (NULL, 'x'):   ids 1,2 → v1=10,20
    // ('x', NULL):   id 3    → v1=30
    // (NULL, NULL):  id 4    → v1=40
    // ('a', 'b'):    id 5    → v1=50
    sql """
        INSERT INTO test_ivm_agg_4_nullkey2_base VALUES
            (1, NULL, 'x', 10),
            (2, NULL, 'x', 20),
            (3, 'x', NULL, 30),
            (4, NULL, NULL, 40),
            (5, 'a', 'b', 50);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_4_nullkey2_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, k2, COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM test_ivm_agg_4_nullkey2_base
           GROUP BY k1, k2;
    """

    // Initial INCREMENTAL refresh establishes the stream offset.
    // (NULL,'x'): cnt=2, sum=30
    // ('x',NULL): cnt=1, sum=30
    // (NULL,NULL): cnt=1, sum=40
    // ('a','b'): cnt=1, sum=50
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey2_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey2_mv")

    order_qt_nullkey2_complete """
        SELECT k1, k2, cnt, sum_v1 FROM test_ivm_agg_4_nullkey2_mv ORDER BY k1, k2
    """

    // Insert into various NULL groups to test incremental correctness
    sql """INSERT INTO test_ivm_agg_4_nullkey2_base VALUES (6, NULL, 'x', 60);"""
    sql """INSERT INTO test_ivm_agg_4_nullkey2_base VALUES (7, 'x', NULL, 70);"""
    sql """INSERT INTO test_ivm_agg_4_nullkey2_base VALUES (8, NULL, NULL, 80);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey2_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey2_mv")

    order_qt_nullkey2_after_incr """
        SELECT k1, k2, cnt, sum_v1 FROM test_ivm_agg_4_nullkey2_mv ORDER BY k1, k2
    """

    // COMPLETE ground truth
    // (NULL,'x'): ids 1(10),2(20),6(60) → cnt=3, sum=90
    // ('x',NULL): ids 3(30),7(70) → cnt=2, sum=100
    // (NULL,NULL): ids 4(40),8(80) → cnt=2, sum=120
    // ('a','b'): id 5(50) → cnt=1, sum=50
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullkey2_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullkey2_mv")

    order_qt_nullkey2_complete_verify """
        SELECT k1, k2, cnt, sum_v1 FROM test_ivm_agg_4_nullkey2_mv ORDER BY k1, k2
    """

    // =========================================================
    // Part 18: NULL group keys — empty-string vs NULL distinction
    //          Verifies that k1='' and k1=NULL are distinct groups.
    //          This is important because the ifnull(cast(k AS VARCHAR), '')
    //          maps NULL to '', so the isnull flag must distinguish them.
    // =========================================================

    sql """drop materialized view if exists test_ivm_agg_4_nullempty_mv;"""
    sql """drop table if exists test_ivm_agg_4_nullempty_base;"""

    sql """
        CREATE TABLE test_ivm_agg_4_nullempty_base (
            id INT,
            k1 VARCHAR(32),
            v1 INT
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 2
        PROPERTIES (
            "replication_num" = "1",
            "binlog.enable" = "true",
            "binlog.format" = "ROW", "binlog.need_historical_value" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    // Groups: k1=NULL, k1='', k1='a'
    sql """
        INSERT INTO test_ivm_agg_4_nullempty_base VALUES
            (1, NULL, 10),
            (2, '', 20),
            (3, 'a', 30);
    """

    sql """
        CREATE MATERIALIZED VIEW test_ivm_agg_4_nullempty_mv
        BUILD DEFERRED REFRESH INCREMENTAL ON MANUAL
        DISTRIBUTED BY RANDOM BUCKETS 2
        PROPERTIES (
            'replication_num' = '1'
        )
        AS SELECT k1, COUNT(*) AS cnt, SUM(v1) AS sum_v1
           FROM test_ivm_agg_4_nullempty_base
           GROUP BY k1;
    """

    // Initial INCREMENTAL establishes the stream offset.
    // NULL→cnt=1,sum=10; ''→cnt=1,sum=20; 'a'→cnt=1,sum=30
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullempty_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullempty_mv")

    order_qt_nullempty_complete """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullempty_mv ORDER BY k1
    """

    // Insert into each group
    sql """INSERT INTO test_ivm_agg_4_nullempty_base VALUES (4, NULL, 40);"""
    sql """INSERT INTO test_ivm_agg_4_nullempty_base VALUES (5, '', 50);"""
    sql """INSERT INTO test_ivm_agg_4_nullempty_base VALUES (6, 'a', 60);"""

    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullempty_mv INCREMENTAL"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullempty_mv")

    order_qt_nullempty_after_incr """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullempty_mv ORDER BY k1
    """

    // COMPLETE ground truth
    // NULL: ids 1(10),4(40) → cnt=2, sum=50
    // '': ids 2(20),5(50) → cnt=2, sum=70
    // 'a': ids 3(30),6(60) → cnt=2, sum=90
    sql """REFRESH MATERIALIZED VIEW test_ivm_agg_4_nullempty_mv COMPLETE"""
    waitingMTMVTaskFinishedByMvName("test_ivm_agg_4_nullempty_mv")

    order_qt_nullempty_complete_verify """
        SELECT k1, cnt, sum_v1 FROM test_ivm_agg_4_nullempty_mv ORDER BY k1
    """
}
