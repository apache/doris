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

// Dedicated regression for the join types surfaced at the SQL level for SPM plan
// fixing (design: mark joins, LEFT NULL_AWARE ANTI):
//   - [LEFT|RIGHT] [SEMI|ANTI] MARK JOIN ... [MARK_CONDITION(<equality>)] MARK_SLOT <name>
//   - LEFT NULL_AWARE ANTI JOIN ...
//
// The mark slot is a three-valued boolean (true / false / null) output column that
// can be referenced by its MARK_SLOT name. Each query below is executed and the
// result is compared (order_qt), so both planning and execution semantics of the new
// join types are covered.
suite("test_join_new_types_p0", "p0") {

    String db = "test_join_new_types_p0"
    sql "DROP DATABASE IF EXISTS ${db}"
    sql "CREATE DATABASE ${db}"
    sql "USE ${db}"

    sql """CREATE TABLE t1 (a INT, b INT) DISTRIBUTED BY HASH(a) BUCKETS 3
          PROPERTIES ('replication_num' = '1')"""
    sql """CREATE TABLE t2 (c INT) DISTRIBUTED BY HASH(c) BUCKETS 3
          PROPERTIES ('replication_num' = '1')"""
    sql "INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (NULL, 40)"
    sql "INSERT INTO t2 VALUES (1), (3), (NULL), (4)"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"

    // ===== LEFT SEMI MARK JOIN (mark column = did the right side match) =====
    order_qt_left_semi_mark """
        SELECT a, b, m FROM t1 LEFT SEMI MARK JOIN t2 MARK_SLOT m ON t1.a = t2.c
        ORDER BY a NULLS LAST, b
    """

    // ===== RIGHT SEMI MARK JOIN (preserved side is the right input t1) =====
    order_qt_right_semi_mark """
        SELECT a, b, m FROM t2 RIGHT SEMI MARK JOIN t1 MARK_SLOT m ON t1.a = t2.c
        ORDER BY a NULLS LAST, b, m
    """

    // ===== LEFT ANTI MARK JOIN (mark column = NOT matched) =====
    order_qt_left_anti_mark """
        SELECT a, b, m FROM t1 LEFT ANTI MARK JOIN t2 MARK_SLOT m ON t1.a = t2.c
        ORDER BY a NULLS LAST, b
    """

    // ===== RIGHT ANTI MARK JOIN (preserved side is the right input t1) =====
    order_qt_right_anti_mark """
        SELECT a, b, m FROM t2 RIGHT ANTI MARK JOIN t1 MARK_SLOT m ON t1.a = t2.c
        ORDER BY a NULLS LAST, b, m
    """

    // ===== LEFT SEMI MARK JOIN with an explicit (equality) MARK_CONDITION =====
    order_qt_left_semi_mark_cond """
        SELECT a, b, m FROM t1 LEFT SEMI MARK JOIN t2
            MARK_CONDITION(t1.a = t2.c) MARK_SLOT m
            ON t1.a = t2.c
        ORDER BY a NULLS LAST, b
    """

    // ===== LEFT NULL_AWARE ANTI JOIN (three-valued NOT IN semantics; a NULL in the
    // build set makes NOT IN never TRUE, so the build side is filtered to non-null) =====
    order_qt_null_aware_anti """
        SELECT a, b FROM t1 LEFT NULL_AWARE ANTI JOIN
            (SELECT c FROM t2 WHERE c IS NOT NULL) t2n ON t1.a = t2n.c
        ORDER BY a NULLS LAST, b
    """

    // ===== CROSS MARK JOIN (a mark join with no hash/other/mark conjunct at all) =====
    // This is the shape the SPM decompiler emits for a folded uncorrelated EXISTS /
    // NOT EXISTS SELECT-list boolean (Q6/Q8 in the SPM repro doc): the mark join carries
    // only a mark slot, the right input is the reduced "exists" source and no ON clause
    // is allowed (CROSS join). The parser rebuilds a CROSS_JOIN node + mark slot.

    // Q6 shape: EXISTS (SELECT ...) - the right source always produces one row (t2 is
    // non-empty), so every left row carries mark = TRUE.
    order_qt_cross_mark_exists """
        SELECT a, b, ifnull(m, FALSE) AS n FROM t1
            CROSS MARK JOIN (SELECT 1 FROM t2 LIMIT 1) t2e MARK_SLOT m
        ORDER BY a NULLS LAST, b
    """

    // Q8 shape: NOT EXISTS (SELECT ...) - the right source is the count-based "empty"
    // probe (count(*) over t2 filtered to zero rows), which produces NO row when t2 is
    // non-empty, so every left row is preserved with mark = FALSE.
    order_qt_cross_mark_not_exists """
        SELECT a, b, ifnull(m, FALSE) AS n FROM t1
            CROSS MARK JOIN
                (SELECT * FROM (SELECT count(*) AS cnt FROM t2) t2c WHERE (cnt = 0)) t2n
                MARK_SLOT m
        ORDER BY a NULLS LAST, b
    """

    sql "DROP DATABASE IF EXISTS ${db}"
}
