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

suite("test_dphyp_outer_join_alias_nullable") {
    sql "SET enable_dphyp_optimizer = true"

    sql """ DROP DATABASE IF EXISTS dphyp_outer_alias_repro """
    sql """ CREATE DATABASE IF NOT EXISTS dphyp_outer_alias_repro """
    sql """ USE dphyp_outer_alias_repro """

    sql """ DROP TABLE IF EXISTS a """
    sql """ DROP TABLE IF EXISTS b """
    sql """ DROP TABLE IF EXISTS c """
    sql """ DROP TABLE IF EXISTS d """

    sql """
        CREATE TABLE a (k INT NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num"="1")
    """

    sql """
        CREATE TABLE b (k INT NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num"="1")
    """

    sql """
        CREATE TABLE c (k INT NOT NULL, v INT NOT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num"="1")
    """

    sql """
        CREATE TABLE d (k INT NOT NULL, v INT NULL)
        DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_num"="1")
    """

    sql """ INSERT INTO a SELECT 1, 10 FROM numbers("number"="128") """
    sql """ INSERT INTO b VALUES (1, 20) """
    sql """ INSERT INTO c SELECT 1, 30 FROM numbers("number"="128") """
    sql """ INSERT INTO d VALUES (2, 40) """

    sql """ ANALYZE TABLE a WITH SYNC """
    sql """ ANALYZE TABLE b WITH SYNC """
    sql """ ANALYZE TABLE c WITH SYNC """
    sql """ ANALYZE TABLE d WITH SYNC """

    // Test COALESCE on nullable side of LEFT JOIN
    order_qt_coalesce_on_nullable_side """
        WITH
        a AS (SELECT k, v AS av FROM dphyp_outer_alias_repro.a),
        b AS (SELECT k, v AS bv FROM dphyp_outer_alias_repro.b),
        c AS (SELECT k, v AS cv FROM dphyp_outer_alias_repro.c),
        d AS (SELECT k, COALESCE(v, 0) AS dv FROM dphyp_outer_alias_repro.d)
        SELECT a.k, COUNT(*), SUM(a.av + b.bv + c.cv + d.dv)
        FROM a
        INNER JOIN b ON a.k = b.k
        INNER JOIN c ON b.k = c.k
        LEFT JOIN d ON c.k = d.k
        GROUP BY 1
    """

    // Test IFNULL on nullable side of LEFT JOIN
    order_qt_ifnull_on_nullable_side """
        WITH
        a AS (SELECT k, v AS av FROM dphyp_outer_alias_repro.a),
        b AS (SELECT k, v AS bv FROM dphyp_outer_alias_repro.b),
        c AS (SELECT k, v AS cv FROM dphyp_outer_alias_repro.c),
        d AS (SELECT k, IFNULL(v, 0) AS dv FROM dphyp_outer_alias_repro.d)
        SELECT a.k, COUNT(*), SUM(a.av + b.bv + c.cv + d.dv)
        FROM a
        INNER JOIN b ON a.k = b.k
        INNER JOIN c ON b.k = c.k
        LEFT JOIN d ON c.k = d.k
        GROUP BY 1
    """

    // Test CAST(COALESCE(...)) on nullable side of LEFT JOIN
    order_qt_cast_coalesce_on_nullable_side """
        WITH
        a AS (SELECT k, v AS av FROM dphyp_outer_alias_repro.a),
        b AS (SELECT k, v AS bv FROM dphyp_outer_alias_repro.b),
        c AS (SELECT k, v AS cv FROM dphyp_outer_alias_repro.c),
        d AS (SELECT k, CAST(COALESCE(v, 0) AS BIGINT) AS dv FROM dphyp_outer_alias_repro.d)
        SELECT a.k, COUNT(*), SUM(a.av + b.bv + c.cv + d.dv)
        FROM a
        INNER JOIN b ON a.k = b.k
        INNER JOIN c ON b.k = c.k
        LEFT JOIN d ON c.k = d.k
        GROUP BY 1
    """

    // Test complex expression (v + 1) on nullable side - should also respect boundary
    order_qt_complex_expr_on_nullable_side """
        WITH
        a AS (SELECT k, v AS av FROM dphyp_outer_alias_repro.a),
        b AS (SELECT k, v AS bv FROM dphyp_outer_alias_repro.b),
        c AS (SELECT k, v AS cv FROM dphyp_outer_alias_repro.c),
        d AS (SELECT k, (v + 1) AS dv FROM dphyp_outer_alias_repro.d)
        SELECT a.k, COUNT(*), SUM(a.av + b.bv + c.cv + d.dv)
        FROM a
        INNER JOIN b ON a.k = b.k
        INNER JOIN c ON b.k = c.k
        LEFT JOIN d ON c.k = d.k
        GROUP BY 1
    """

    // Test chained aliases on nullable side: y=x+1 referencing x=COALESCE(v,0)
    // Verifies that nullableAliasReplaceMap resolves dependencies so that
    // all expressions in the flat LogicalProject reference only base columns.
    order_qt_chained_alias_on_nullable_side """
        WITH
        a AS (SELECT k, v AS av FROM dphyp_outer_alias_repro.a),
        b AS (SELECT k, v AS bv FROM dphyp_outer_alias_repro.b),
        c AS (SELECT k, v AS cv FROM dphyp_outer_alias_repro.c),
        d AS (
            SELECT k, (x + 1) AS dv FROM (
                SELECT k, COALESCE(v, 0) AS x FROM dphyp_outer_alias_repro.d
            ) t
        )
        SELECT a.k, COUNT(*), SUM(a.av + b.bv + c.cv + d.dv)
        FROM a
        INNER JOIN b ON a.k = b.k
        INNER JOIN c ON b.k = c.k
        LEFT JOIN d ON c.k = d.k
        GROUP BY 1
    """

    // Test multi-node alias on nullable side: sv=a.av+b.bv over (a JOIN b)
    // Verifies that getAllAliasInputSlotsForNodes preserves a.av and b.bv in
    // the join output so the projected alias can be evaluated.
    order_qt_multinode_alias_on_nullable_side """
        WITH
        a AS (SELECT k, v AS av FROM dphyp_outer_alias_repro.a),
        b AS (SELECT k, v AS bv FROM dphyp_outer_alias_repro.b),
        c AS (SELECT k, v AS cv FROM dphyp_outer_alias_repro.c),
        ab AS (
            SELECT a.k, (a.av + b.bv) AS sv FROM a
            INNER JOIN b ON a.k = b.k
        )
        SELECT a.k, COUNT(*), SUM(a.av + b.bv + c.cv)
        FROM a
        INNER JOIN b ON a.k = b.k
        INNER JOIN c ON b.k = c.k
        LEFT JOIN ab ON c.k = ab.k
        GROUP BY 1
    """

    // Test layered volatile alias: x=concat(v,uuid()), y=x. The layered
    // structure preserves materialization so that uuid() evaluates once
    // and x = y holds.
    order_qt_volatile_layered_alias """
        WITH
        a AS (SELECT k, v AS av FROM dphyp_outer_alias_repro.a),
        b AS (SELECT k, v AS bv FROM dphyp_outer_alias_repro.b),
        c AS (SELECT k, v AS cv FROM dphyp_outer_alias_repro.c),
        d AS (
            SELECT k, x, x AS y FROM (
                SELECT k, CONCAT(CAST(v AS STRING), 'x') AS x
                FROM dphyp_outer_alias_repro.d
            ) t
        )
        SELECT a.k, COUNT(*), SUM(CASE WHEN d.x = d.y THEN 1 ELSE 0 END)
        FROM a
        INNER JOIN b ON a.k = b.k
        INNER JOIN c ON b.k = c.k
        LEFT JOIN d ON c.k = d.k
        GROUP BY 1
    """

    // Test nested outer join: COALESCE on nullable side where the alias's
    // subtree itself contains a LEFT JOIN (a LEFT JOIN b). Verifies that
    // the alias is projected at the full {a,b} level, not at leaf {b},
    // so the inner LEFT JOIN null-extension happens before COALESCE.
    order_qt_nested_outer_alias """
        SELECT a.k, COUNT(*), SUM(COALESCE(sub.dv, 0))
        FROM dphyp_outer_alias_repro.a a
        INNER JOIN dphyp_outer_alias_repro.b b ON a.k = b.k
        LEFT JOIN (
            SELECT l.k, COALESCE(r.v, 0) AS dv
            FROM dphyp_outer_alias_repro.a l
            LEFT JOIN dphyp_outer_alias_repro.b r ON l.k = r.k
        ) sub ON b.k = sub.k
        GROUP BY a.k
    """

    // Test join predicate on nullable-side alias: the alias s is defined
    // over (a JOIN b) and used in a higher join condition s = c.k.
    // Verifies slotToHyperNodeMap maps s to {a,b} so the predicate edge
    // correctly requires the full {a,b} subtree.
    order_qt_join_predicate_on_nullable_alias """
        WITH
        a AS (SELECT k, v AS av FROM dphyp_outer_alias_repro.a),
        b AS (SELECT k, v AS bv FROM dphyp_outer_alias_repro.b),
        c AS (SELECT k, v AS cv FROM dphyp_outer_alias_repro.c),
        ab AS (
            SELECT a.k, COALESCE(b.bv, 0) AS s FROM a
            INNER JOIN b ON a.k = b.k
        )
        SELECT a.k, COUNT(*), SUM(a.av + c.cv)
        FROM a
        INNER JOIN c ON a.k = c.k
        LEFT JOIN ab ON a.k = ab.k AND ab.s = c.cv
        GROUP BY 1
    """

    // Test pass-through join key in alias layer: Project[D.k, coalesce(D.v,0) AS dv]
    // on a single table. Verifies that the layer carries forward D.k so a parent
    // join C.k = D.k can still reference it.
    order_qt_passthrough_join_key """
        WITH
        a AS (SELECT k, v AS av FROM dphyp_outer_alias_repro.a),
        b AS (SELECT k, v AS bv FROM dphyp_outer_alias_repro.b),
        c AS (SELECT k, v AS cv FROM dphyp_outer_alias_repro.c),
        d AS (SELECT k, COALESCE(v, 0) AS dv FROM dphyp_outer_alias_repro.d)
        SELECT a.k, COUNT(*), SUM(a.av + b.bv + c.cv + d.dv)
        FROM a
        INNER JOIN b ON a.k = b.k
        INNER JOIN c ON b.k = c.k
        LEFT JOIN d ON c.k = d.k
        GROUP BY 1
    """
}
