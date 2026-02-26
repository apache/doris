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

suite("test_match_with_or_on_alias", "p0") {
    // Test for: MATCH expressions on alias columns (from CTE/subquery) combined with OR
    // Bug: When an alias wraps a non-SlotReference expression (e.g., Cast(ElementAt(...))),
    //       Alias.toSlot() lost column metadata (originalColumn, originalTable, subPath),
    //       causing ExpressionTranslator.visitMatch() to crash with:
    //       "SlotReference in Match failed to get Column"
    //
    // This only manifested when MATCH was inside an OR predicate, because:
    // - AND-only: MATCH is pushed through the project and slot is correctly replaced
    // - OR: MATCH stays above the project as part of the OR, referencing the alias slot

    def tblVariant = "test_match_or_alias_variant"
    def tblNormal = "test_match_or_alias_normal"
    def tblAssoc = "test_match_or_alias_assoc"

    sql "DROP TABLE IF EXISTS ${tblVariant}"
    sql "DROP TABLE IF EXISTS ${tblNormal}"
    sql "DROP TABLE IF EXISTS ${tblAssoc}"

    // Table with variant column + inverted index
    sql """
        CREATE TABLE ${tblVariant} (
            id INT,
            OBJECTID INT,
            OVERFLOWPROPERTIES VARIANT,
            INDEX idx_var (OVERFLOWPROPERTIES) USING INVERTED PROPERTIES("parser"="standard")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    // Table with normal string column + inverted index
    sql """
        CREATE TABLE ${tblNormal} (
            id INT,
            name VARCHAR(100),
            category INT,
            INDEX idx_name (name) USING INVERTED PROPERTIES("parser"="standard")
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    // Association table for joins
    sql """
        CREATE TABLE ${tblAssoc} (
            from_id INT,
            to_id INT
        ) ENGINE=OLAP
        DUPLICATE KEY(from_id)
        DISTRIBUTED BY HASH(from_id) BUCKETS 1
        PROPERTIES('replication_num'='1')
    """

    sql """INSERT INTO ${tblVariant} VALUES
        (1, 100, '{"firstname": "hello world"}'),
        (2, 200, '{"firstname": "foo bar"}'),
        (3, 300, '{"firstname": "hello foo"}')"""

    sql """INSERT INTO ${tblNormal} VALUES
        (1, 'hello world', 1),
        (2, 'foo bar', 2),
        (3, 'hello foo', 1)"""

    sql """INSERT INTO ${tblAssoc} VALUES (1, 1), (2, 3)"""

    sql "sync"

    // =============================
    // Variant subcolumn + CTE + OR
    // =============================

    // Case 1: variant subcolumn + CTE + MATCH_ALL + OR (was crashing)
    qt_variant_cte_or """
        WITH cte AS (
            SELECT id, OBJECTID,
                   cast(OVERFLOWPROPERTIES['firstname'] as VARCHAR) as firstname
            FROM ${tblVariant}
        )
        SELECT cte.id, cte.OBJECTID, cte.firstname FROM cte
        LEFT JOIN ${tblAssoc} a ON cte.id = a.to_id
        WHERE (cte.firstname MATCH_ALL 'hello' AND a.to_id IS NOT NULL)
           OR cte.OBJECTID > 200
        ORDER BY cte.id
    """

    // Case 2: variant subcolumn + subquery + MATCH_ALL + OR (same bug, no CTE)
    qt_variant_subquery_or """
        SELECT cte.id, cte.OBJECTID, cte.firstname FROM (
            SELECT id, OBJECTID,
                   cast(OVERFLOWPROPERTIES['firstname'] as VARCHAR) as firstname
            FROM ${tblVariant}
        ) cte
        LEFT JOIN ${tblAssoc} a ON cte.id = a.to_id
        WHERE (cte.firstname MATCH_ALL 'hello' AND a.to_id IS NOT NULL)
           OR cte.OBJECTID > 200
        ORDER BY cte.id
    """

    // Case 3: variant subcolumn + CTE + MATCH_ANY + OR
    qt_variant_cte_match_any_or """
        WITH cte AS (
            SELECT id, OBJECTID,
                   cast(OVERFLOWPROPERTIES['firstname'] as VARCHAR) as firstname
            FROM ${tblVariant}
        )
        SELECT cte.id, cte.OBJECTID, cte.firstname FROM cte
        LEFT JOIN ${tblAssoc} a ON cte.id = a.to_id
        WHERE (cte.firstname MATCH_ANY 'hello foo' AND a.to_id IS NOT NULL)
           OR cte.OBJECTID > 200
        ORDER BY cte.id
    """

    // Case 4: variant subcolumn + CTE + MATCH_ALL + AND only (baseline, was always working)
    qt_variant_cte_and_only """
        WITH cte AS (
            SELECT id, OBJECTID,
                   cast(OVERFLOWPROPERTIES['firstname'] as VARCHAR) as firstname
            FROM ${tblVariant}
        )
        SELECT cte.id, cte.OBJECTID, cte.firstname FROM cte
        LEFT JOIN ${tblAssoc} a ON cte.id = a.to_id
        WHERE cte.firstname MATCH_ALL 'hello'
          AND a.to_id IS NOT NULL
        ORDER BY cte.id
    """

    // =============================================
    // Normal string + explicit Cast + CTE + OR
    // =============================================

    // Case 5: normal string + explicit CAST + CTE + OR (was crashing)
    qt_normal_cast_cte_or """
        WITH cte AS (
            SELECT id, CAST(name AS VARCHAR(200)) as name_casted, category
            FROM ${tblNormal}
        )
        SELECT cte.id, cte.name_casted, cte.category FROM cte
        LEFT JOIN ${tblAssoc} a ON cte.id = a.to_id
        WHERE (cte.name_casted MATCH_ALL 'hello' AND a.to_id IS NOT NULL)
           OR cte.category > 1
        ORDER BY cte.id
    """

    // Case 6: normal string + no Cast + CTE + OR (was always working)
    qt_normal_no_cast_cte_or """
        WITH cte AS (
            SELECT id, name, category FROM ${tblNormal}
        )
        SELECT cte.id, cte.name, cte.category FROM cte
        LEFT JOIN ${tblAssoc} a ON cte.id = a.to_id
        WHERE (cte.name MATCH_ALL 'hello' AND a.to_id IS NOT NULL)
           OR cte.category > 1
        ORDER BY cte.id
    """

    // =============================================
    // EXISTS + OR pattern (original bug report)
    // =============================================

    // Case 7: variant subcolumn + CTE + EXISTS + OR (original bug pattern)
    qt_variant_exists_or """
        WITH cte AS (
            SELECT id, OBJECTID,
                   cast(OVERFLOWPROPERTIES['firstname'] as VARCHAR) as firstname
            FROM ${tblVariant}
        )
        SELECT cte.id, cte.OBJECTID, cte.firstname FROM cte
        WHERE (cte.firstname MATCH_ALL 'hello'
               AND EXISTS (SELECT 1 FROM ${tblAssoc} a WHERE a.to_id = cte.id))
           OR cte.OBJECTID > 200
        ORDER BY cte.id
    """

    sql "DROP TABLE IF EXISTS ${tblVariant}"
    sql "DROP TABLE IF EXISTS ${tblNormal}"
    sql "DROP TABLE IF EXISTS ${tblAssoc}"
}
