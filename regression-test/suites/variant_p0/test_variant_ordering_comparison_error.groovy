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

suite("test_variant_ordering_comparison_error", "p0,nonConcurrent") {
    def variantV2Function = "parse_to_variant"
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

        qt_variant_order """
            SELECT id
            FROM (
                SELECT 1 AS id, parse_to_variant('2') AS v
                UNION ALL
                SELECT 2 AS id, parse_to_variant('1') AS v
                UNION ALL
                SELECT 3 AS id, parse_to_variant('1.5') AS v
            ) t
            ORDER BY v, id
        """
        qt_variant_topn """
            SELECT id
            FROM (
                SELECT 1 AS id, parse_to_variant('2') AS v
                UNION ALL
                SELECT 2 AS id, parse_to_variant('1') AS v
                UNION ALL
                SELECT 3 AS id, parse_to_variant('1.5') AS v
            ) t
            ORDER BY v DESC, id
            LIMIT 2
        """
        qt_variant_window """
            SELECT id, row_number() OVER (ORDER BY v, id)
            FROM (
                SELECT 1 AS id, parse_to_variant('2') AS v
                UNION ALL
                SELECT 2 AS id, parse_to_variant('1') AS v
                UNION ALL
                SELECT 3 AS id, parse_to_variant('1.5') AS v
            ) t
            ORDER BY id
        """
    // Canonical peers (1, 1.0 and objects with reordered keys), SQL NULL, a JSON null (read back
    // as {}) and a boolean. One bucket keeps the numbers ahead of the boolean in the segment.
    sql "DROP TABLE IF EXISTS variant_ordering_peers"
    sql """CREATE TABLE variant_ordering_peers (id INT, v VARIANT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql """INSERT INTO variant_ordering_peers VALUES
        (1, parse_to_variant('2')), (2, parse_to_variant('1')), (3, parse_to_variant('1.0')),
        (4, parse_to_variant('"a"')), (5, NULL), (6, parse_to_variant('null')),
        (7, parse_to_variant('{"k":1,"j":2}')), (8, parse_to_variant('{"j":2,"k":1.0}')),
        (9, parse_to_variant('[1,2]')), (10, parse_to_variant('true'))"""
    qt_asc_nulls_last "SELECT id FROM variant_ordering_peers ORDER BY v ASC NULLS LAST, id"
    qt_desc_nulls_first "SELECT id FROM variant_ordering_peers ORDER BY v DESC NULLS FIRST, id"
    qt_rank_peers """SELECT id, rank() OVER (ORDER BY v), dense_rank() OVER (ORDER BY v)
        FROM variant_ordering_peers ORDER BY id"""
    qt_partition_topn """SELECT id, rn FROM (
            SELECT id, row_number() OVER (ORDER BY v, id) rn FROM variant_ordering_peers) t
        WHERE rn <= 4 ORDER BY rn"""
    sql "SET enable_spill = true"
    sql "SET enable_force_spill = true"
    sql "SET spill_min_revocable_mem = 1"
    qt_spill_sort "SELECT id FROM variant_ordering_peers ORDER BY v NULLS FIRST, id"
    qt_spill_sort_desc "SELECT id FROM variant_ordering_peers ORDER BY v DESC NULLS LAST, id"
    sql "SET enable_force_spill = false"
    sql "SET enable_spill = false"

    // A bare NULL takes the Variant type on either side. `=` and `!=` are NULL for every row;
    // `<=>` matches the SQL NULL row only.
    order_qt_bare_null_equals """SELECT id, v = NULL, NULL = v, v != NULL, NULL != v
        FROM variant_ordering_peers WHERE id IN (2, 5, 6)"""
    order_qt_bare_null_safe_equals "SELECT id FROM variant_ordering_peers WHERE v <=> NULL"
    order_qt_bare_null_safe_equals_reversed "SELECT id FROM variant_ordering_peers WHERE NULL <=> v"
    order_qt_bare_null_safe_not_equals "SELECT id FROM variant_ordering_peers WHERE NOT (v <=> NULL)"
    test {
        sql "SELECT id FROM variant_ordering_peers WHERE v > NULL"
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT CAST('2' AS VARIANT) > CAST('1' AS VARIANT)"
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT CAST('2' AS VARIANT) > 1"
        exception "CAST to a concrete type first"
    }

    order_qt_explicit_cast_order """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT CAST('2' AS VARIANT) AS v
            UNION ALL
            SELECT CAST('1' AS VARIANT) AS v
        ) t
        ORDER BY CAST(v AS STRING)
    """

    order_qt_explicit_cast_comparison """
        SELECT CAST(v AS STRING) > '1'
        FROM (
            SELECT CAST('2' AS VARIANT) AS v
            UNION ALL
            SELECT CAST('1' AS VARIANT) AS v
        ) t
        ORDER BY 1
    """

    order_qt_variant_is_null """
        SELECT CAST(NULL AS VARIANT) IS NULL,
               CAST('1' AS VARIANT) IS NOT NULL
    """

    test {
        sql "SELECT CAST('1' AS JSON) > CAST('2' AS JSON)"
        exception "comparison predicate could not contains json type"
    }

    order_qt_jsonb_order """
        SELECT CAST(CAST(number AS STRING) AS JSON)
        FROM numbers("number" = "2")
        ORDER BY 1
    """

    test {
        sql """
            SELECT HLL_HASH(CAST(number AS STRING))
            FROM numbers("number" = "2")
            ORDER BY 1
        """
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column"
    }

}
