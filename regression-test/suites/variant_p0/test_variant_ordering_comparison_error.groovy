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

suite("test_variant_ordering_comparison_error") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"
    setBeConfigTemporary([enable_variant_v2: true]) {

    test {
        sql """
            SELECT v
            FROM (
                SELECT CAST('2' AS VARIANT) AS v
                UNION ALL
                SELECT CAST('1' AS VARIANT) AS v
            ) t
            ORDER BY v
        """
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column"
    }

    test {
        sql """
            SELECT v
            FROM (
                SELECT CAST('2' AS VARIANT) AS v
                UNION ALL
                SELECT CAST('1' AS VARIANT) AS v
            ) t
            ORDER BY v
            LIMIT 1
        """
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column"
    }

    test {
        sql "SELECT CAST('2' AS VARIANT) > CAST('1' AS VARIANT)"
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT CAST('2' AS VARIANT) > 1"
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT CAST('2' AS VARIANT) <=> CAST('1' AS VARIANT)"
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
}
