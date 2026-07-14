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

suite("test_variant_equality_contexts", "p0,nonConcurrent") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    setBeConfigTemporary([enable_variant_v2: true]) {
    explain {
        sql """
            SELECT v, COUNT(*)
            FROM (SELECT CAST(CAST(number % 2 AS STRING) AS VARIANT) v
                  FROM numbers("number" = "4")) t
            GROUP BY v
        """
        contains "AGGREGATE"
    }

    explain {
        sql """
            SELECT DISTINCT v
            FROM (SELECT CAST(CAST(number % 2 AS STRING) AS VARIANT) v
                  FROM numbers("number" = "4")) t
        """
        contains "AGGREGATE"
    }

    explain {
        sql """
            SELECT COUNT(DISTINCT v)
            FROM (SELECT CAST(CAST(number % 2 AS STRING) AS VARIANT) v
                  FROM numbers("number" = "4")) t
        """
        contains "COUNT(DISTINCT v)"
    }

    explain {
        sql """
            SELECT CAST(CAST(number AS STRING) AS VARIANT) v
            FROM numbers("number" = "2")
            INTERSECT
            SELECT CAST(CAST(number AS STRING) AS VARIANT) v
            FROM numbers("number" = "2")
        """
        contains "INTERSECT"
    }

    explain {
        sql """
            SELECT CAST(CAST(number AS STRING) AS VARIANT) v
            FROM numbers("number" = "2")
            EXCEPT
            SELECT CAST(CAST(number AS STRING) AS VARIANT) v
            FROM numbers("number" = "2")
        """
        contains "EXCEPT"
    }

    test {
        sql "SELECT parse_to_variant('1') = parse_to_variant('1.0')"
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT parse_to_variant('1') != parse_to_variant('1.0')"
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT parse_to_variant('1') <=> parse_to_variant('1.0')"
        exception "CAST to a concrete type first"
    }

    test {
        sql """
            SELECT *
            FROM (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) a
            JOIN (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) b
            ON a.v = b.v
        """
        exception "CAST to a concrete type first"
    }

    test {
        sql """
            SELECT *
            FROM (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) a
            JOIN (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) b
            ON a.v <=> b.v
        """
        exception "CAST to a concrete type first"
    }

    order_qt_group_by """
        SELECT CAST(v AS STRING), COUNT(*)
        FROM (SELECT parse_to_variant(CAST(number % 2 AS STRING)) v
              FROM numbers("number" = "4")) t
        GROUP BY v
        ORDER BY 1
    """

    order_qt_group_by_sql_null_semantics """
        SELECT v IS NULL, CAST(v AS STRING), COUNT(*)
        FROM (
            SELECT CAST(IF(number = 0, NULL, (number - 1) % 2) AS VARIANT) v
            FROM numbers("number" = "6")
        ) t
        GROUP BY v
        ORDER BY 1 DESC, 2
    """

    order_qt_group_by_variant_null_semantics """
        SELECT v IS NULL, CAST(v AS STRING), COUNT(*)
        FROM (
            SELECT parse_to_variant(
                       IF(number = 0, 'null', CAST((number - 1) % 2 AS STRING))) v
            FROM numbers("number" = "6")
        ) t
        GROUP BY v
        ORDER BY 1 DESC, 2
    """

    qt_count_distinct_canonical """
        SELECT COUNT(DISTINCT v)
        FROM (
            SELECT parse_to_variant('{"b":2,"a":1}') v
            UNION ALL
            SELECT parse_to_variant('{"a":1,"b":2}')
            UNION ALL
            SELECT parse_to_variant('[1,2]')
            UNION ALL
            SELECT parse_to_variant('[2,1]')
            UNION ALL
            SELECT parse_to_variant('1')
            UNION ALL
            SELECT parse_to_variant('1.0')
        ) t
    """

    order_qt_intersect_encoded_canonical_numeric """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant(CAST(number AS STRING)) v
            FROM numbers("number" = "4")
            INTERSECT
            SELECT parse_to_variant(CONCAT(CAST(number AS STRING), '.0')) v
            FROM numbers("number" = "3")
        ) t
        ORDER BY 1
    """

    order_qt_except_encoded_array_order """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant(IF(number = 0, '[1,2]', '[2,1]')) v
            FROM numbers("number" = "2")
            EXCEPT
            SELECT parse_to_variant('[1,2]') v
        ) t
        ORDER BY 1
    """

    order_qt_intersect_typed_fast_path """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT CAST(number AS VARIANT) v
            FROM numbers("number" = "4")
            INTERSECT
            SELECT CAST(number AS VARIANT) v
            FROM numbers("number" = "3")
        ) t
        ORDER BY 1
    """

    order_qt_union_distinct_canonical """
        SELECT CAST(v AS STRING)
        FROM (
            SELECT parse_to_variant('{"b":2,"a":1}') v
            UNION DISTINCT
            SELECT parse_to_variant('{"a":1,"b":2}')
            UNION DISTINCT
            SELECT parse_to_variant('1')
            UNION DISTINCT
            SELECT parse_to_variant('1.0')
        ) t
        ORDER BY 1
    """

    test {
        sql "SELECT MAX(CAST('1' AS VARIANT))"
        exception "Doris hll, bitmap"
    }

    order_qt_explicit_cast_equality """
        SELECT CAST(v AS STRING) = CAST(v AS STRING)
        FROM (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
              FROM numbers("number" = "2")) t
        ORDER BY 1
    """
    }
}
