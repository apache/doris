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

suite("test_variant_equality_contexts") {
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

    test {
        sql """
            SELECT COUNT(DISTINCT v)
            FROM (SELECT CAST(CAST(number % 2 AS STRING) AS VARIANT) v
                  FROM numbers("number" = "4")) t
        """
        exception "COUNT DISTINCT"
    }

    explain {
        sql """
            SELECT *
            FROM (SELECT number, CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) a
            JOIN (SELECT number, CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) b
            ON a.v = b.v
        """
        contains "HASH JOIN"
    }

    explain {
        sql """
            SELECT *
            FROM (SELECT number, CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) a,
                 (SELECT number, CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) b,
                 (SELECT number, CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) c
            WHERE a.v <=> c.v
        """
        contains "HASH JOIN"
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
        sql "SELECT CAST('1' AS VARIANT) = CAST('1' AS VARIANT)"
        exception "CAST to a concrete type first"
    }

    test {
        sql """
            SELECT *
            FROM (SELECT number, CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) t
            WHERE v = v
        """
        exception "CAST to a concrete type first"
    }

    test {
        sql """
            SELECT *
            FROM (SELECT number, CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) a
            JOIN (SELECT number, CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) b
            ON a.v = b.v OR a.number = b.number
        """
        exception "CAST to a concrete type first"
    }

    test {
        sql """
            SELECT *
            FROM (SELECT number, CAST(CAST(number AS STRING) AS VARIANT) v
                  FROM numbers("number" = "2")) a
            JOIN numbers("number" = "2") b ON a.v = a.v
        """
        exception "CAST to a concrete type first"
    }

    test {
        sql "SELECT MAX(CAST('1' AS VARIANT))"
        exception "Doris hll, bitmap"
    }

    test {
        sql "SELECT CAST('1' AS JSON) = CAST('1' AS JSON)"
        exception "comparison predicate could not contains json type"
    }

    order_qt_explicit_cast_equality """
        SELECT CAST(v AS STRING) = CAST(v AS STRING)
        FROM (SELECT CAST(CAST(number AS STRING) AS VARIANT) v
              FROM numbers("number" = "2")) t
        ORDER BY 1
    """

    order_qt_jsonb_group_by """
        SELECT CAST(CAST(number % 2 AS STRING) AS JSON), COUNT(*)
        FROM numbers("number" = "4")
        GROUP BY 1
        ORDER BY 1
    """
    }
}
