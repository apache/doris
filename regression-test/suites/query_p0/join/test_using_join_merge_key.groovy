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

suite("test_using_join_merge_key", "query_p0") {
    order_qt_left_outer """
        SELECT l.k, r.k, k, l.*, r.*, *
        FROM (
            SELECT 1 AS k, 'l1' AS v1
            UNION ALL SELECT 2, 'l2'
            UNION ALL SELECT 4, 'l4'
        ) l
        LEFT OUTER JOIN (
            SELECT 2 AS k, 'r2' AS v2
            UNION ALL SELECT 3, 'r3'
            UNION ALL SELECT 4, 'r4'
        ) r USING(k)
    """

    order_qt_right_outer """
        SELECT l.k, r.k, k, l.*, r.*, *
        FROM (
            SELECT 1 AS k, 'l1' AS v1
            UNION ALL SELECT 2, 'l2'
            UNION ALL SELECT 4, 'l4'
        ) l
        RIGHT OUTER JOIN (
            SELECT 2 AS k, 'r2' AS v2
            UNION ALL SELECT 3, 'r3'
            UNION ALL SELECT 4, 'r4'
        ) r USING(k)
    """

    order_qt_full_outer """
        SELECT l.k, r.k, k, l.*, r.*, *
        FROM (
            SELECT 1 AS k, 'l1' AS v1
            UNION ALL SELECT 2, 'l2'
            UNION ALL SELECT 4, 'l4'
        ) l
        FULL OUTER JOIN (
            SELECT 2 AS k, 'r2' AS v2
            UNION ALL SELECT 3, 'r3'
            UNION ALL SELECT 4, 'r4'
        ) r USING(k)
    """

    order_qt_left_semi """
        SELECT l.k, k, l.*, *
        FROM (
            SELECT 1 AS k, 'l1' AS v1
            UNION ALL SELECT 2, 'l2'
            UNION ALL SELECT 4, 'l4'
        ) l
        LEFT SEMI JOIN (
            SELECT 2 AS k, 'r2' AS v2
            UNION ALL SELECT 3, 'r3'
            UNION ALL SELECT 4, 'r4'
        ) r USING(k)
    """

    order_qt_right_semi """
        SELECT r.k, k, r.*, *
        FROM (
            SELECT 1 AS k, 'l1' AS v1
            UNION ALL SELECT 2, 'l2'
            UNION ALL SELECT 4, 'l4'
        ) l
        RIGHT SEMI JOIN (
            SELECT 2 AS k, 'r2' AS v2
            UNION ALL SELECT 3, 'r3'
            UNION ALL SELECT 4, 'r4'
        ) r USING(k)
    """

    order_qt_left_anti """
        SELECT l.k, k, l.*, *
        FROM (
            SELECT 1 AS k, 'l1' AS v1
            UNION ALL SELECT 2, 'l2'
            UNION ALL SELECT 4, 'l4'
        ) l
        LEFT ANTI JOIN (
            SELECT 2 AS k, 'r2' AS v2
            UNION ALL SELECT 3, 'r3'
            UNION ALL SELECT 4, 'r4'
        ) r USING(k)
    """

    order_qt_right_anti """
        SELECT r.k, k, r.*, *
        FROM (
            SELECT 1 AS k, 'l1' AS v1
            UNION ALL SELECT 2, 'l2'
            UNION ALL SELECT 4, 'l4'
        ) l
        RIGHT ANTI JOIN (
            SELECT 2 AS k, 'r2' AS v2
            UNION ALL SELECT 3, 'r3'
            UNION ALL SELECT 4, 'r4'
        ) r USING(k)
    """

    order_qt_inner """
        SELECT l.k, r.k, k, l.*, r.*, *
        FROM (
            SELECT 1 AS k, 'l1' AS v1
            UNION ALL SELECT 2, 'l2'
            UNION ALL SELECT 4, 'l4'
        ) l
        JOIN (
            SELECT 2 AS k, 'r2' AS v2
            UNION ALL SELECT 3, 'r3'
            UNION ALL SELECT 4, 'r4'
        ) r USING(k)
    """

    order_qt_chained_full_outer_using """
        WITH l AS (SELECT 10 AS lv, 1 AS pk, 1 AS ak),
             r AS (SELECT 20 AS rv, 2 AS pk),
             n AS (SELECT 30 AS nv, 3 AS ak)
        SELECT *
        FROM l FULL OUTER JOIN r USING(pk) FULL OUTER JOIN n USING(ak)
        ORDER BY l.pk NULLS LAST, r.pk NULLS LAST, n.ak NULLS LAST;
    """
}
