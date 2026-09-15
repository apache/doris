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

suite("correlated_in_scalar_aggregate") {
    sql "DROP TABLE IF EXISTS cisa_o"
    sql """
        CREATE TABLE IF NOT EXISTS cisa_o (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS cisa_i"
    sql """
        CREATE TABLE IF NOT EXISTS cisa_i (
            k INT NULL,
            g INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k, g)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO cisa_o VALUES (1), (2), (7), (NULL), (-1)"
    sql "INSERT INTO cisa_i VALUES (1, 10), (2, 10), (5, 20), (NULL, 30)"

    // the supported correlated scalar subqueries: equality correlation with a global aggregation
    order_qt_scalar_eq_count """
        SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k = o.k) AS c FROM cisa_o o ORDER BY o.k
    """
    order_qt_scalar_eq_max """
        SELECT o.k, (SELECT max(i.k) FROM cisa_i i WHERE i.k = o.k) AS c FROM cisa_o o ORDER BY o.k
    """
    order_qt_scalar_eq_sum """
        SELECT o.k, (SELECT sum(i.g) FROM cisa_i i WHERE i.k = o.k) AS s FROM cisa_o o ORDER BY o.k
    """
    // IN over the correlated inner rows, and its negation with the usual null handling
    order_qt_in_plain """
        SELECT o.k FROM cisa_o o WHERE o.k IN (SELECT i.k FROM cisa_i i WHERE i.k = o.k) ORDER BY o.k
    """
    order_qt_not_in_plain """
        SELECT o.k FROM cisa_o o WHERE o.k NOT IN (SELECT i.k FROM cisa_i i WHERE i.k = o.k) ORDER BY o.k
    """
    // EXISTS which is correlated only by its HAVING clause: the aggregation is evaluated for every
    // outer row, so the predicates of the HAVING clause are applied to the global aggregation
    order_qt_exists_having_only_corr_ge """
        SELECT o.k FROM cisa_o o
        WHERE EXISTS (SELECT count(*) FROM cisa_i i HAVING count(*) >= o.k - 4) ORDER BY o.k
    """
    order_qt_exists_having_only_corr_le """
        SELECT o.k FROM cisa_o o
        WHERE EXISTS (SELECT count(*) FROM cisa_i i HAVING count(*) <= o.k - 4) ORDER BY o.k
    """

    // `x IN (subquery)` with a HAVING clause which references the outer query: the aggregation of
    // the subquery is evaluated for every outer row and the HAVING clause decides whether that row
    // exists, so `x IN (subquery)` is `if(having, x = aggregation, false)` and the negation is
    // `if(having, x <> aggregation, true)`. When the `IN` predicate is used as a value, its plan is
    // a null aware mark join (the equality with the subquery output is its hash condition and the
    // predicates of the HAVING clause are its other join conjuncts) and the mark of an outer row
    // whose HAVING clause holds but whose value is not equal to the aggregation must be false
    // instead of null.
    sql "DROP TABLE IF EXISTS cisa_m"
    sql """
        CREATE TABLE IF NOT EXISTS cisa_m (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO cisa_m VALUES (3), (4), (9), (NULL)"

    // cisa_i has 4 rows, so the aggregation of the subquery is 4
    order_qt_in_having_refs_outer """
        SELECT m.k, m.k IN (SELECT count(*) FROM cisa_i i HAVING m.k <= 8) AS v
        FROM cisa_m m ORDER BY m.k
    """
    order_qt_not_in_having_refs_outer """
        SELECT m.k, m.k NOT IN (SELECT count(*) FROM cisa_i i HAVING m.k <= 8) AS v
        FROM cisa_m m ORDER BY m.k
    """
    // the HAVING clause may also reference the aggregation of the subquery
    order_qt_in_having_refs_outer_and_agg """
        SELECT m.k, 4 IN (SELECT count(*) FROM cisa_i i HAVING count(*) >= m.k - 4) AS v
        FROM cisa_m m ORDER BY m.k
    """
    order_qt_not_in_having_refs_outer_and_agg """
        SELECT m.k, 4 NOT IN (SELECT count(*) FROM cisa_i i HAVING count(*) >= m.k - 4) AS v
        FROM cisa_m m ORDER BY m.k
    """
    order_qt_in_having_aggregation_alias """
        SELECT m.k, m.k IN (SELECT count(*) AS c FROM cisa_i i HAVING c = m.k) AS v
        FROM cisa_m m ORDER BY m.k
    """
    // a HAVING clause which does not reference the outer query keeps the comparison of the values of
    // the subquery (null and false are different results here)
    order_qt_in_having_uncorrelated """
        SELECT m.k, m.k IN (SELECT count(*) FROM cisa_i i HAVING count(*) = 4) AS v
        FROM cisa_m m ORDER BY m.k
    """

    // `x [not] in (subquery)` where the subquery keeps a correlated predicate in the filter above
    // the projection of a derived table and another one in the filter below that projection: the
    // rules which pull the correlated predicates into the apply run one after the other, and the
    // condition of the second one may not replace the condition of the first one:
    //   o.k in (select x.k from (select k from cisa_i i where i.k = o.k) x where x.k > o.k)
    // the subquery is empty for every outer row (`i.k = o.k` and `i.k > o.k` cannot hold at the
    // same time), so the IN is false and the NOT IN is true for every row, the null row included
    order_qt_in_with_correlated_filters_above_and_below_a_projection """
        SELECT o.k, o.k IN (SELECT x.k FROM (SELECT i.k FROM cisa_i i WHERE i.k = o.k) x
            WHERE x.k > o.k) AS v
        FROM cisa_o o ORDER BY o.k
    """
    order_qt_not_in_with_correlated_filters_above_and_below_a_projection """
        SELECT o.k, o.k NOT IN (SELECT x.k FROM (SELECT i.k FROM cisa_i i WHERE i.k = o.k) x
            WHERE x.k > o.k) AS v
        FROM cisa_o o ORDER BY o.k
    """

    // The shapes below are not supported by the scalar and IN subquery rewrites: they must be
    // rejected with a user error and must never return a wrong result silently.
    test {
        sql "SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k < o.k) AS c FROM cisa_o o"
        exception "scalar subquery's correlatedPredicates's operator must be EQ"
    }
    test {
        sql "SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k <=> o.k) AS c FROM cisa_o o"
        exception "scalar subquery's correlatedPredicates's operator must be EQ"
    }
    test {
        sql "SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k = o.k GROUP BY i.g) AS c FROM cisa_o o"
        exception "access outer query's column before agg with group by is not supported"
    }
    test {
        sql "SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k = o.k HAVING count(*) = 0) AS c" +
                " FROM cisa_o o"
        exception "only project, sort and subquery alias node is allowed after agg node"
    }
    test {
        sql "SELECT o.k FROM cisa_o o WHERE o.k IN (SELECT count(*) FROM cisa_i i WHERE i.k = o.k)"
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    test {
        sql "SELECT o.k FROM cisa_o o" +
                " WHERE o.k IN (SELECT o.k FROM cisa_i i WHERE i.k = o.k HAVING count(*) >= o.k - 4)"
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
}
