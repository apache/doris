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
    // IN over the global aggregation of the correlated rows: the aggregation of an outer row whose
    // domain is empty is 0 (the global aggregation always produces one row), so that row compares
    // against 0 instead of being dropped or compared with nothing
    order_qt_in_correlated_global_agg """
        SELECT o.k FROM cisa_o o WHERE o.k IN (SELECT count(*) FROM cisa_i i WHERE i.k = o.k) ORDER BY o.k
    """
    order_qt_not_in_correlated_global_agg """
        SELECT o.k FROM cisa_o o WHERE o.k NOT IN (SELECT count(*) FROM cisa_i i WHERE i.k = o.k) ORDER BY o.k
    """
    order_qt_in_correlated_global_agg_as_value """
        SELECT o.k, o.k IN (SELECT count(*) FROM cisa_i i WHERE i.k = o.k) AS v
        FROM cisa_o o ORDER BY o.k
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
    // the HAVING clause filters the only row which the aggregation of a correlation key produces
    // (count(*) = m.k is unknown for the null key), so the right side of the mark join is empty:
    // the outer row with the null key probes that empty build side with a null probe key
    order_qt_in_having_filters_the_aggregation """
        SELECT m.k, m.k IN (SELECT count(*) FROM cisa_i i WHERE i.k = m.k HAVING count(*) = m.k) AS v
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
    // the same two filters around the projection of the derived table, with an aggregation above
    // them: the filter above the projection and the filter below it both restrict the rows of the
    // domain of an outer row (the predicates contradict each other here, so the domain of every
    // outer row holds no row and the count of the domain of every outer row is 0)
    order_qt_in_with_correlated_filters_above_and_below_a_projection_count """
        SELECT o.k, o.k IN (SELECT count(*) FROM (SELECT i.k FROM cisa_i i WHERE i.k = o.k) x
            WHERE x.k > o.k) AS v
        FROM cisa_o o ORDER BY o.k
    """
    // the predicate below the projection of the derived table is the one which restricts the rows of
    // the domain, and the predicate above it is evaluated on those rows as well
    order_qt_in_with_correlated_filters_above_and_below_a_projection_eq """
        SELECT o.k, o.k IN (SELECT count(*) FROM (SELECT i.k FROM cisa_i i WHERE i.k = o.k) x
            WHERE x.k = o.k) AS v
        FROM cisa_o o ORDER BY o.k
    """

    // The scalar subquery whose correlated predicate is not an equality between the outer side and
    // the inner side is evaluated on the aggregation of the domain of every outer row as well: the
    // left outer join which a scalar subquery is unnested into pairs the outer row with the groups
    // of the inner side whose key is the value of the outer row, which is the aggregation of the
    // domain of the outer row only when that predicate is an equality
    order_qt_scalar_lt_count """
        SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k < o.k) AS c FROM cisa_o o ORDER BY o.k
    """
    order_qt_scalar_lt_sum """
        SELECT o.k, (SELECT sum(i.g) FROM cisa_i i WHERE i.k < o.k) AS s FROM cisa_o o ORDER BY o.k
    """
    // the number of values which topn_array keeps controls the aggregate: it is not replaced by the
    // guard of the row which is kept for an empty correlated domain, while the values which the
    // aggregate aggregates are (the array is sorted to make the order of its values deterministic)
    order_qt_scalar_lt_topn_array """
        SELECT o.k, array_sort((SELECT topn_array(i.g, 1) FROM cisa_i i WHERE i.k < o.k)) AS a
        FROM cisa_o o ORDER BY o.k
    """
    // `i.k <=> o.k` is an equality as well, but one whose domain contains the inner rows of the
    // null key: the condition of the left outer join of a scalar subquery cannot express it, so it
    // is evaluated on the aggregation of the domain as well (the outer row with the null key
    // compares against the count of the inner rows whose key is null)
    order_qt_scalar_nullsafe_equal_count """
        SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k <=> o.k) AS c FROM cisa_o o ORDER BY o.k
    """
    // the aggregation of a scalar subquery may group the rows of its domain: the value of an outer
    // row is the value of the single row of its domain, and the outer rows whose domain has no row
    // at all (or whose rows form several groups, which the runtime check of a scalar subquery
    // rejects) return null
    order_qt_scalar_grouped_count """
        SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k = o.k GROUP BY i.g) AS c FROM cisa_o o ORDER BY o.k
    """
    // a HAVING clause of a global aggregation decides whether the row of the domain survives: the
    // aggregation of the empty domain (the count 0) satisfies the HAVING clause of this subquery, so
    // those outer rows return the count of their empty domain
    order_qt_scalar_having_count_eq_0 """
        SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k = o.k HAVING count(*) = 0) AS c
        FROM cisa_o o ORDER BY o.k
    """
    // ... while a HAVING clause which rejects the row of the empty domain keeps it out
    order_qt_scalar_having_count_gt_0 """
        SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k = o.k HAVING count(*) > 0) AS c
        FROM cisa_o o ORDER BY o.k
    """
    // the aggregation of the subquery may itself be aggregated by a derived table: the outer row is
    // matched against the rows of that aggregation for the correlation key of the outer row
    order_qt_in_nested_aggregation """
        SELECT o.k FROM cisa_o o
        WHERE o.k IN (SELECT max(c) FROM (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k GROUP BY i.g) x)
        ORDER BY o.k
    """
    // a HAVING clause which reads the aggregation above the aggregation of the derived table: the
    // predicate is pulled into the apply and reads the output of that aggregation, while the
    // projections below it may only expose the correlation key
    order_qt_in_nested_aggregation_having """
        SELECT o.k FROM cisa_o o
        WHERE o.k IN (SELECT max(c) FROM (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k GROUP BY i.g) x
            HAVING max(c) <= o.k)
        ORDER BY o.k
    """
    // A NOT IN whose select list is a global aggregation above the aggregation of the derived table:
    // the aggregation of an empty correlated domain returns one row whose value is null (the max of
    // an empty derived table), so the NOT IN of that row is unknown and the row is not returned. The
    // rewrite keeps the row of such a key and the max above it ignores that row and returns the null
    // of its empty input, so the outer row of an empty domain is dropped by the unknown of the null
    // comparison as well (the same holds for an IN which is used as a value, whose plan is a mark
    // join and which returns the unknown null for those rows)
    order_qt_not_in_nested_aggregation """
        SELECT o.k FROM cisa_o o
        WHERE o.k NOT IN (SELECT max(c) FROM (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k GROUP BY i.g) x)
        ORDER BY o.k
    """
    order_qt_in_nested_aggregation_as_value """
        SELECT o.k, o.k IN (SELECT max(c) FROM (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k GROUP BY i.g) x) AS v
        FROM cisa_o o ORDER BY o.k
    """
    // The aggregation above the aggregation of the derived table may be aggregated by yet another
    // derived table: the marker which the rewrite exposes to tell the row of an empty domain apart
    // from the rows of a non empty one is read by the aggregations of every level above the domain,
    // so every aggregation which is built between them has to expose that marker as well
    order_qt_not_in_three_aggregations """
        SELECT o.k FROM cisa_o o
        WHERE o.k NOT IN (SELECT max(m) FROM (SELECT max(c) AS m FROM
            (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k GROUP BY i.g) x) y)
        ORDER BY o.k
    """
    order_qt_in_three_aggregations_as_value """
        SELECT o.k, o.k IN (SELECT max(m) FROM (SELECT max(c) AS m FROM
            (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k GROUP BY i.g) x) y) AS v
        FROM cisa_o o ORDER BY o.k
    """
    // The group by of the aggregation of the derived table may read a column which is declared not
    // null: the left outer join of the rewrite (it keeps the row of an empty correlated domain)
    // reports that column as nullable, so the group by of the new aggregation and the group by of
    // the aggregations above it have to read the nullable version of it as well (a plan which keeps
    // the not nullable column and its nullable version with one ExprId reports an error while
    // fe_debug is set)
    sql "DROP TABLE IF EXISTS cisa_nn"
    sql """
        CREATE TABLE IF NOT EXISTS cisa_nn (
            k INT NULL,
            g INT NOT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO cisa_nn VALUES (1, 10), (2, 10), (5, 20)"
    sql "set fe_debug = true"
    order_qt_not_in_nested_aggregation_not_null_group_key """
        SELECT o.k FROM cisa_o o
        WHERE o.k NOT IN (SELECT max(c) FROM (SELECT count(*) AS c FROM cisa_nn i WHERE i.k = o.k GROUP BY i.g) x)
        ORDER BY o.k
    """
    // A HAVING clause above the aggregation of the domain which reads the not null group key keeps
    // the same column in the filter which the rewrite retains: that filter has to read the nullable
    // version of the column as well, the same one which the group by of the aggregation reads
    order_qt_not_in_nested_aggregation_having_reads_the_not_null_group_key """
        SELECT o.k FROM cisa_o o
        WHERE o.k NOT IN (SELECT max(c) FROM (SELECT count(*) AS c FROM cisa_nn i
            WHERE i.k = o.k GROUP BY i.g HAVING i.g + count(*) > 0) x)
        ORDER BY o.k
    """
    sql "set fe_debug = false"
    // an IN subquery whose select list reads the outer query cannot be unnested: the rewrite reads
    // the value it compares from the aggregation of the domain, which cannot aggregate the value of
    // the outer row
    test {
        sql "SELECT o.k FROM cisa_o o WHERE o.k IN (SELECT sum(i.g + o.k) FROM cisa_i i)"
        exception "access outer query's column in aggregate is not supported"
    }
    // a window of a correlated IN subquery cannot be rewritten either: the rewrite groups the
    // aggregation of the subquery by the correlation key, so the window of the rewrite would be
    // evaluated over the rows of every correlation key together, while the window of the subquery
    // of the query is evaluated over the rows of one domain
    test {
        sql "SELECT o.k FROM cisa_o o WHERE o.k IN" +
                " (SELECT sum(i.g) OVER () FROM cisa_i i WHERE i.k = o.k GROUP BY i.g)"
        exception "access outer query's column before window function is not supported"
    }
    // A window below the correlated predicate instead is evaluated before that predicate selects the
    // rows of the domain of an outer row in the plan of the query as well, so the rewrite keeps its
    // evaluation domain: the row number of an inner row is its position in the whole inner table,
    // and the outer row whose key equals that number is the row which the IN keeps
    order_qt_in_window_below_the_correlated_predicate """
        SELECT o.k FROM cisa_o o
        WHERE o.k IN (SELECT rn FROM (SELECT k, row_number() OVER (ORDER BY k) AS rn FROM cisa_i
            WHERE k IS NOT NULL) x WHERE x.k = o.k)
        ORDER BY o.k
    """
    // The limit of the derived table keeps one row of the rows of the correlation key of an outer
    // row, and the lateral view of the derived table explodes the arrays of the rows of that key: the
    // rewrite reads the value which the IN compares from the aggregation of the domain of an outer
    // row, so the limit of the rewrite would read the domains of every correlation key together and
    // the lateral view would be evaluated once for all of them. The wrappers above the correlated
    // predicate are therefore reported instead of building a plan which reads the columns of the
    // outer query from the rows of another correlation key
    test {
        sql "SELECT o.k FROM cisa_o o WHERE o.k IN (SELECT max(c) FROM" +
                " (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k GROUP BY i.g LIMIT 1) x)"
        exception "access outer query's column before limit is not supported"
    }
    test {
        sql "SELECT o.k FROM cisa_o o WHERE o.k IN (SELECT max(c) FROM" +
                " (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k GROUP BY i.g ORDER BY c LIMIT 1) x)"
        exception "access outer query's column before limit is not supported"
    }
    test {
        sql "SELECT o.k FROM cisa_o o WHERE o.k IN (SELECT max(e) FROM" +
                " (SELECT count(*) AS c, array_agg(i.g) AS a FROM cisa_i i" +
                " WHERE i.k = o.k GROUP BY i.g) x LATERAL VIEW explode(a) t AS e)"
        exception "access outer query's column before lateral view is not supported"
    }
    // a generator of a lateral view which reads the outer column has no row to read that column from
    // on the inner side of the join of the rewrite either
    test {
        sql "SELECT o.k FROM cisa_o o WHERE o.k IN (SELECT e FROM cisa_i i" +
                " LATERAL VIEW explode(array(o.k, i.g)) t AS e WHERE i.k = o.k)"
        exception "access outer query's column in lateral view is not supported"
    }
    // A global aggregation above the aggregation of the derived table loses its key when the HAVING
    // clause of the aggregation of the derived table removes the row of an empty domain (the count 0
    // does not satisfy count(*) > 0): the original subquery compares the outer value with the null of
    // the max of the empty derived table, while a rewrite which dropped the key would compare it with
    // nothing. The rewrite keeps the row of such a key and lets it pass the HAVING clause, and the
    // max above it ignores that row and returns the null of its empty input, so a NOT IN and an IN
    // which is used as a value return the unknown of the null comparison for those rows
    order_qt_not_in_having_of_the_domain_aggregation """
        SELECT o.k, o.k NOT IN (SELECT max(c) FROM
            (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k HAVING count(*) > 0) x) AS v
        FROM cisa_o o ORDER BY o.k
    """
    order_qt_in_having_of_the_domain_aggregation_as_value """
        SELECT o.k, o.k IN (SELECT max(c) FROM
            (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k HAVING count(*) > 0) x) AS v
        FROM cisa_o o ORDER BY o.k
    """
    // a HAVING clause of the aggregation of the domain decides whether the row of an empty domain
    // survives, and the outer value of a positive IN is compared with the row which the aggregation
    // above it returns for that empty input: the value of the subquery of an outer row whose domain is
    // empty is null here (the count 0 does not satisfy the HAVING clause, so the max of the empty
    // derived table is null), and a null matches no outer value, so those rows are not returned
    order_qt_in_having_of_the_domain_aggregation """
        SELECT o.k FROM cisa_o o WHERE o.k IN (SELECT max(c) FROM
            (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k HAVING count(*) > 0) x)
        ORDER BY o.k
    """
    // The nodes above the aggregation of the domain may expose a value of their own for the empty
    // input (the coalesce of the subquery below turns the null of the max of the empty derived table
    // into the 0 which an outer value compares with): the rewrite keeps the row of such a key, the
    // max above it ignores that row and returns the null of its empty input, and the coalesce of the
    // plan of the subquery turns that null into the 0 as well, so an outer value of 0 matches the
    // subquery
    order_qt_in_with_a_value_exposed_for_the_empty_input """
        SELECT o.k, 0 IN (SELECT coalesce(max(c), 0) FROM
            (SELECT count(*) AS c FROM cisa_i i WHERE i.k = o.k GROUP BY i.g) x) AS v
        FROM cisa_o o ORDER BY o.k
    """

    // The shapes below are not supported by the scalar and IN subquery rewrites: they must be
    // rejected with a user error and must never return a wrong result silently.
    test {
        sql "SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k = o.k GROUP BY i.g HAVING count(*) >= o.k - 1) AS c" +
                " FROM cisa_o o"
        exception "access outer query's column in two places is not supported"
    }
    // a correlated predicate whose side mixes the outer query and the subquery cannot be evaluated
    // by the join which unnests the scalar subquery either
    test {
        sql "SELECT o.k, (SELECT count(*) FROM cisa_i i WHERE i.k - o.k = 0) AS c FROM cisa_o o"
        exception "Unsupported correlated subquery with correlated predicate"
    }
    // an IN subquery whose select list is the correlated column itself keeps the correlated column
    // in the projection of the subquery, which the rewrites of the IN subquery cannot expose: the
    // plan of the subquery reads the outer column from that projection, so it is rejected when the
    // subquery is analyzed
    test {
        sql "SELECT o.k FROM cisa_o o" +
                " WHERE o.k IN (SELECT o.k FROM cisa_i i WHERE i.k = o.k HAVING count(*) >= o.k - 4)"
        exception "access outer query's column in project is not supported"
    }
}
