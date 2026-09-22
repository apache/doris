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

suite("correlated_exists_having") {
    sql "DROP TABLE IF EXISTS ceh_o"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_o (
            k INT NOT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS ceh_e"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_e (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS ceh_i"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_i (
            k INT NULL,
            g INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k, g)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS ceh_n"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_n (
            k INT NULL,
            g INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k, g)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO ceh_o VALUES (3)"
    // 7 has no matching inner row and NULL does not match the inner rows either
    sql "INSERT INTO ceh_e VALUES (1), (2), (7), (NULL)"
    sql "INSERT INTO ceh_i VALUES (1, 10), (2, 10), (5, 20), (NULL, 30)"
    // the group key of the inner rows may be null, and an inner row whose correlation value is null
    // never matches an outer row
    sql "INSERT INTO ceh_n VALUES (1, NULL), (1, NULL), (2, 5), (NULL, NULL)"

    // the inner rows of o.k = 3 are (1, 10) and (2, 10), they form one group g = 10 with 2 rows
    order_qt_noneq_grouped_having """
        SELECT o.k FROM ceh_o o
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k < o.k GROUP BY i.g HAVING count(*) = 2)
    """
    order_qt_noneq_grouped_having_not_exists """
        SELECT o.k FROM ceh_o o
        WHERE NOT EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k < o.k GROUP BY i.g HAVING count(*) = 2)
    """
    // the same query with an inner row which is not part of the domain of o.k = 3
    order_qt_noneq_grouped_having_one_more_row """
        SELECT o.k FROM ceh_o o
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k < o.k GROUP BY i.g HAVING count(*) >= 2)
    """
    // a global aggregate is computed over the whole domain of the outer row
    order_qt_noneq_global_having """
        SELECT o.k FROM ceh_o o
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k < o.k HAVING count(*) = 2)
    """
    // an equality correlated predicate maps the domain onto one group, the rows of the outer table
    // whose domain is empty must still be counted by count(*) = 0
    order_qt_eq_global_having_count_eq_0 """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k HAVING count(*) = 0)
        ORDER BY e.k
    """
    order_qt_eq_global_having_count_eq_0_not_exists """
        SELECT e.k FROM ceh_e e
        WHERE NOT EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k HAVING count(*) = 0)
        ORDER BY e.k
    """
    // sum() returns null for an empty input
    order_qt_eq_global_sum_is_null """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT sum(i.g) AS s FROM ceh_i i WHERE i.k = e.k HAVING sum(i.g) IS NULL)
        ORDER BY e.k
    """
    // a having clause which is false for an empty input, only the non empty domains are kept
    order_qt_eq_global_having_gt_0 """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k HAVING count(*) > 0)
        ORDER BY e.k
    """
    // equality + group by: the domain of the group is not changed by the rewrite
    order_qt_eq_grouped_having """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k GROUP BY i.g HAVING count(*) = 1)
        ORDER BY e.k
    """
    // mark join form of the same queries
    order_qt_mark_noneq_grouped_having """
        SELECT o.k FROM ceh_o o
        WHERE ifnull(EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k < o.k GROUP BY i.g HAVING count(*) = 2),
            false)
    """
    order_qt_mark_eq_global_having_count_eq_0 """
        SELECT e.k FROM ceh_e e
        WHERE ifnull(EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k HAVING count(*) = 0), false)
        ORDER BY e.k
    """
    // a having clause which references the outer row is evaluated on the aggregation of the whole
    // domain of the outer row: the domain of e.k = 7 is empty, count(*) is 0 and 0 <= 7 - 7 holds
    order_qt_eq_global_having_refs_outer """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k HAVING count(*) <= e.k - 7)
        ORDER BY e.k
    """
    order_qt_eq_global_having_refs_outer_not_exists """
        SELECT e.k FROM ceh_e e
        WHERE NOT EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k HAVING count(*) <= e.k - 7)
        ORDER BY e.k
    """
    // a non equality correlated predicate and a having clause which references the outer row
    order_qt_noneq_global_having_refs_outer """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k < e.k HAVING count(*) >= e.k - 4)
        ORDER BY e.k
    """
    // the domain of one outer row is one group of the grouped aggregate, the having clause of that
    // group is the having clause of the outer row
    order_qt_eq_grouped_having_refs_outer """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k GROUP BY i.g HAVING count(*) = e.k - 1)
        ORDER BY e.k
    """
    // the predicate of the having clause which does not reference the outer row stays below the
    // projection of the subquery and has to be evaluated on the aggregation as well
    order_qt_eq_global_having_refs_outer_partial """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k
            HAVING count(*) <= e.k - 7 AND count(*) >= 0)
        ORDER BY e.k
    """
    order_qt_noneq_global_having_refs_outer_partial """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_i i WHERE i.k < e.k
            HAVING count(*) >= e.k - 4 AND count(*) < 100)
        ORDER BY e.k
    """
    // the group key of the aggregate may be null: the rows of the domain of e.k = 1 form one group
    // whose key is null, and they are the only rows of the domain of e.k = 1
    order_qt_null_group_key """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_n n WHERE n.k = e.k GROUP BY n.g HAVING count(*) = 2)
        ORDER BY e.k
    """
    order_qt_null_group_key_not_exists """
        SELECT e.k FROM ceh_e e
        WHERE NOT EXISTS (SELECT count(*) AS c FROM ceh_n n WHERE n.k = e.k GROUP BY n.g HAVING count(*) = 2)
        ORDER BY e.k
    """
    // a non equality correlated predicate, a null correlation value and a null group key: the
    // aggregation of the domain of e.k = 1 has one group whose key is null, and the rows of the
    // empty domain of e.k = null must not be matched by the null safe key of the back join
    order_qt_noneq_null_group_key_having_refs_outer """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_n n WHERE n.k <= e.k GROUP BY n.g HAVING count(*) >= e.k - 1)
        ORDER BY e.k
    """
    order_qt_noneq_null_group_key_having_refs_outer_not_exists """
        SELECT e.k FROM ceh_e e
        WHERE NOT EXISTS (SELECT count(*) AS c FROM ceh_n n WHERE n.k <= e.k
            GROUP BY n.g HAVING count(*) >= e.k - 1)
        ORDER BY e.k
    """

    // ---------------------------------------------------------------------------------------------
    // regression of the review of the rewrite: the aggregation of one outer row is the aggregation
    // of exactly the inner rows satisfying the correlated predicate, so the predicates which were
    // pulled out of the HAVING clause have to stay on that aggregation (they must not be lost by
    // the fallback of the rule), and the aggregates of the domain of an outer row have to be the
    // aggregates of the subquery (the predicate of the WHERE clause may not be dropped either).
    sql "DROP TABLE IF EXISTS ceh_r_e"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_r_e (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS ceh_r_i"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_r_i (
            k INT NULL,
            g INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k, g)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO ceh_r_e VALUES (3)"
    // the domain of e.k = 3 is one group (g = 20) whose count is 1 and does not satisfy the HAVING
    // clause; the group g = 10 of the whole table has the count 2 which would satisfy it, so the
    // query must not return 3
    sql "INSERT INTO ceh_r_i VALUES (1, 10), (1, 10), (3, 20)"
    order_qt_eq_grouped_having_refs_outer_domain """
        SELECT e.k FROM ceh_r_e e
        WHERE EXISTS (SELECT count(*) AS c FROM ceh_r_i i WHERE i.k = e.k GROUP BY i.g
            HAVING count(*) >= e.k - 1)
        ORDER BY e.k
    """
    order_qt_eq_grouped_having_refs_outer_domain_not_exists """
        SELECT e.k FROM ceh_r_e e
        WHERE NOT EXISTS (SELECT count(*) AS c FROM ceh_r_i i WHERE i.k = e.k GROUP BY i.g
            HAVING count(*) >= e.k - 1)
        ORDER BY e.k
    """
    // the kept row of an empty correlated domain must not be an input of any aggregate: sum(1) of
    // an empty domain is null, while the kept row would evaluate the argument 1
    sql "DROP TABLE IF EXISTS ceh_s_e"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_s_e (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS ceh_s_i"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_s_i (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO ceh_s_e VALUES (1)"
    sql "INSERT INTO ceh_s_i VALUES (2)"
    order_qt_global_sum_of_empty_domain """
        SELECT e.k FROM ceh_s_e e
        WHERE EXISTS (SELECT sum(1) FROM ceh_s_i i WHERE i.k = e.k HAVING sum(1) IS NULL)
        ORDER BY e.k
    """
    // a distinct count of a literal keeps its distinct flag and its argument: two matching inner
    // rows count as one
    sql "DROP TABLE IF EXISTS ceh_d_e"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_d_e (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS ceh_d_i"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_d_i (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO ceh_d_e VALUES (2)"
    sql "INSERT INTO ceh_d_i VALUES (2), (2)"
    order_qt_global_count_distinct_literal """
        SELECT e.k FROM ceh_d_e e
        WHERE EXISTS (SELECT count(DISTINCT 1) AS c FROM ceh_d_i i WHERE i.k = e.k
            HAVING c = e.k - 1)
        ORDER BY e.k
    """
    // a predicate of the HAVING clause which references the outer row only still rejects the row of
    // the aggregation, it may not become a condition of the join of the domain
    sql "DROP TABLE IF EXISTS ceh_f_e"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_f_e (
            k INT NULL,
            flag INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS ceh_f_i"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_f_i (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO ceh_f_e VALUES (1, 0)"
    sql "INSERT INTO ceh_f_i VALUES (1)"
    order_qt_having_predicate_of_outer_row_only """
        SELECT e.k FROM ceh_f_e e
        WHERE EXISTS (SELECT count(*) FROM ceh_f_i i WHERE i.k = e.k HAVING e.flag = 1)
        ORDER BY e.k
    """
    // a volatile column which only decorates the output of the outer query does not change the rows
    // of the outer plan nor the value of a correlation key, so the rewrite may duplicate it
    order_qt_volatile_output_which_does_not_feed_the_correlation """
        SELECT t.k FROM (SELECT e.k AS k, random() AS r FROM ceh_s_e e) t
        WHERE EXISTS (SELECT count(*) FROM ceh_s_i i WHERE i.k < t.k HAVING count(*) = 0)
        ORDER BY t.k
    """
    // a volatile argument of an aggregate whose value neither the HAVING clause nor the subquery
    // reads does not change the result: such a value is not observed by the EXISTS
    order_qt_volatile_output_which_does_not_feed_the_having """
        SELECT t.k FROM (SELECT e.k AS k FROM ceh_s_e e) t
        WHERE EXISTS (SELECT count(*), sum(random()) AS s FROM ceh_s_i i
            WHERE i.k < t.k HAVING count(*) = 0)
        ORDER BY t.k
    """
    // ... while a value which the HAVING clause uses has to be computed for every outer row: the
    // rewrite would compute it once for two outer rows with the same correlation key
    test {
        sql "SELECT t.k FROM (SELECT e.k AS k FROM ceh_s_e e) t" +
                " WHERE EXISTS (SELECT sum(random()) AS s, count(*) FROM ceh_s_i i" +
                " WHERE i.k < t.k HAVING s >= 0)"
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    // the aggregation of the subquery may itself be aggregated (the derived table wraps it): the rows
    // of the subquery for one outer row are the rows of that aggregation for the correlation key of
    // the outer row, so every aggregate above the aggregation of the domain keeps the keys apart
    order_qt_eq_nested_aggregation_having_refs_outer """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT m FROM (SELECT max(c) AS m FROM (SELECT count(*) AS c FROM ceh_i i
            WHERE i.k = e.k GROUP BY i.g) y) z WHERE m <= e.k - 1)
        ORDER BY e.k
    """
    order_qt_eq_nested_aggregation_having_refs_outer_not_exists """
        SELECT e.k FROM ceh_e e
        WHERE NOT EXISTS (SELECT m FROM (SELECT max(c) AS m FROM (SELECT count(*) AS c FROM ceh_i i
            WHERE i.k = e.k GROUP BY i.g) y) z WHERE m <= e.k - 1)
        ORDER BY e.k
    """
    // the max of the derived table of an empty correlated domain is null, and the HAVING clause
    // keeps that row: the row of the aggregation of such a key exists for the subquery, although
    // the aggregation of the domain returns no row of its own for it. The rewrite keeps the row of
    // the key as well and lets the max above the aggregation of the domain return the null of its
    // empty input for it, so that the HAVING clause decides on the row like the original subquery
    // does, while the rewrite without that row would drop the outer row
    order_qt_exists_having_of_the_domain_aggregation """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT max(c) FROM (SELECT count(*) AS c FROM ceh_i i
            WHERE i.k = e.k GROUP BY i.g) x HAVING max(c) IS NULL)
        ORDER BY e.k
    """
    order_qt_not_exists_having_of_the_domain_aggregation """
        SELECT e.k FROM ceh_e e
        WHERE NOT EXISTS (SELECT max(c) FROM (SELECT count(*) AS c FROM ceh_i i
            WHERE i.k = e.k GROUP BY i.g) x HAVING max(c) IS NULL)
        ORDER BY e.k
    """
    // the HAVING clause rejects the row of the empty input (the null of the max does not satisfy
    // max(c) > 0), and the rewrite without a kept row drops that row as well: the two agree
    order_qt_exists_having_which_rejects_the_empty_input """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT max(c) FROM (SELECT count(*) AS c FROM ceh_i i
            WHERE i.k = e.k GROUP BY i.g) x HAVING max(c) > 0)
        ORDER BY e.k
    """
    // the count of the derived table of an empty correlated domain is 0, which the IN subquery
    // compares with the outer value, while an aggregate which reads the rows of the derived table
    // has no group at all for a key whose rows below it are missing when the rewrite adds the
    // correlation keys to its group by. The rewrite keeps a row for such a key and hands the marker
    // of that row to the count in place of the rows it has to count, so that the count of the kept
    // row is the count of the empty input and an outer row whose value is 0 matches the subquery
    order_qt_in_count_of_nested_aggregation """
        SELECT e.k FROM ceh_e e
        WHERE e.k IN (SELECT count(*) FROM (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k GROUP BY i.g) x)
        ORDER BY e.k
    """
    order_qt_in_count_of_nested_aggregation_as_value """
        SELECT e.k, e.k IN (SELECT count(*) FROM
            (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k GROUP BY i.g) x) AS v
        FROM ceh_e e ORDER BY e.k
    """
    // the aggregation of the subquery can only be evaluated for the correlation keys of the outer
    // rows when the two evaluations of the outer plan return the same rows, when the correlation
    // keys of the two evaluations are the same, and when the predicates of the subquery do not have
    // to be evaluated for every outer row: the shapes below are rejected instead of returning a
    // wrong result
    test {
        sql "SELECT t.k FROM (SELECT e.k AS k FROM ceh_s_e e LIMIT 1) t" +
                " WHERE EXISTS (SELECT count(*) FROM ceh_s_i i WHERE i.k < t.k HAVING count(*) = 0)"
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    test {
        sql "SELECT t.k FROM (SELECT random() AS k FROM ceh_s_e e) t" +
                " WHERE EXISTS (SELECT count(*) FROM ceh_s_i i WHERE i.k < t.k HAVING count(*) = 0)"
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    test {
        sql "SELECT e.k FROM ceh_s_e e" +
                " WHERE EXISTS (SELECT array_agg(i.k) FROM ceh_s_i i WHERE i.k = e.k HAVING count(*) = e.k)"
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    // a predicate which the rule pulled out of a filter above the projection of the select list
    // reads a column of that projection, which the aggregation of the rewrite cannot evaluate
    test {
        sql "SELECT e.k FROM ceh_e e" +
                " WHERE EXISTS (SELECT x.c FROM (SELECT count(*) AS c, count(*) + 1 AS d FROM ceh_i i" +
                " WHERE i.k = e.k) x WHERE x.d <= e.k)"
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    // a filter which sits above the HAVING clause of the subquery (its predicate reads a volatile
    // column of the projection of the select list, so filter pushdown cannot push it below that
    // projection) was dropped by the rewrite: the subquery then returned a row for every outer row
    test {
        sql "SELECT e.k FROM ceh_s_e e" +
                " WHERE EXISTS (SELECT x.c FROM (SELECT count(*) AS c, random() AS r FROM ceh_s_i i" +
                " WHERE i.k < e.k HAVING count(*) = 0) x WHERE x.r < -1)"
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    // sum/avg/min/max return null for an empty input: such a HAVING clause rejects the row of the
    // empty correlated domain and the original rewrite stays valid, even when the outer plan cannot
    // be evaluated twice
    sql "DROP TABLE IF EXISTS ceh_n_e"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_n_e (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS ceh_n_i"
    sql """
        CREATE TABLE IF NOT EXISTS ceh_n_i (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO ceh_n_e VALUES (1), (2)"
    sql "INSERT INTO ceh_n_i VALUES (1)"
    order_qt_having_sum_is_not_null_of_the_empty_domain """
        SELECT t.k FROM (SELECT e.k AS k FROM ceh_n_e e ORDER BY e.k LIMIT 1) t
        WHERE EXISTS (SELECT sum(i.k) FROM ceh_n_i i WHERE i.k = t.k HAVING sum(i.k) IS NOT NULL)
        ORDER BY t.k
    """
    // ... while a conjunct which rejects the row of the empty input dominates the conjuncts which
    // are unknown for an empty input (the value of array_agg for an empty input is unknown here)
    order_qt_having_with_a_rejecting_conjunct_dominates_unknown_conjuncts """
        SELECT e.k FROM ceh_n_e e
        WHERE EXISTS (SELECT array_agg(i.k), count(*) FROM ceh_n_i i WHERE i.k = e.k
            HAVING array_agg(i.k) IS NOT NULL AND count(*) = 999)
        ORDER BY e.k
    """
    // the outer plan of an apply can be a table valued function: a deep copy which reuses the slots
    // of the copied relation cannot be used as an independent branch of the rewrite
    test {
        sql 'SELECT n.number FROM numbers("number" = "3") n' +
                " WHERE EXISTS (SELECT count(*) FROM ceh_n_i i WHERE i.k < n.number HAVING count(*) = 0)"
        exception "Unsupported correlated subquery with grouping and/or aggregation"
    }
    // The count of the aggregation of the domain is global, so the subquery computes one row for
    // every outer row, the outer rows whose correlated domain is empty included (the count 0 of the
    // empty derived table), and the count(*) above it groups that row: the group of the count 0 has
    // one row as well, so the EXISTS reports every outer row, the keys of the outer rows without any
    // matching inner row included. The aggregation of the inner side would add the keys to the group
    // by of every aggregate of the chain, so those keys would have no row at all and the semi join
    // would drop their outer rows (a NOT EXISTS would report them instead)
    order_qt_exists_grouping_the_row_of_the_empty_domain """
        SELECT e.k FROM ceh_e e
        WHERE EXISTS (SELECT count(*) FROM (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k) x
            GROUP BY c)
        ORDER BY e.k
    """
    order_qt_not_exists_grouping_the_row_of_the_empty_domain """
        SELECT e.k FROM ceh_e e
        WHERE NOT EXISTS (SELECT count(*) FROM (SELECT count(*) AS c FROM ceh_i i WHERE i.k = e.k) x
            GROUP BY c)
        ORDER BY e.k
    """
    // The grouping sets of the subquery are computed by a repeat node above the aggregation of the
    // domain, and the rewrite which unnests the subquery reads the aggregation of that domain from
    // below the repeat: the grouping sets of a repeat above the correlated predicate would be
    // computed for the rows of every correlation key together, so the subquery is reported
    test {
        sql "SELECT e.k FROM ceh_e e WHERE EXISTS (SELECT count(*) FROM ceh_i i" +
                " WHERE i.k = e.k GROUP BY GROUPING SETS ((i.g), ()))"
        exception "access outer query's column before grouping sets is not supported"
    }
    // The join combines the rows of the domain of an outer row with the rows of its other side, and
    // the rewrite reads the aggregation of that domain from below the join: the aggregation above
    // the join would group the rows of every correlation key together, so the subquery is reported
    // instead of reporting the outer rows whose key another correlation key decides on
    test {
        sql "SELECT e.k FROM ceh_e e WHERE EXISTS (SELECT count(*) FROM" +
                " (SELECT i.k, i.g FROM ceh_i i WHERE i.k = e.k) x JOIN ceh_i j ON x.g = j.g" +
                " GROUP BY x.g)"
        exception "access outer query's column before join is not supported"
    }
}
