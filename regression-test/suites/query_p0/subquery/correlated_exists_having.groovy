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
}
