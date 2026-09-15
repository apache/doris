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

suite("scalar_subquery_having") {
    // An uncorrelated scalar subquery which aggregates returns one row per outer row: its
    // aggregation, filtered by the HAVING clause. When the HAVING clause removes the row of the
    // aggregation, the scalar subquery is null, but the outer row is kept: the assertion which
    // implements the subquery has to build that null row, so it must not be eliminated above a
    // filter.
    sql "DROP TABLE IF EXISTS sqh_o"
    sql """
        CREATE TABLE IF NOT EXISTS sqh_o (
            id INT NULL,
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "DROP TABLE IF EXISTS sqh_i"
    sql """
        CREATE TABLE IF NOT EXISTS sqh_i (
            k INT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(k)
        DISTRIBUTED BY HASH(k) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql "INSERT INTO sqh_o VALUES (7, 2)"
    sql "INSERT INTO sqh_i VALUES (1), (2), (3)"

    // the aggregation is 3, so the HAVING clause of the first query removes the row of the
    // aggregation and the scalar subquery of the outer row is null
    order_qt_agg_having_false """
        SELECT o.id, (SELECT COUNT(*) FROM sqh_i WHERE k > 0 HAVING COUNT(*) > 5) AS s
        FROM sqh_o o ORDER BY o.id
    """
    order_qt_agg_having_true """
        SELECT o.id, (SELECT COUNT(*) FROM sqh_i WHERE k > 0 HAVING COUNT(*) > 1) AS s
        FROM sqh_o o ORDER BY o.id
    """
    // an empty input of the aggregation is also removed by the HAVING clause
    order_qt_agg_having_false_empty_input """
        SELECT o.id, (SELECT COUNT(*) FROM sqh_i WHERE k > 100 HAVING COUNT(*) > 1) AS s
        FROM sqh_o o ORDER BY o.id
    """
    order_qt_agg_having_equals """
        SELECT o.id, (SELECT SUM(k) FROM sqh_i HAVING COUNT(*) = 3) AS s
        FROM sqh_o o ORDER BY o.id
    """
    // without a HAVING clause the aggregation is the value of the scalar subquery
    order_qt_agg_without_having """
        SELECT o.id, (SELECT COUNT(*) FROM sqh_i WHERE k > 1) AS s
        FROM sqh_o o ORDER BY o.id
    """
    // the null value of a scalar subquery without any row, and the controls of the assertion
    order_qt_scalar_without_rows """
        SELECT o.id, (SELECT k FROM sqh_i WHERE k > 100) AS s
        FROM sqh_o o ORDER BY o.id
    """
    order_qt_scalar_with_one_row """
        SELECT o.id, (SELECT k FROM sqh_i WHERE k = 2) AS s
        FROM sqh_o o ORDER BY o.id
    """
    test {
        sql "SELECT o.id, (SELECT k FROM sqh_i) AS s FROM sqh_o o"
        exception "Expected EQ 1 to be returned by expression"
    }
    test {
        sql "SELECT o.id, (SELECT k FROM sqh_i GROUP BY k) AS s FROM sqh_o o"
        exception "Expected EQ 1 to be returned by expression"
    }
    // the assertion of a scalar subquery which only aggregates without a HAVING clause is still
    // redundant: the aggregation always returns exactly one row
    order_qt_agg_redundant_assertion """
        SELECT o.id, (SELECT COUNT(*) FROM sqh_i) AS s FROM sqh_o o ORDER BY o.id
    """
}
