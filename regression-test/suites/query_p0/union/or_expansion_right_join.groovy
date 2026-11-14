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

suite("or_expansion_right_join", "p0") {
    // Keep RIGHT joins through RBO: SemiJoinCommute would otherwise normalize them to LEFT joins.
    sql "SET disable_join_reorder = true"
    sql "DROP TABLE IF EXISTS or_expansion_right_join_l"
    sql "DROP TABLE IF EXISTS or_expansion_right_join_r"
    sql """CREATE TABLE or_expansion_right_join_l (id INT NOT NULL, a INT, b INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    sql """CREATE TABLE or_expansion_right_join_r (id BIGINT NOT NULL, a BIGINT, b BIGINT, v INT)
        DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")"""
    // Include duplicate matches, overlapping OR branches, UNKNOWN predicates and unmatched rows.
    sql """INSERT INTO or_expansion_right_join_l VALUES
        (1, 1, 10), (2, 2, 20), (2, 2, 20), (3, NULL, 30),
        (4, 4, NULL), (5, NULL, NULL), (6, 6, 60)"""
    sql """INSERT INTO or_expansion_right_join_r VALUES
        (101, 1, 10, 1), (102, 9, 20, 2), (103, NULL, 30, 3),
        (104, 4, NULL, 4), (105, NULL, NULL, 5), (106, 7, 70, 6), (106, 7, 70, 6)"""

    for (String joinType : ["RIGHT OUTER", "RIGHT ANTI"]) {
        for (String condition : [
                "l.a = r.a OR l.b = r.b",
                "(l.a = r.a OR l.b = r.b) AND l.id + r.v < 6",
                "(l.a + 1 = r.a OR l.b = r.b + 1 OR r.a + 2 = l.b) AND l.id < r.id",
                "l.a <=> r.a OR l.b = r.b"]) {
            String query = """SELECT %s * FROM or_expansion_right_join_l l
                ${joinType} JOIN or_expansion_right_join_r r ON ${condition}"""
            explain {
                sql String.format(query, "")
                contains "VHASH JOIN"
                notContains "VNESTED LOOP JOIN"
            }
            explain {
                sql String.format(query, "/*+ SET_VAR(disable_nereids_rules='OR_EXPANSION') */")
                contains "${joinType} JOIN"
                contains "VNESTED LOOP JOIN"
                notContains "VHASH JOIN"
            }
            order_qt_enabled String.format(query, "")
            order_qt_disabled String.format(query, "/*+ SET_VAR(disable_nereids_rules='OR_EXPANSION') */")
        }

        // Enlarging OrExpansion's supported types also enables CASE extraction for right joins.
        String caseQuery = """SELECT %s * FROM or_expansion_right_join_l l
            ${joinType} JOIN or_expansion_right_join_r r
            ON CASE WHEN r.v < 3 THEN l.a ELSE l.b END = r.a"""
        explain {
            sql String.format(caseQuery, "")
            contains "VHASH JOIN"
            notContains "VNESTED LOOP JOIN"
        }
        explain {
            sql String.format(caseQuery,
                    "/*+ SET_VAR(disable_nereids_rules='OR_EXPANSION,JOIN_EXTRACT_OR_FROM_CASE_WHEN') */")
            contains "${joinType} JOIN"
            contains "VNESTED LOOP JOIN"
            notContains "VHASH JOIN"
        }
        order_qt_case_enabled String.format(caseQuery, "")
        order_qt_case_disabled String.format(caseQuery,
                "/*+ SET_VAR(disable_nereids_rules='OR_EXPANSION,JOIN_EXTRACT_OR_FROM_CASE_WHEN') */")
    }

    // Exercise the full rewrite pipeline with aggregation above the right join.
    order_qt_aggregate """SELECT r.id, COUNT(l.id), SUM(l.b), SUM(r.v)
        FROM or_expansion_right_join_l l RIGHT OUTER JOIN or_expansion_right_join_r r
        ON l.a = r.a OR l.b = r.b GROUP BY r.id"""
    order_qt_aggregate_disabled """SELECT /*+ SET_VAR(disable_nereids_rules='OR_EXPANSION') */
        r.id, COUNT(l.id), SUM(l.b), SUM(r.v)
        FROM or_expansion_right_join_l l RIGHT OUTER JOIN or_expansion_right_join_r r
        ON l.a = r.a OR l.b = r.b GROUP BY r.id"""

    // Test empty inputs on each side while keeping an equijoin OR predicate.
    for (String joinType : ["RIGHT OUTER", "RIGHT ANTI"]) {
        for (String emptySide : ["l", "r"]) {
            String left = emptySide == "l"
                    ? "(SELECT * FROM or_expansion_right_join_l WHERE id < 0)" : "or_expansion_right_join_l"
            String right = emptySide == "r"
                    ? "(SELECT * FROM or_expansion_right_join_r WHERE id < 0)" : "or_expansion_right_join_r"
            String query = """SELECT %s * FROM ${left} l ${joinType} JOIN ${right} r
                ON l.a = r.a OR l.b = r.b"""
            order_qt_empty_enabled String.format(query, "")
            order_qt_empty_disabled String.format(query, "/*+ SET_VAR(disable_nereids_rules='OR_EXPANSION') */")
        }
    }
}
