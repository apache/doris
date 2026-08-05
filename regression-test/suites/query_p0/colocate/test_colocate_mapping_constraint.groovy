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

suite("test_colocate_mapping_constraint") {
    sql """ DROP TABLE IF EXISTS test_colocate_mapping_constraint_left """
    sql """ DROP TABLE IF EXISTS test_colocate_mapping_constraint_right """
    sql """ DROP TABLE IF EXISTS test_colocate_mapping_composite_left """
    sql """ DROP TABLE IF EXISTS test_colocate_mapping_composite_right """

    sql """
        CREATE TABLE test_colocate_mapping_constraint_left (
            k1 INT,
            k2 INT,
            d1 INT,
            d2 INT,
            extra_col INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1, k2) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "test_colocate_mapping_constraint_group"
        )
    """
    sql """
        CREATE TABLE test_colocate_mapping_constraint_right (
            k1 INT,
            k2 INT,
            d1 INT,
            d2 INT,
            extra_col INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1, k2) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "test_colocate_mapping_constraint_group"
        )
    """

    sql """
        ALTER TABLE test_colocate_mapping_constraint_left
        ADD CONSTRAINT left_mapping_1
        COLOCATE MAPPING mapping_1 (d1) DETERMINES DISTRIBUTION KEY (k1) NOT ENFORCED
    """
    sql """
        ALTER TABLE test_colocate_mapping_constraint_right
        ADD CONSTRAINT right_mapping_1
        COLOCATE MAPPING mapping_1 (d1) DETERMINES DISTRIBUTION KEY (k1) NOT ENFORCED
    """
    sql """
        ALTER TABLE test_colocate_mapping_constraint_left
        ADD CONSTRAINT left_mapping_2
        COLOCATE MAPPING mapping_2 (d2) DETERMINES DISTRIBUTION KEY (k2) NOT ENFORCED
    """
    sql """
        ALTER TABLE test_colocate_mapping_constraint_right
        ADD CONSTRAINT right_mapping_2
        COLOCATE MAPPING mapping_2 (d2) DETERMINES DISTRIBUTION KEY (k2) NOT ENFORCED
    """
    test {
        sql """
            ALTER TABLE test_colocate_mapping_constraint_left
            DROP COLUMN D1
        """
        exception "left_mapping_1"
    }
    test {
        sql """
            ALTER TABLE test_colocate_mapping_constraint_left
            RENAME COLUMN D1 renamed_d1
        """
        exception "left_mapping_1"
    }
    sql """
        CREATE TABLE test_colocate_mapping_composite_left (
            k1 INT,
            k2 INT,
            d1 INT,
            d2 INT,
            extra_col INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1, k2) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "test_colocate_mapping_composite_group"
        )
    """
    sql """
        CREATE TABLE test_colocate_mapping_composite_right (
            k1 INT,
            k2 INT,
            d1 INT,
            d2 INT,
            extra_col INT
        ) ENGINE=OLAP
        DUPLICATE KEY(k1, k2)
        DISTRIBUTED BY HASH(k1, k2) BUCKETS 4
        PROPERTIES (
            "replication_num" = "1",
            "colocate_with" = "test_colocate_mapping_composite_group"
        )
    """
    sql """
        ALTER TABLE test_colocate_mapping_composite_left
        ADD CONSTRAINT composite_left_mapping
        COLOCATE MAPPING composite_mapping (d1, d2)
        DETERMINES DISTRIBUTION KEY (k1) NOT ENFORCED
    """
    sql """
        ALTER TABLE test_colocate_mapping_composite_right
        ADD CONSTRAINT composite_right_mapping
        COLOCATE MAPPING composite_mapping (d1, d2)
        DETERMINES DISTRIBUTION KEY (k1) NOT ENFORCED
    """

    sql """ INSERT INTO test_colocate_mapping_constraint_left VALUES
            (1, 10, 100, 1000, 7), (2, 20, 200, 2000, 8) """
    sql """ INSERT INTO test_colocate_mapping_constraint_right VALUES
            (1, 10, 100, 1000, 7), (2, 20, 200, 2000, 9) """
    sql """ INSERT INTO test_colocate_mapping_composite_left VALUES
            (1, 10, 100, 1000, 7), (2, 20, 200, 2000, 8) """
    sql """ INSERT INTO test_colocate_mapping_composite_right VALUES
            (1, 10, 100, 1000, 7), (2, 20, 200, 2000, 9) """
    sql """ SYNC """
    createMV("""
        CREATE MATERIALIZED VIEW mapping_alias_rollup AS
        SELECT k1 AS alias_k1, k2 AS alias_k2, d1 AS alias_d1
        FROM test_colocate_mapping_constraint_left
    """)
    createMV("""
        CREATE MATERIALIZED VIEW mapping_without_determinant_rollup AS
        SELECT k1 AS no_determinant_k1, k2 AS no_determinant_k2,
               extra_col AS no_determinant_extra
        FROM test_colocate_mapping_constraint_left
    """)
    waitForColocateGroupStable("test_colocate_mapping_constraint_group")
    waitForColocateGroupStable("test_colocate_mapping_composite_group")

    sql """ SET auto_broadcast_join_threshold = -1 """
    sql """ SET broadcast_row_count_limit = 0 """
    // A selected rollup Slot alias is recognized, then conservatively falls back under the
    // current selected-rollup boundary. The base-provenance binding is asserted by FE UT.
    explain {
        sql """
            SELECT /*+ use_mv(test_colocate_mapping_constraint_left.mapping_alias_rollup) */
                   l.d1, r.d1
            FROM test_colocate_mapping_constraint_left l
            JOIN test_colocate_mapping_constraint_left r
              ON l.d1 = r.d1 AND l.k2 = r.k2
        """
        contains "test_colocate_mapping_constraint_left(mapping_alias_rollup)"
        notContains "COLOCATE"
    }
    // A selected rollup without the determinant cannot propagate the mapping proof.
    explain {
        sql """
            SELECT /*+ use_mv(test_colocate_mapping_constraint_left.mapping_without_determinant_rollup) */
                   l.extra_col, r.extra_col
            FROM test_colocate_mapping_constraint_left l
            JOIN test_colocate_mapping_constraint_left r
              ON l.extra_col = r.extra_col AND l.k2 = r.k2
        """
        contains "test_colocate_mapping_constraint_left(mapping_without_determinant_rollup)"
        notContains "COLOCATE"
    }
    def nestedSubqueryJoinQueries = [
        // Parallel Project subqueries on both sides.
        """
            SELECT *
            FROM (
                SELECT d1, k2, extra_col
                FROM test_colocate_mapping_constraint_left
            ) l
            JOIN (
                SELECT d1, k2, extra_col
                FROM test_colocate_mapping_constraint_right
            ) r
              ON l.d1 = r.d1 AND l.k2 = r.k2
        """,
        // Parallel Aggregate subqueries on both sides.
        """
            SELECT *
            FROM (
                SELECT d1, k2, SUM(extra_col) AS sum_extra
                FROM test_colocate_mapping_constraint_left
                GROUP BY k1, k2, d1
            ) l
            JOIN (
                SELECT d1, k2, SUM(extra_col) AS sum_extra
                FROM test_colocate_mapping_constraint_right
                GROUP BY k1, k2, d1
            ) r
              ON l.d1 = r.d1 AND l.k2 = r.k2
        """,
        // Multiple nested Project subqueries on both sides.
        """
            SELECT *
            FROM (
                SELECT inner_l.d1 AS nested_d1, inner_l.k2 AS nested_k2
                FROM (
                    SELECT d1, k2
                    FROM test_colocate_mapping_constraint_left
                ) inner_l
            ) l
            JOIN (
                SELECT inner_r.d1 AS nested_d1, inner_r.k2 AS nested_k2
                FROM (
                    SELECT d1, k2
                    FROM test_colocate_mapping_constraint_right
                ) inner_r
            ) r
              ON l.nested_d1 = r.nested_d1 AND l.nested_k2 = r.nested_k2
        """,
        // Aggregate subqueries followed by another Project layer on both sides.
        """
            SELECT *
            FROM (
                SELECT aggregate_l.d1 AS nested_d1,
                       aggregate_l.k2 AS nested_k2,
                       aggregate_l.sum_extra
                FROM (
                    SELECT d1, k2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1
                ) aggregate_l
            ) l
            JOIN (
                SELECT aggregate_r.d1 AS nested_d1,
                       aggregate_r.k2 AS nested_k2,
                       aggregate_r.sum_extra
                FROM (
                    SELECT d1, k2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_right
                    GROUP BY k1, k2, d1
                ) aggregate_r
            ) r
              ON l.nested_d1 = r.nested_d1 AND l.nested_k2 = r.nested_k2
        """
    ]

    sql """ SET enable_colocate_mapping_constraint = false """
    nestedSubqueryJoinQueries.each { query ->
        explain {
            sql query
            notContains "COLOCATE"
        }
    }
    // The feature switch must not affect the original direct distribution-key colocate path.
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.k1 = r.k1 AND l.k2 = r.k2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.d2 = r.d2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 AND l.extra_col = r.extra_col """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT l.d1, l.k2, SUM(l.extra_col + r.extra_col)
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2
                GROUP BY l.d1, l.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT l.d1, r.d1
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1 AS aggregate_k1,
                           k2 AS aggregate_k2,
                           d1 AS aggregate_d1,
                           SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.aggregate_d1 = r.d1 AND l.aggregate_k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1 AS aggregate_k1,
                           k2 AS aggregate_k2,
                           d1 AS aggregate_d1,
                           d2 AS aggregate_d2,
                           SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1, d2
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.aggregate_d1 = r.d1 AND l.aggregate_d2 = r.d2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k2 AS aggregate_k2,
                           d1 AS aggregate_d1,
                           SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.aggregate_d1 = r.d1 AND l.aggregate_k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1 AS aggregate_d1,
                           d2 AS aggregate_d2,
                           SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1, d2
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.aggregate_d1 = r.d1 AND l.aggregate_d2 = r.d2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1, d2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY d1, d2
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.d2 = r.d2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1, d2, k2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_composite_left
                    GROUP BY d1, d2, k2
                ) l
                JOIN test_colocate_mapping_composite_right r
                  ON l.d1 = r.d1 AND l.d2 = r.d2 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k2, d1, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k2, d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1, k2, d1, COUNT(DISTINCT extra_col) AS distinct_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k2, d1, COUNT(DISTINCT extra_col) AS distinct_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k2, d1
                ) l
                JOIN (
                    SELECT k2, d1, COUNT(DISTINCT extra_col) AS distinct_extra
                    FROM test_colocate_mapping_constraint_right
                    GROUP BY k2, d1
                ) r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1, k2, d1, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY GROUPING SETS ((k1, k2, d1), (k1, k2))
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1, k2, d1 + 0 AS d1_expression, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1 + 0
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1_expression = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1, k2, d1
                    FROM test_colocate_mapping_constraint_left
                    UNION ALL
                    SELECT k1, k2, d1
                    FROM test_colocate_mapping_constraint_left
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }

    sql """ SET enable_colocate_mapping_constraint = true """
    nestedSubqueryJoinQueries.each { query ->
        explain {
            sql query
            contains "COLOCATE"
        }
    }
    // Cases supported before Aggregate propagation.
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.k1 = r.k1 AND l.k2 = r.k2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        contains "COLOCATE"
    }
    ["LEFT", "RIGHT", "FULL"].each { outerJoinType ->
        explain {
            sql """ SELECT *
                    FROM test_colocate_mapping_constraint_left l
                    ${outerJoinType} OUTER JOIN test_colocate_mapping_constraint_right r
                      ON l.d1 = r.d1 AND l.k2 = r.k2 """
            contains "COLOCATE"
        }
    }
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.d2 = r.d2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 AND l.extra_col = r.extra_col """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT l.d1, l.k2, SUM(l.extra_col + r.extra_col)
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2
                GROUP BY l.d1, l.k2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT l.d1, r.d1
                FROM test_colocate_mapping_constraint_left l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        contains "COLOCATE"
    }
    // Cases supported by the first conservative Aggregate propagation.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1 AS aggregate_k1,
                           k2 AS aggregate_k2,
                           d1 AS aggregate_d1,
                           SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.aggregate_d1 = r.d1 AND l.aggregate_k2 = r.k2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1 AS aggregate_k1,
                           k2 AS aggregate_k2,
                           d1 AS aggregate_d1,
                           d2 AS aggregate_d2,
                           SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1, d2
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.aggregate_d1 = r.d1 AND l.aggregate_d2 = r.d2 """
        contains "COLOCATE"
    }
    // Cases supported after carrying hidden natural bucket locality.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k2 AS aggregate_k2,
                           d1 AS aggregate_d1,
                           SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.aggregate_d1 = r.d1 AND l.aggregate_k2 = r.k2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1 AS aggregate_d1,
                           d2 AS aggregate_d2,
                           SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1, d2
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.aggregate_d1 = r.d1 AND l.aggregate_d2 = r.d2 """
        contains "COLOCATE"
    }
    // Multiple mappings can replace every distribution key in Aggregate Group By.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1, d2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY d1, d2
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.d2 = r.d2 """
        contains "COLOCATE"
    }
    // A composite mapping determinant must be complete.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1, d2, k2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_composite_left
                    GROUP BY d1, d2, k2
                ) l
                JOIN test_colocate_mapping_composite_right r
                  ON l.d1 = r.d1 AND l.d2 = r.d2 AND l.k2 = r.k2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1, k2, MAX(d2) AS d2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_composite_left
                    GROUP BY d1, k2
                ) l
                JOIN test_colocate_mapping_composite_right r
                  ON l.d1 = r.d1 AND l.d2 = r.d2 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    // Distinct Aggregate can preserve locality when the selected physical path does not redistribute data.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1, k2, d1, COUNT(DISTINCT extra_col) AS distinct_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k2, d1, COUNT(DISTINCT extra_col) AS distinct_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k2, d1
                ) l
                JOIN (
                    SELECT k2, d1, COUNT(DISTINCT extra_col) AS distinct_extra
                    FROM test_colocate_mapping_constraint_right
                    GROUP BY k2, d1
                ) r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1, MAX(k2) AS k2, COUNT(DISTINCT extra_col) AS distinct_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    // Mapping determinants can cover distribution keys that are absent from Group By.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k2, d1, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k2, d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        contains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k2, d1, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k2, d1
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1
                 AND l.k2 = r.k2
                 AND l.sum_extra = r.extra_col """
        contains "COLOCATE"
    }
    // Unsupported Aggregate shapes must discard mapping locality.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1, k2, d1, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY GROUPING SETS ((k1, k2, d1), (k1, k2))
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1, k2, d1 + 0 AS d1_expression, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY k1, k2, d1 + 0
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1_expression = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    // A redistribution before Aggregate cuts the storage bucket locality.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1, k2, SUM(extra_col) AS sum_extra
                    FROM (
                        SELECT d1, k2, extra_col
                        FROM test_colocate_mapping_constraint_left
                        ORDER BY extra_col
                        LIMIT 10
                    ) ordered_l
                    GROUP BY d1, k2
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    // Removing the determinant from Aggregate output prevents the parent Join proof.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY d1, k2
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.sum_extra = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    explain {
        sql """ SELECT *
                FROM (
                    SELECT k1, k2, d1
                    FROM test_colocate_mapping_constraint_left
                    UNION ALL
                    SELECT k1, k2, d1
                    FROM test_colocate_mapping_constraint_left
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }
    // Union does not merge the natural mapping locality of Aggregate branches.
    explain {
        sql """ SELECT *
                FROM (
                    SELECT d1, k2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY d1, k2
                    UNION ALL
                    SELECT d1, k2, SUM(extra_col) AS sum_extra
                    FROM test_colocate_mapping_constraint_left
                    GROUP BY d1, k2
                ) l
                JOIN test_colocate_mapping_constraint_right r
                  ON l.d1 = r.d1 AND l.k2 = r.k2 """
        notContains "COLOCATE"
    }

    order_qt_colocate_mapping_result """
        SELECT l.k1, l.k2, l.d1, l.d2, l.extra_col,
               r.k1, r.k2, r.d1, r.d2, r.extra_col
        FROM test_colocate_mapping_constraint_left l
        JOIN test_colocate_mapping_constraint_right r
          ON l.d1 = r.d1 AND l.k2 = r.k2
        ORDER BY l.k1, l.k2
    """

    order_qt_aggregate_colocate_mapping_result """
        SELECT l.aggregate_k1, l.aggregate_k2, l.aggregate_d1, l.sum_extra,
               r.k1, r.k2, r.d1
        FROM (
            SELECT k1 AS aggregate_k1,
                   k2 AS aggregate_k2,
                   d1 AS aggregate_d1,
                   SUM(extra_col) AS sum_extra
            FROM test_colocate_mapping_constraint_left
            GROUP BY k1, k2, d1
        ) l
        JOIN test_colocate_mapping_constraint_right r
          ON l.aggregate_d1 = r.d1 AND l.aggregate_k2 = r.k2
        ORDER BY l.aggregate_k1, l.aggregate_k2
    """

    order_qt_hidden_distribution_key_aggregate_result """
        SELECT l.aggregate_k2, l.aggregate_d1, l.sum_extra,
               r.k1, r.k2, r.d1
        FROM (
            SELECT k2 AS aggregate_k2,
                   d1 AS aggregate_d1,
                   SUM(extra_col) AS sum_extra
            FROM test_colocate_mapping_constraint_left
            GROUP BY k1, k2, d1
        ) l
        JOIN test_colocate_mapping_constraint_right r
          ON l.aggregate_d1 = r.d1 AND l.aggregate_k2 = r.k2
        ORDER BY l.aggregate_k2, l.aggregate_d1
    """

    sql """ DROP TABLE test_colocate_mapping_constraint_left """
    sql """ RECOVER TABLE test_colocate_mapping_constraint_left """
    waitForColocateGroupStable("test_colocate_mapping_constraint_group")
    explain {
        sql """
            SELECT *
            FROM test_colocate_mapping_constraint_left l
            JOIN test_colocate_mapping_constraint_right r
              ON l.d1 = r.d1 AND l.k2 = r.k2
        """
        contains "COLOCATE"
    }
}
