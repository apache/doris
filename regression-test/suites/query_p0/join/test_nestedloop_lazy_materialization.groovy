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

suite("test_nestedloop_lazy_materialization") {
    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set disable_join_reorder=true"

    sql "DROP TABLE IF EXISTS test_nestedloop_lazy_materialization_probe"
    sql """
        CREATE TABLE test_nestedloop_lazy_materialization_probe (
            id INT NOT NULL,
            v INT NOT NULL,
            flag INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql "DROP TABLE IF EXISTS test_nestedloop_lazy_materialization_build"
    sql """
        CREATE TABLE test_nestedloop_lazy_materialization_build (
            id INT NOT NULL,
            v INT NOT NULL,
            flag INT NOT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql "DROP TABLE IF EXISTS test_nestedloop_lazy_materialization_null_build"
    sql """
        CREATE TABLE test_nestedloop_lazy_materialization_null_build (
            id INT NOT NULL,
            v INT NULL
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        INSERT INTO test_nestedloop_lazy_materialization_probe VALUES
            (1, 1, 10),
            (2, 3, 20),
            (3, 5, 30),
            (4, 7, 40),
            (5, 200, 50);
    """
    sql """
        INSERT INTO test_nestedloop_lazy_materialization_build VALUES
            (5, 0, 1),
            (10, 2, 1),
            (20, 4, 0),
            (30, 6, 1),
            (40, 8, 0),
            (50, 100, 1);
    """
    sql """
        INSERT INTO test_nestedloop_lazy_materialization_null_build VALUES
            (1, NULL),
            (2, 2),
            (3, 4);
    """
    sql "sync"

    // Eval-only demand: predicate columns are real in the temporary eval block,
    // but no payload column is needed after the join for count(*).
    def countOnly = sql """
        SELECT COUNT(*)
        FROM test_nestedloop_lazy_materialization_probe p
        JOIN test_nestedloop_lazy_materialization_build b
            ON p.v < b.v AND p.flag + b.flag > 10;
    """
    assertEquals("12", countOnly[0][0].toString())

    // Materialize-only payload: p.id is not needed by ON predicate, but is needed
    // after predicate filtering as final output.
    def materializeOnly = sql """
        SELECT p.id
        FROM test_nestedloop_lazy_materialization_probe p
        JOIN test_nestedloop_lazy_materialization_build b
            ON p.v < b.v AND p.flag + b.flag > 10
        ORDER BY p.id, b.id;
    """
    assertEquals([[1], [1], [1], [2], [2], [2], [2], [3], [3], [3], [4], [4]],
            materializeOnly)

    // Overlap demand: p.v and b.v are needed both by ON predicate and by output.
    def evalAndMaterialize = sql """
        SELECT p.v, b.v
        FROM test_nestedloop_lazy_materialization_probe p
        JOIN test_nestedloop_lazy_materialization_build b
            ON p.v < b.v AND b.flag = 1
        WHERE p.v < 4
        ORDER BY p.v, b.v;
    """
    assertEquals([[1, 2], [1, 6], [1, 100], [3, 6], [3, 100]], evalAndMaterialize)

    // Post-join filter demand: b.flag is not required by ON predicate here, but
    // it must be materialized because WHERE is evaluated on the joined block.
    def postJoinFilter = sql """
        SELECT p.id, b.id
        FROM test_nestedloop_lazy_materialization_probe p
        LEFT OUTER JOIN test_nestedloop_lazy_materialization_build b
            ON p.v < b.v
        WHERE b.flag = 1
        ORDER BY p.id, b.id;
    """
    assertEquals([[1, 10], [1, 30], [1, 50], [2, 30], [2, 50], [3, 30], [3, 50], [4, 50]],
            postJoinFilter)

    // Nullable eval demand: RIGHT/FULL outer joins make probe-side intermediate
    // slots nullable, while the source probe block column is still non-nullable.
    def rightOuterNullableEval = sql """
        SELECT b.id
        FROM test_nestedloop_lazy_materialization_probe p
        RIGHT OUTER JOIN test_nestedloop_lazy_materialization_build b
            ON p.v < b.v
        ORDER BY b.id, p.id;
    """
    assertEquals([[5], [10], [20], [20], [30], [30], [30], [40], [40], [40], [40],
            [50], [50], [50], [50]], rightOuterNullableEval)

    // Build-side and probe-side finalization should be resumable across output
    // batches when lazy FULL OUTER JOIN has many unmatched rows.
    def fullOuterResume = sql """
        SELECT /*+SET_VAR(batch_size=2)*/ COALESCE(p.id, -1), COALESCE(b.id, -1)
        FROM test_nestedloop_lazy_materialization_probe p
        FULL OUTER JOIN test_nestedloop_lazy_materialization_build b
            ON p.v + 100 < b.v
        ORDER BY 1, 2;
    """
    assertEquals([[-1, 5], [-1, 10], [-1, 20], [-1, 30], [-1, 40], [-1, 50],
            [1, -1], [2, -1], [3, -1], [4, -1], [5, -1]], fullOuterResume)

    // Probe-side matched tracking and finalization for lazy LEFT SEMI JOIN.
    def leftSemi = sql """
        SELECT p.id
        FROM test_nestedloop_lazy_materialization_probe p
        LEFT SEMI JOIN test_nestedloop_lazy_materialization_build b
            ON p.v < b.v AND b.flag = 1
        ORDER BY p.id;
    """
    assertEquals([[1], [2], [3], [4]], leftSemi)

    // Probe-side unmatched tracking and finalization for lazy LEFT ANTI JOIN.
    def leftAnti = sql """
        SELECT p.id
        FROM test_nestedloop_lazy_materialization_probe p
        LEFT ANTI JOIN test_nestedloop_lazy_materialization_build b
            ON p.v < b.v AND b.flag = 1
        ORDER BY p.id;
    """
    assertEquals([[5]], leftAnti)

    // MARK LEFT SEMI lazy finalization: EXISTS is represented as a mark value
    // because it is combined with an OR predicate.
    def markLeftSemi = sql """
        SELECT p.id
        FROM test_nestedloop_lazy_materialization_probe p
        WHERE EXISTS (
            SELECT *
            FROM test_nestedloop_lazy_materialization_build b
            WHERE p.v < b.v AND b.flag = 1
        ) OR p.id = 5
        ORDER BY p.id;
    """
    assertEquals([[1], [2], [3], [4], [5]], markLeftSemi)

    // MARK LEFT ANTI lazy finalization: NOT EXISTS inverts the mark value for
    // rows without a successful predicate match.
    def markLeftAnti = sql """
        SELECT p.id
        FROM test_nestedloop_lazy_materialization_probe p
        WHERE NOT EXISTS (
            SELECT *
            FROM test_nestedloop_lazy_materialization_build b
            WHERE p.v < b.v AND b.flag = 1
        ) OR p.id = 1
        ORDER BY p.id;
    """
    assertEquals([[1], [5]], markLeftAnti)

    // MARK NULL state: IN can produce NULL when the correlated subquery contains
    // NULL and no equal value, and IS NULL observes that mark state.
    def markNull = sql """
        SELECT p.id
        FROM test_nestedloop_lazy_materialization_probe p
        WHERE (p.v IN (
            SELECT b.v
            FROM test_nestedloop_lazy_materialization_null_build b
            WHERE p.id > b.id
        )) IS NULL
        ORDER BY p.id;
    """
    assertEquals([[2], [3], [4], [5]], markNull)

    // NULL-aware anti join lazy finalization: the NULL in the correlated subquery
    // result should make NOT IN unknown for rows whose subquery sees it.
    def nullAwareAnti = sql """
        SELECT p.id
        FROM test_nestedloop_lazy_materialization_probe p
        WHERE p.v NOT IN (
            SELECT b.v
            FROM test_nestedloop_lazy_materialization_null_build b
            WHERE p.id > b.id
        )
        ORDER BY p.id;
    """
    assertEquals([[1]], nullAwareAnti)

    // Probe-side finalization must not consume a probe row when the output
    // batch is already full and the build side is empty.
    def leftOuterEmptyBuild = sql """
        SELECT /*+SET_VAR(batch_size=2)*/ p.id, COALESCE(b.id, -1)
        FROM test_nestedloop_lazy_materialization_probe p
        LEFT OUTER JOIN (
            SELECT *
            FROM test_nestedloop_lazy_materialization_build
            WHERE id < 0
        ) b
            ON p.v < b.v
        ORDER BY p.id;
    """
    assertEquals([[1, -1], [2, -1], [3, -1], [4, -1], [5, -1]], leftOuterEmptyBuild)

    def leftAntiEmptyBuild = sql """
        SELECT /*+SET_VAR(batch_size=2)*/ p.id
        FROM test_nestedloop_lazy_materialization_probe p
        LEFT ANTI JOIN (
            SELECT *
            FROM test_nestedloop_lazy_materialization_build
            WHERE id < 0
        ) b
            ON p.v < b.v
        ORDER BY p.id;
    """
    assertEquals([[1], [2], [3], [4], [5]], leftAntiEmptyBuild)
}
