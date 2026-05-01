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

// Test that ANN queries work with various constant expression forms for the
// query vector, not just direct array literals or CAST expressions.
// Covers: array_repeat, array_with_constant, nested function calls, etc.
suite("ann_const_expr_vector") {
    sql "unset variable all;"
    sql "set enable_common_expr_pushdown=true;"

    def tableName = "ann_const_expr_tbl"

    sql "drop table if exists ${tableName}"
    sql """
        CREATE TABLE ${tableName} (
            id INT NOT NULL,
            embedding ARRAY<FLOAT> NOT NULL,
            INDEX idx_emb (`embedding`) USING ANN PROPERTIES(
                "index_type"="hnsw",
                "metric_type"="l2_distance",
                "dim"="4"
            )
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS AUTO
        PROPERTIES ("replication_num" = "1");
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, [0.01, 0.01, 0.01, 0.01]),
        (2, [1.0, 1.0, 1.0, 1.0]),
        (3, [5.0, 5.0, 5.0, 5.0]),
        (4, [10.0, 10.0, 10.0, 10.0]);
    """


    // =========================================================================
    // Test 1: array_repeat - generates a constant array via function call
    // This is the key test case: array_repeat produces a ColumnConst wrapping
    // an array, which the old code rejected.
    // =========================================================================
    qt_array_repeat """
        SELECT id, l2_distance_approximate(embedding, array_repeat(CAST(0.01 AS FLOAT), 4)) AS score
        FROM ${tableName}
        ORDER BY score ASC
        LIMIT 3;
    """

    // =========================================================================
    // Test 2: array_with_constant - another way to produce constant arrays
    // =========================================================================
    qt_array_with_constant """
        SELECT id, l2_distance_approximate(embedding, array_with_constant(4, CAST(0.01 AS FLOAT))) AS score
        FROM ${tableName}
        ORDER BY score ASC
        LIMIT 3;
    """

    // =========================================================================
    // Test 3: Direct array literal (baseline, should always work)
    // =========================================================================
    qt_direct_literal """
        SELECT id, l2_distance_approximate(embedding, [0.01, 0.01, 0.01, 0.01]) AS score
        FROM ${tableName}
        ORDER BY score ASC
        LIMIT 3;
    """

    // =========================================================================
    // Test 4: CAST string to array (existing functionality)
    // =========================================================================
    qt_cast_string """
        SELECT id, l2_distance_approximate(embedding, cast('[0.01,0.01,0.01,0.01]' as array<float>)) AS score
        FROM ${tableName}
        ORDER BY score ASC
        LIMIT 3;
    """

    // =========================================================================
    // Test 5: Error case - array_repeat with wrong dimension
    // =========================================================================
    test {
        sql """
            SELECT id FROM ${tableName}
            ORDER BY l2_distance_approximate(embedding, array_repeat(CAST(0.01 AS FLOAT), 3))
            LIMIT 1;
        """
        exception "[INVALID_ARGUMENT]"
    }

    // =========================================================================
    // Test 6: Error case - empty array via array_repeat
    // =========================================================================
    test {
        sql """
            SELECT id FROM ${tableName}
            ORDER BY l2_distance_approximate(embedding, array_repeat(CAST(0.01 AS FLOAT), 0))
            LIMIT 1;
        """
        exception "Ann topn query vector cannot be empty"
    }
}
