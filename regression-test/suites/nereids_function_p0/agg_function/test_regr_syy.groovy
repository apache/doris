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

suite("test_regr_syy") {
    sql """ DROP TABLE IF EXISTS test_regr_syy """
    sql """ SET enable_nereids_planner=true """
    sql """ SET enable_fallback_to_original_planner=false """

    sql """
        CREATE TABLE test_regr_syy (
            id INT,
            x  DOUBLE,
            y  DOUBLE
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 4
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    // Empty table: verify NULL
    qt_empty "SELECT regr_syy(y, x) FROM test_regr_syy"

    // Base dataset
    sql """
        INSERT INTO test_regr_syy VALUES
            -- id=1: one row, syy should be 0
            (1, 10, 20),

            -- id=2: multiple rows
            (2, 1, 2),
            (2, 2, 4),
            (2, 3, 6),

            -- id=3: contains NULL, will be filtered out
            (3, 1, NULL),
            (3, NULL, 2),
            (3, 2, 5),
            (3, 3, 7),

            -- id=4: all rows contain NULL, syy should be NULL
            (4, NULL, 1),
            (4, 2, NULL),

            -- id=5: constant y
            (5, 1, 5),
            (5, 2, 5),
            (5, 3, 5)
    """

    // SYY(y) = sum(y*y) - sum(y)*sum(y)/n
    qt_syy_ref """
        SELECT
            id,
            regr_syy(y, x) AS syy,
            sum(y*y) - sum(y)*sum(y)/count(*) AS ref
        FROM test_regr_syy
        WHERE x IS NOT NULL AND y IS NOT NULL
        GROUP BY id
        ORDER BY id
    """

    // Single row
    qt_single_row "SELECT regr_syy(y, x) FROM test_regr_syy WHERE id = 1"

    // All rows invalid (no valid x/y pairs): verify NULL
    qt_all_filtered "SELECT regr_syy(y, x) FROM test_regr_syy WHERE id = 4"

    // Mix non_nullable
    qt_non_nullable_y "SELECT regr_syy(non_nullable(y), x) FROM test_regr_syy WHERE id = 2"
    qt_non_nullable_x "SELECT regr_syy(y, non_nullable(x)) FROM test_regr_syy WHERE id = 2"
    qt_non_nullable_xy "SELECT regr_syy(non_nullable(y), non_nullable(x)) FROM test_regr_syy WHERE id = 2"

    // Literal
    qt_literal_1 "SELECT regr_syy(1, 2)"
    qt_literal_2 "SELECT regr_syy(10, x) FROM test_regr_syy WHERE id = 2"
    qt_literal_3 "SELECT regr_syy(y, 3) FROM test_regr_syy WHERE id = 2"

    // exception
    test {
        sql "select regr_syy('y', 1)"
        exception "regr_syy requires numeric for first parameter"
    }
    test {
        sql "select regr_syy(1, 'x')"
        exception "regr_syy requires numeric for second parameter"
    }
    test {
        sql "select regr_syy(1, CAST([1, 2, 3] AS ARRAY<INT>))"
        exception "Doris hll, bitmap, array, map, struct, jsonb, variant column"
    }
}
