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

suite("lambda_null_pruning") {
    sql """ DROP TABLE IF EXISTS lambda_null_pruning_tbl """
    sql """
        CREATE TABLE lambda_null_pruning_tbl (
            id  INT,
            a   ARRAY<INT> NULL,
            b   ARRAY<INT> NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO lambda_null_pruning_tbl VALUES
            (1, [1, 2, 3],    [10, 20, 30]),
            (2, NULL,          NULL),
            (3, [],            []),
            (4, [null],        [1])
    """

    // ================================================================
    // Case 1: single-variable constant lambda body + IS NULL
    // body = Literal(true), array item variable unreferenced
    // collectArrayPathInLambda won't register full-access path.
    // If IS NULL already registered [a.NULL], pruning goes wrong.
    // ================================================================
    explain {
        sql """
            SELECT id, a IS NULL, array_count(x -> true, a)
            FROM lambda_null_pruning_tbl ORDER BY id
        """
        contains "nested columns"
        notContains "a.NULL"
    }

    order_qt_case1 """
        SELECT id, a IS NULL, array_count(x -> true, a)
        FROM lambda_null_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 2: two-variable lambda, body references x but not y
    // array_map((x, y) -> x, a, b):
    //   x -> body references -> visitArrayItemSlot fires -> [a, *] OK
    //   y -> body does NOT reference -> visitArrayItemSlot missing
    //   b IS NULL -> [b, NULL] registered -> bug triggered
    // After fix: fallback adds [b, *] for unreferenced y
    // ================================================================
    explain {
        sql """
            SELECT id, a IS NULL, b IS NULL,
                   array_map((x, y) -> x, a, b)
            FROM lambda_null_pruning_tbl ORDER BY id
        """
        contains "nested columns"
        notContains "a.NULL"
        notContains "b.NULL"
    }

    order_qt_case2 """
        SELECT id, a IS NULL, b IS NULL,
               array_map((x, y) -> x, a, b)
        FROM lambda_null_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 3: two-variable lambda, body references both
    // array_map((x, y) -> x + y, a, b):
    //   both x and y referenced -> visitArrayItemSlot fires for both
    // ================================================================
    explain {
        sql """
            SELECT id, a IS NULL, b IS NULL,
                   array_map((x, y) -> x + y, a, b)
            FROM lambda_null_pruning_tbl ORDER BY id
        """
        contains "nested columns"
        notContains "a.NULL"
        notContains "b.NULL"
    }

    order_qt_case3 """
        SELECT id, a IS NULL, b IS NULL,
               array_map((x, y) -> x + y, a, b)
        FROM lambda_null_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 4: array_filter constant lambda + IS NULL
    // ================================================================
    explain {
        sql """
            SELECT id, a IS NULL, array_filter(x -> true, a)
            FROM lambda_null_pruning_tbl ORDER BY id
        """
        contains "nested columns"
        notContains "a.NULL"
    }

    order_qt_case4 """
        SELECT id, a IS NULL, array_filter(x -> true, a)
        FROM lambda_null_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 5: IS NOT NULL also registers [a.NULL]
    // ================================================================
    explain {
        sql """
            SELECT id, a IS NOT NULL, array_count(x -> true, a)
            FROM lambda_null_pruning_tbl ORDER BY id
        """
        contains "nested columns"
        notContains "a.NULL"
    }

    // ================================================================
    // Case 6: cardinality (OFFSET path) + array_count constant lambda
    // ================================================================
    explain {
        sql """
            SELECT id, cardinality(a), array_count(x -> true, a)
            FROM lambda_null_pruning_tbl ORDER BY id
        """
        contains "nested columns"
        notContains "a.OFFSET"
    }

    order_qt_case6 """
        SELECT id, cardinality(a), array_count(x -> TRUE, a)
        FROM lambda_null_pruning_tbl ORDER BY id
    """
}
