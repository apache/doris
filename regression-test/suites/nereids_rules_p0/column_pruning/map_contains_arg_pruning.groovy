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

// Regression tests for map_contains_key / map_contains_value / map_contains_entry
// when the key/value/entry argument references nested sub-columns.
//
// Bug: visitMapContainsKey/Value/Entry only visited the map argument and skipped
// the key/value/entry argument. When the key is a nested sub-column expression
// (e.g. element_at(s, 'a')) whose data path was not registered, and the same
// sub-column also appears in IS NULL, NestedColumnPruning would prune it to
// null-only metadata access, causing wrong results.

suite("map_contains_arg_pruning") {
    sql """ DROP TABLE IF EXISTS map_contains_arg_pruning_tbl """
    sql """
        CREATE TABLE map_contains_arg_pruning_tbl (
            id  INT,
            s   STRUCT<a: STRING, b: INT> NULL,
            m   MAP<STRING, INT> NULL,
            v   VARIANT NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO map_contains_arg_pruning_tbl VALUES
            (1, named_struct('a', 'hello', 'b', 100),  {'hello': 1, 'world': 2},  '{"k": 42}'),
            (2, named_struct('a', 'doris', 'b', 200),  {'doris': 3},              NULL),
            (3, named_struct('a', null, 'b', 300),      NULL,                      '{"x": 1}'),
            (4, NULL,                                   {},                        '{}')
    """

    // ================================================================
    // Case 1: map_contains_key + element_at IS NULL (original bug)
    // map_contains_key(m, element_at(s, 'a')) needs full access to s.a
    // as the key lookup value. Without fix, only [s.a.NULL] from
    // element_at(s, 'a') IS NULL is registered.
    // ================================================================
    explain {
        sql """
            SELECT id,
                   element_at(s, 'a') IS NULL,
                   map_contains_key(m, element_at(s, 'a'))
            FROM map_contains_arg_pruning_tbl ORDER BY id
        """
        contains "nested columns"
        contains "s.a"                       // s.a should appear in access paths
        notContains "s.a.NULL"               // should NOT be null-only
        contains "m.KEYS"                    // map_contains_key needs KEYS path
    }

    order_qt_case1 """
        SELECT id,
               element_at(s, 'a') IS NULL,
               map_contains_key(m, element_at(s, 'a'))
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 2: map_contains_value, value arg references sub-column
    // map_contains_value(m, element_at(s, 'b')) needs s.b as value
    // ================================================================
    explain {
        sql """
            SELECT id,
                   element_at(s, 'b') IS NULL,
                   map_contains_value(m, element_at(s, 'b'))
            FROM map_contains_arg_pruning_tbl ORDER BY id
        """
        contains "nested columns"
        contains "s.b"                       // s.b should appear in access paths
        notContains "s.b.NULL"               // should NOT be null-only
    }

    order_qt_case2 """
        SELECT id,
               element_at(s, 'b') IS NULL,
               map_contains_value(m, element_at(s, 'b'))
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 3: map_contains_entry(m, key, value) — ternary function.
    // visitMapContainsEntry must visit BOTH search arguments (arg1=key,
    // arg2=value) with fresh contexts. Without the fix, only arg1 was
    // collected and arg2 was silently skipped, leaving s.b unregistered
    // so that element_at(s, 'b') IS NULL would prune s.b to null-only.
    // ================================================================
    explain {
        sql """
            SELECT id,
                   element_at(s, 'a') IS NULL,
                   element_at(s, 'b') IS NULL,
                   map_contains_entry(m, element_at(s, 'a'), element_at(s, 'b'))
            FROM map_contains_arg_pruning_tbl ORDER BY id
        """
        contains "nested columns"
        contains "s.a"
        contains "s.b"
        notContains "s.a.NULL"
        notContains "s.b.NULL"
    }

    order_qt_case3 """
        SELECT id,
               element_at(s, 'a') IS NULL,
               element_at(s, 'b') IS NULL,
               map_contains_entry(m, element_at(s, 'a'), element_at(s, 'b'))
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 4: map_contains_key(...) IS NULL — the key argument must NOT
    // be collected as a full-data path because map_contains_key returns
    // NULL only when the map itself is NULL. The search key does not
    // affect nullability.
    //
    // When map_contains_key(...) IS NULL is the only expression, the
    // NULL-only context causes the key argument to be skipped entirely,
    // so s.a is never registered and no nested column pruning is emitted.
    // ================================================================
    explain {
        sql """
            SELECT id,
                   map_contains_key(m, element_at(s, 'a')) IS NULL
            FROM map_contains_arg_pruning_tbl ORDER BY id
        """
        // s.a must NOT appear in access paths: the key argument inside
        // map_contains_key(...) IS NULL is skipped in NULL-only context.
        notContains "[s.a]"
    }

    order_qt_case4 """
        SELECT id,
               map_contains_key(m, element_at(s, 'a')) IS NULL
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 5: map_contains_value(...) IS NULL — same behaviour as
    // map_contains_key: the value argument does not affect nullability.
    // ================================================================
    explain {
        sql """
            SELECT id,
                   map_contains_value(m, element_at(s, 'b')) IS NULL
            FROM map_contains_arg_pruning_tbl ORDER BY id
        """
        notContains "[s.b]"
    }

    order_qt_case5 """
        SELECT id,
               map_contains_value(m, element_at(s, 'b')) IS NULL
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 6: map_contains_entry(...) IS NULL — both key and value
    // arguments must be skipped. No nested column pruning is expected
    // because only the map argument is visited with NULL-only path.
    // ================================================================
    explain {
        sql """
            SELECT id,
                   map_contains_entry(m, element_at(s, 'a'), element_at(s, 'b')) IS NULL
            FROM map_contains_arg_pruning_tbl ORDER BY id
        """
        notContains "s.a"
        notContains "s.b"
    }

    order_qt_case6 """
        SELECT id,
               map_contains_entry(m, element_at(s, 'a'), element_at(s, 'b')) IS NULL
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """

    // ================================================================
    // Case 7: IS NOT NULL variants — NOT(IS NULL) delegates to the
    // same visitIsNull path, so the NULL-only routing logic applies.
    // ================================================================
    order_qt_case7a """
        SELECT id,
               map_contains_key(m, element_at(s, 'a')) IS NOT NULL
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """

    order_qt_case7b """
        SELECT id,
               map_contains_value(m, element_at(s, 'b')) IS NOT NULL
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """

    order_qt_case7c """
        SELECT id,
               map_contains_entry(m, element_at(s, 'a'), element_at(s, 'b')) IS NOT NULL
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """
}
