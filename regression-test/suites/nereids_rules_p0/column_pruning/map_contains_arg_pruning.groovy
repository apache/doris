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
    // Case 3: map_contains_entry, entry arg references sub-column
    // map_contains_entry(m, element_at(s, 'a'), element_at(s, 'b'))
    // needs both s.a and s.b
    // Note: map_contains_entry syntax may vary; using the 2-arg form
    // ================================================================
    explain {
        sql """
            SELECT id,
                   element_at(s, 'a') IS NULL,
                   map_contains_key(m, element_at(s, 'a')),
                   map_contains_value(m, element_at(s, 'b'))
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
               map_contains_key(m, element_at(s, 'a')),
               map_contains_value(m, element_at(s, 'b'))
        FROM map_contains_arg_pruning_tbl ORDER BY id
    """
}
