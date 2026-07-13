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

suite("test_constant_column_pruned_complex") {
    sql "SET batch_size = 4"
    sql "DROP TABLE IF EXISTS test_constant_column_pruned_complex"
    sql """
        CREATE TABLE test_constant_column_pruned_complex (
            id INT,
            payload VARCHAR(20)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "light_schema_change" = "true",
            "disable_auto_compaction" = "true"
        )
    """

    // These rows are stored before either complex column exists. Both columns must therefore be
    // supplied by ConstantColumnIterator after the light schema change.
    sql """
        INSERT INTO test_constant_column_pruned_complex VALUES
            (1, 'old-one'),
            (2, 'old-two')
    """

    sql """
        ALTER TABLE test_constant_column_pruned_complex
        ADD COLUMN m MAP<STRING, INT> NULL
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_column_pruned_complex'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }

    sql """
        ALTER TABLE test_constant_column_pruned_complex
        ADD COLUMN s STRUCT<a:INT, b:STRING> NULL
    """
    waitForSchemaChangeDone {
        sql """
            SHOW ALTER TABLE COLUMN
            WHERE TableName = 'test_constant_column_pruned_complex'
            ORDER BY CreateTime DESC LIMIT 1
        """
        time 600
    }

    // Mix physical complex values, empty containers, and explicit NULL with the historical rows.
    sql """
        INSERT INTO test_constant_column_pruned_complex (id, payload, m, s) VALUES
            (3, 'new-three', map('x', 30, 'y', 31), named_struct('a', 300, 'b', 'three')),
            (4, 'new-four', map(), named_struct('a', 400, 'b', 'four')),
            (5, 'new-null', NULL, NULL)
    """

    sql "SET enable_prune_nested_column = true"
    order_qt_pruned_complex_enabled """
        SELECT id,
               m IS NULL,
               array_sort(map_keys(m)),
               array_sort(map_values(m)),
               cardinality(map_keys(m)),
               element_at(m, 'x'),
               s IS NULL,
               struct_element(s, 'a'),
               struct_element(s, 'b')
        FROM test_constant_column_pruned_complex
        ORDER BY id
    """

    // The same result without nested-column pruning guards against a pruning-only interpretation
    // of the constant-backed MAP and STRUCT children.
    sql "SET enable_prune_nested_column = false"
    order_qt_pruned_complex_disabled """
        SELECT id,
               m IS NULL,
               array_sort(map_keys(m)),
               array_sort(map_values(m)),
               cardinality(map_keys(m)),
               element_at(m, 'x'),
               s IS NULL,
               struct_element(s, 'a'),
               struct_element(s, 'b')
        FROM test_constant_column_pruned_complex
        ORDER BY id
    """

    sql "SET enable_prune_nested_column = true"

    // Read only one MAP child at a time so nested-column pruning cannot materialize the other
    // child as a shortcut. Historical rows still have to produce a valid nullable constant.
    order_qt_pruned_map_keys_only """
        SELECT id, array_sort(map_keys(m))
        FROM test_constant_column_pruned_complex
        ORDER BY id
    """
    order_qt_pruned_map_values_only """
        SELECT id, array_sort(map_values(m))
        FROM test_constant_column_pruned_complex
        ORDER BY id
    """

    // Likewise, make each STRUCT child its own access path. This mixes NULL constants from the
    // pre-ALTER segment with physical child streams from the post-ALTER segment.
    order_qt_pruned_struct_a_only """
        SELECT id, struct_element(s, 'a')
        FROM test_constant_column_pruned_complex
        ORDER BY id
    """
    order_qt_pruned_struct_b_only """
        SELECT id, struct_element(s, 'b')
        FROM test_constant_column_pruned_complex
        ORDER BY id
    """

    // Constant-backed complex values are outputs while an ordinary physical column is the
    // predicate. This also exercises map/struct child pruning on the old rowset.
    order_qt_constant_complex_as_output """
        SELECT id,
               element_at(m, 'x'),
               map_keys(m),
               map_values(m),
               cardinality(map_keys(m)),
               struct_element(s, 'a'),
               struct_element(s, 'b')
        FROM test_constant_column_pruned_complex
        WHERE payload LIKE 'old-%'
        ORDER BY id
    """

    // Exercise the inverse assignment: the constant-backed columns participate in predicates
    // while values from both old and new physical schemas are materialized as output.
    order_qt_constant_complex_as_predicate """
        SELECT id, payload
        FROM test_constant_column_pruned_complex
        WHERE m IS NULL AND s IS NULL
        ORDER BY id
    """

    // All candidate historical rows reach the complex predicates, but SQL NULL semantics leave
    // an empty selection. Finalizing the constant placeholders must still produce a valid block.
    qt_constant_complex_empty_selection """
        SELECT COUNT(*)
        FROM test_constant_column_pruned_complex
        WHERE m IS NULL
          AND struct_element(s, 'a') > 0
    """
}
