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

suite("test_arrays_overlap_const") {
    sql "DROP TABLE IF EXISTS test_arrays_overlap_const"
    sql """
        CREATE TABLE test_arrays_overlap_const (
            id INT,
            ints ARRAY<INT> NULL,
            strings ARRAY<STRING> NULL,
            INDEX idx_ints(ints) USING INVERTED,
            INDEX idx_strings(strings) USING INVERTED
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_arrays_overlap_const VALUES
            (1, [1, 2], ['a', 'b']),
            (2, [3, 4, 5, 6, 7], ['c', '', 'd', 'e']),
            (3, [NULL, 8], [NULL, 'f']),
            (4, [], []),
            (5, NULL, NULL),
            (6, [NULL], [NULL])
    """

    // Projection exercises the prepared expression even when the table has indexes.
    order_qt_projection """
        SELECT id, arrays_overlap(ints, [1, NULL, 1]),
               arrays_overlap([1, NULL, 1], ints),
               arrays_overlap(strings, ['a', '']),
               arrays_overlap(['a', ''], strings),
               arrays_overlap(ints, CAST([] AS ARRAY<INT>)),
               arrays_overlap(CAST(NULL AS ARRAY<INT>), ints),
               arrays_overlap([1, NULL], [NULL, 2]), arrays_overlap([], [1])
        FROM test_arrays_overlap_const
    """

    // Cover both the index path and expression evaluation, including the NULL-element
    // queries for which the inverted index falls back to evaluating the expression.
    for (def enabled : [false, true]) {
        sql "SET enable_inverted_index_query = ${enabled}"
        order_qt_int_predicate """
            SELECT id FROM test_arrays_overlap_const WHERE arrays_overlap(ints, [1, 7])
        """
        order_qt_string_predicate """
            SELECT id FROM test_arrays_overlap_const WHERE arrays_overlap(strings, ['a', ''])
        """
        order_qt_null_element_predicate """
            SELECT id FROM test_arrays_overlap_const WHERE arrays_overlap(ints, [NULL, 8])
        """
        order_qt_empty_predicate """
            SELECT id FROM test_arrays_overlap_const
            WHERE arrays_overlap(ints, CAST([] AS ARRAY<INT>))
        """
    }
}
