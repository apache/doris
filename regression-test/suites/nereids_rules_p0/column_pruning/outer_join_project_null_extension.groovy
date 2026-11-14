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

suite("outer_join_project_null_extension") {
    sql "DROP TABLE IF EXISTS outer_join_project_null_left"
    sql "DROP TABLE IF EXISTS outer_join_project_null_right"
    sql """
        CREATE TABLE outer_join_project_null_left (
            id INT, arr ARRAY<STRING>, m MAP<STRING, STRING>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE outer_join_project_null_right (
            id INT, arr ARRAY<STRING>, m MAP<STRING, STRING>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO outer_join_project_null_left VALUES
            (1, ['a', ''], map('a', 'A')), (2, ['b'], map('b', 'B')), (3, NULL, NULL)
    """
    sql """
        INSERT INTO outer_join_project_null_right VALUES
            (1, ['p', ''], map('p', 'P')), (3, NULL, NULL), (4, ['r'], map('r', 'R'))
    """

    // Cover both unmatched rows and matched rows containing NULL.
    order_qt_array_functions """
        SELECT a.id,
            IF(b.arr IS NULL, ['n'], b.arr),
            array_map(x -> upper(x), IF(b.arr IS NULL, ['n'], b.arr)),
            array_filter(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_count(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_exists(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_match_any(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_match_all(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_first(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_last(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_first_index(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_last_index(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_split(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_reverse_split(x -> x != '', IF(b.arr IS NULL, ['n'], b.arr)),
            array_sortby(x -> upper(x), IF(b.arr IS NULL, ['n'], b.arr)),
            element_at(IF(b.arr IS NULL, ['n'], b.arr), 1)
        FROM outer_join_project_null_left a LEFT JOIN outer_join_project_null_right b ON a.id = b.id
    """

    order_qt_map_functions """
        SELECT a.id,
            map_keys(IF(b.m IS NULL, map('n', 'N'), b.m)),
            map_values(IF(b.m IS NULL, map('n', 'N'), b.m)),
            map_entries(IF(b.m IS NULL, map('n', 'N'), b.m)),
            map_contains_key(IF(b.m IS NULL, map('n', 'N'), b.m), 'n'),
            map_contains_value(IF(b.m IS NULL, map('n', 'N'), b.m), 'N'),
            map_contains_entry(IF(b.m IS NULL, map('n', 'N'), b.m), 'n', 'N'),
            map_from_arrays(IF(b.arr IS NULL, ['n'], b.arr), IF(b.arr IS NULL, ['N'], b.arr)),
            map_from_entries(IF(b.m IS NULL, array(named_struct('key', 'n', 'value', 'N')), map_entries(b.m))),
            map_apply((k, v) -> struct(k, upper(v)), IF(b.m IS NULL, map('n', 'N'), b.m)),
            transform_keys((k, v) -> upper(k), IF(b.m IS NULL, map('n', 'N'), b.m)),
            transform_values((k, v) -> upper(v), IF(b.m IS NULL, map('n', 'N'), b.m)),
            map_filter((k, v) -> k != '', IF(b.m IS NULL, map('n', 'N'), b.m)),
            map_exists((k, v) -> k != '', IF(b.m IS NULL, map('n', 'N'), b.m)),
            map_all((k, v) -> k != '', IF(b.m IS NULL, map('n', 'N'), b.m))
        FROM outer_join_project_null_left a LEFT JOIN outer_join_project_null_right b ON a.id = b.id
    """

    order_qt_right_join """
        SELECT a.id,
            array_map(x -> upper(x), coalesce(b.arr, ['n'])),
            map_keys(coalesce(b.m, map('n', 'N')))
        FROM outer_join_project_null_right b RIGHT JOIN outer_join_project_null_left a ON a.id = b.id
    """

    order_qt_full_join """
        SELECT coalesce(a.id, b.id),
            array_map(x -> upper(x), coalesce(a.arr, ['left'])),
            array_map(x -> upper(x), coalesce(b.arr, ['right'])),
            map_keys(coalesce(a.m, map('left', 'L'))),
            map_keys(coalesce(b.m, map('right', 'R')))
        FROM outer_join_project_null_left a FULL OUTER JOIN outer_join_project_null_right b ON a.id = b.id
    """

    // The filter must evaluate after null extension and retain the unmatched row.
    order_qt_filter """
        SELECT a.id
        FROM outer_join_project_null_left a LEFT JOIN outer_join_project_null_right b ON a.id = b.id
        WHERE element_at(IF(b.arr IS NULL, ['n'], b.arr), 1) = 'n'
    """

    // ON expressions are evaluated before null extension and can still be pushed down.
    order_qt_on_condition """
        SELECT a.id, b.id
        FROM outer_join_project_null_left a LEFT JOIN outer_join_project_null_right b
            ON a.id = b.id AND element_at(IF(b.arr IS NULL, ['n'], b.arr), 1) = 'n'
    """

    order_qt_null_propagating """
        SELECT a.id, array_map(x -> upper(x), b.arr), map_keys(b.m), element_at(b.arr, 1)
        FROM outer_join_project_null_left a LEFT JOIN outer_join_project_null_right b ON a.id = b.id
    """

    order_qt_multiple_arrays """
        SELECT a.id,
            array_map((x, y) -> x, a.arr, b.arr),
            array_map((x, y) -> x, coalesce(b.arr, ['n']), b.arr),
            array_map((x, y) -> x, coalesce(b.arr, ['n']), coalesce(b.arr, ['N']))
        FROM outer_join_project_null_left a LEFT JOIN outer_join_project_null_right b ON a.id = b.id
    """

    order_qt_comparator_functions """
        SELECT a.id,
            array_sort((x, y) -> IF(x < y, -1, IF(x = y, 0, 1)), b.arr),
            array_sort((x, y) -> IF(x < y, -1, IF(x = y, 0, 1)), coalesce(b.arr, ['n'])),
            array_sorted_merge((x, y) -> IF(x = y, x, NULL),
                array_sort(b.arr), array_sort(b.arr), true),
            array_sorted_merge((x, y) -> IF(x = y, x, NULL),
                array_sort(coalesce(b.arr, ['n'])), array_sort(coalesce(b.arr, ['n'])), true)
        FROM outer_join_project_null_left a LEFT JOIN outer_join_project_null_right b ON a.id = b.id
    """

    order_qt_constant_null_inputs """
        SELECT array_sort((x, y) -> 0, CAST(NULL AS ARRAY<STRING>)),
            array_sorted_merge((x, y) -> x, CAST(NULL AS ARRAY<STRING>), CAST(['n'] AS ARRAY<STRING>), true),
            array_sorted_merge((x, y) -> x, CAST(['n'] AS ARRAY<STRING>), CAST(NULL AS ARRAY<STRING>), false)
    """

    test {
        sql """SELECT array_sorted_merge((x, y) -> x,
                   CAST(NULL AS ARRAY<STRING>), CAST(['n'] AS ARRAY<STRING>), CAST(NULL AS BOOLEAN))"""
        exception "requires a non-null constant ascending flag"
    }

    order_qt_inner_join """
        SELECT a.id,
            array_map(x -> upper(x), coalesce(b.arr, ['n'])),
            map_keys(coalesce(b.m, map('n', 'N')))
        FROM outer_join_project_null_left a INNER JOIN outer_join_project_null_right b ON a.id = b.id
    """
}
