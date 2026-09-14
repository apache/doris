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

suite("test_nested_array_map") {
    sql "DROP TABLE IF EXISTS test_nested_array_map_insert_src"
    sql "DROP TABLE IF EXISTS test_nested_array_map_insert_dst"

    sql """
        CREATE TABLE test_nested_array_map_insert_src (
            id INT,
            bucket_counts ARRAY<BIGINT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        CREATE TABLE test_nested_array_map_insert_dst (
            id INT,
            bucket_counts ARRAY<BIGINT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO test_nested_array_map_insert_src VALUES
            (1, [1, 2, 3]),
            (1, [4, 5, 6]),
            (2, [10, 20]),
            (2, [1, 2, 3])
    """

    sql """
        INSERT INTO test_nested_array_map_insert_dst (id, bucket_counts)
        WITH rollup_grouped AS (
            SELECT
                id,
                ARRAY_AGG(bucket_counts) AS bucket_count_arrays,
                MAX(ARRAY_SIZE(bucket_counts)) AS max_bucket_len
            FROM test_nested_array_map_insert_src
            GROUP BY id
        )
        SELECT
            id,
            ARRAY_MAP(
                i -> ARRAY_SUM(ARRAY_MAP(a -> COALESCE(a[CAST(i AS INT)], 0), bucket_count_arrays)),
                ARRAY_RANGE(1, max_bucket_len + 1)
            ) AS bucket_counts
        FROM rollup_grouped
    """

    order_qt_select """
        SELECT id, bucket_counts
        FROM test_nested_array_map_insert_dst
        ORDER BY id
    """

    qt_select2 """
        select array_map(x -> array_map(y -> y - x, [1, 2]), [10, 20]);
    """

    qt_select_nested_array_sortby """
        select array_map(x -> array_sortby(y -> abs(y - x), [5, 1, 3, 9]), [2, 6, 8]);
    """

    qt_select_same_name_shadow """
        select array_map(x -> array_map(x -> x + 1, x), [[1, 2], [3, 4]]);
    """

    // the nested lambda body captures the columns of the query and the arguments of the enclosing lambda
    sql "DROP TABLE IF EXISTS test_nested_array_map_capture"
    sql """
        CREATE TABLE test_nested_array_map_capture (
            id INT,
            flag BOOLEAN,
            arr1 ARRAY<VARCHAR(10)>,
            arr2 ARRAY<INT>
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """
    sql """
        INSERT INTO test_nested_array_map_capture VALUES
            (1, true, ['A', 'B'], [1, 2, 3]),
            (2, false, ['A'], [4, 5]),
            (3, NULL, ['A', 'B', 'C'], [6]),
            (4, true, [], [7, 8])
    """

    order_qt_nested_capture_column """
        select id, array_map(a -> array_sum(array_map(b -> if(flag, b, 0), arr2)), arr1)
        from test_nested_array_map_capture
    """

    order_qt_nested_capture_column_sortby """
        select id, array_map(a -> array_sortby(b -> if(flag, -b, b), arr2), arr1)
        from test_nested_array_map_capture
    """

    order_qt_nested_capture_column_and_outer_argument """
        select id, array_map(a -> array_map(b -> concat(a, b, if(flag, 'y', 'n')), arr2), arr1)
        from test_nested_array_map_capture
    """

    order_qt_nested_capture_three_levels """
        select id, array_map(a -> array_map(b -> array_map(c -> if(flag, b + c, id), arr2), arr2), arr1)
        from test_nested_array_map_capture
    """

    order_qt_nested_capture_cte_alias """
        with t2 as (
            select id, flag is null as dict_flag, arr1, arr2 from test_nested_array_map_capture
        )
        select id, array_size(array_map(a -> array_sum(array_map(b -> if(dict_flag, 1, 0), arr2)), arr1)) as l
        from t2
    """

    test {
        sql "select array_map(a -> array_map(b -> unknown_col + b, arr2), arr1) from test_nested_array_map_capture"
        exception "Unknown lambda slot 'unknown_col"
    }
}
