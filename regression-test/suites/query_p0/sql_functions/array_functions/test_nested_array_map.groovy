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

    qt_select_same_name_shadow """
        select array_map(x -> array_map(x -> x + 1, x), [[1, 2], [3, 4]]);
    """
}
