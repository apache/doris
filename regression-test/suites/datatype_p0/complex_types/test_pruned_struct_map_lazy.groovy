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

suite("test_pruned_struct_map_lazy") {
    sql "set enable_prune_nested_column = true"
    sql "DROP TABLE IF EXISTS struct_pruned_map_lazy_tbl"
    sql """
        CREATE TABLE struct_pruned_map_lazy_tbl (
            id INT,
            heavy_struct STRUCT<
                big_string:VARCHAR(20),
                small_int:INT,
                nested_map:MAP<INT,VARCHAR(20)>
            >
        ) ENGINE=OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1"
        )
    """

    sql """
        INSERT INTO struct_pruned_map_lazy_tbl VALUES
            (1, named_struct('big_string', 'Alpha', 'small_int', 100,
                    'nested_map', map(1, 'val1', 2, 'val2'))),
            (2, named_struct('big_string', 'Beta', 'small_int', 200,
                    'nested_map', map(3, 'val3'))),
            (3, named_struct('big_string', 'Gamma', 'small_int', 300,
                    'nested_map', map(4, 'val4')))
    """

    order_qt_map_values_lazy_pruned """
        SELECT id, array_sort(map_values(struct_element(heavy_struct, 'nested_map')))
        FROM struct_pruned_map_lazy_tbl
        WHERE struct_element(heavy_struct, 'big_string') LIKE 'B%'
        ORDER BY id
    """

    order_qt_map_keys_lazy_pruned """
        SELECT id, array_sort(map_keys(struct_element(heavy_struct, 'nested_map')))
        FROM struct_pruned_map_lazy_tbl
        WHERE struct_element(heavy_struct, 'small_int') = 200
        ORDER BY id
    """
}
