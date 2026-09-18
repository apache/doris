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

suite("struct_map_pruning_lazy_read") {
    sql "set enable_prune_nested_column = true"
    sql "DROP TABLE IF EXISTS struct_map_pruning_lazy_read_tbl"
    sql """
        CREATE TABLE struct_map_pruning_lazy_read_tbl (
            id INT,
            heavy_struct STRUCT<
                small_int: INT,
                big_string: VARCHAR(2048),
                nested_map: MAP<INT, VARCHAR(100)>
            >,
            standalone_map MAP<VARCHAR(50), INT>
        )
        UNIQUE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO struct_map_pruning_lazy_read_tbl VALUES
        (1, struct(100, repeat('A', 2048), map(1, 'val1', 2, 'val2')), map('key_x', 1, 'key_y', 2)),
        (2, struct(200, repeat('B', 2048), map(3, 'val3')), map('key_z', 3)),
        (3, struct(300, repeat('C', 2048), map()), map('empty', 0))
    """

    order_qt_map_values """
        SELECT id, map_values(struct_element(heavy_struct, 'nested_map'))
        FROM struct_map_pruning_lazy_read_tbl
        WHERE struct_element(heavy_struct, 'big_string') LIKE 'B%'
        ORDER BY id
    """

    order_qt_map_keys """
        SELECT id, map_keys(struct_element(heavy_struct, 'nested_map'))
        FROM struct_map_pruning_lazy_read_tbl
        WHERE struct_element(heavy_struct, 'small_int') = 200
        ORDER BY id
    """
}
