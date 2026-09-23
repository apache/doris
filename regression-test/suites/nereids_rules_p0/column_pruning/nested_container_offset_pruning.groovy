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

suite("nested_container_offset_pruning") {
    sql """ DROP TABLE IF EXISTS nested_container_offset_pruning_tbl """
    sql """
        CREATE TABLE nested_container_offset_pruning_tbl (
            id INT,
            s STRUCT<
                arr: ARRAY<STRUCT<str_field: STRING, int_field: INT>>,
                m: MAP<STRING, STRING>
            >
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO nested_container_offset_pruning_tbl VALUES (
            1,
            named_struct(
                'arr', array(
                    named_struct('str_field', 'hello', 'int_field', 10),
                    named_struct('str_field', 'world', 'int_field', 20)
                ),
                'm', {'a': 'x', 'b': 'y'}
            )
        )
    """

    // cardinality(s.arr) only needs array offsets, but element_at(...).int_field also needs
    // array item data. The redundant s.arr.OFFSET path must be removed even though the root slot
    // itself is STRUCT.
    order_qt_struct_root_arr_mixed """
        SELECT id,
               cardinality(struct_element(s, 'arr')),
               struct_element(element_at(struct_element(s, 'arr'), 1), 'int_field')
        FROM nested_container_offset_pruning_tbl ORDER BY id
    """

    // Same issue for nested maps: length(element_at(s.m, 'a')) needs the key lookup path,
    // while map_values(s.m)[1] needs full value data. Dedup must therefore keep KEYS + VALUES
    // and drop only the redundant value-side OFFSET path under the nested map container.
    order_qt_struct_root_map_mixed """
        SELECT id,
               length(element_at(struct_element(s, 'm'), 'a')),
               element_at(map_values(struct_element(s, 'm')), 1)
        FROM nested_container_offset_pruning_tbl ORDER BY id
    """

    explain {
        sql """
            SELECT cardinality(struct_element(s, 'arr')),
                   struct_element(element_at(struct_element(s, 'arr'), 1), 'int_field')
            FROM nested_container_offset_pruning_tbl
        """
        contains "s.arr.*.int_field"
        notContains "s.arr.OFFSET"
    }

    explain {
        sql """
            SELECT length(element_at(struct_element(s, 'm'), 'a')),
                   element_at(map_values(struct_element(s, 'm')), 1)
            FROM nested_container_offset_pruning_tbl
        """
        contains "s.m.KEYS"
        contains "s.m.VALUES"
        notContains "OFFSET"
    }
}
