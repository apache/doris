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

suite("array_map_container_metadata_pruning") {
    sql """ DROP TABLE IF EXISTS array_map_container_metadata_pruning_tbl """
    sql """
        CREATE TABLE array_map_container_metadata_pruning_tbl (
            id INT,
            a  ARRAY<STRUCT<v: INT>>,
            s  STRUCT<arr: ARRAY<STRUCT<v: INT>>>,
            m  MAP<STRING, ARRAY<STRUCT<v: INT>>>
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO array_map_container_metadata_pruning_tbl VALUES
            (1,
             array(named_struct('v', 1), named_struct('v', 2)),
             named_struct('arr', array(named_struct('v', 1), named_struct('v', 2))),
             map('k', array(named_struct('v', 1), named_struct('v', 2)))),
            (2,
             CAST([] AS ARRAY<STRUCT<v: INT>>),
             named_struct('arr', CAST([] AS ARRAY<STRUCT<v: INT>>)),
             map('k', CAST([] AS ARRAY<STRUCT<v: INT>>))),
            (3,
             CAST(NULL AS ARRAY<STRUCT<v: INT>>),
             CAST(NULL AS STRUCT<arr: ARRAY<STRUCT<v: INT>>>),
             CAST(NULL AS MAP<STRING, ARRAY<STRUCT<v: INT>>>)),
            (4,
             array(CAST(NULL AS STRUCT<v: INT>), named_struct('v', 3)),
             named_struct('arr', array(CAST(NULL AS STRUCT<v: INT>), named_struct('v', 3))),
             map('k', array(CAST(NULL AS STRUCT<v: INT>), named_struct('v', 3))))
    """

    // ARRAY_MAP keeps the container shape of its inputs: the result offsets come from the first
    // input array and the result null map is the union of the inputs' null maps. A metadata
    // requirement (OFFSET from cardinality(), NULL from IS NULL) on the ARRAY_MAP result therefore
    // has to be collected on the input array container. Collecting it through the lambda element
    // instead yields paths such as a.*.OFFSET, which the BE rejects when the element is a struct.
    order_qt_root_array_map_cardinality """
        SELECT id, cardinality(a) AS direct, cardinality(array_map(x -> x, a)) AS mapped
        FROM array_map_container_metadata_pruning_tbl ORDER BY id
    """

    order_qt_struct_field_array_map_cardinality """
        SELECT id,
               cardinality(element_at(s, 'arr')) AS direct,
               cardinality(array_map(x -> x, element_at(s, 'arr'))) AS mapped
        FROM array_map_container_metadata_pruning_tbl ORDER BY id
    """

    order_qt_map_value_array_map_cardinality """
        SELECT id,
               cardinality(element_at(m, 'k')) AS direct,
               cardinality(array_map(x -> x, element_at(m, 'k'))) AS mapped
        FROM array_map_container_metadata_pruning_tbl ORDER BY id
    """

    order_qt_multi_array_array_map_cardinality """
        SELECT id, cardinality(array_map((x, y) -> x.v + y.v, a, a)) AS mapped
        FROM array_map_container_metadata_pruning_tbl ORDER BY id
    """

    order_qt_array_map_is_null """
        SELECT id, a IS NULL AS direct, array_map(x -> x, a) IS NULL AS mapped
        FROM array_map_container_metadata_pruning_tbl ORDER BY id
    """

    // The lambda body still reads element data: only the element field it references is needed.
    order_qt_lambda_reads_element_field """
        SELECT id, cardinality(array_map(x -> x.v + 1, a)) AS mapped
        FROM array_map_container_metadata_pruning_tbl ORDER BY id
    """

    explain {
        sql """
            SELECT cardinality(array_map(x -> x, a))
            FROM array_map_container_metadata_pruning_tbl
        """
        contains "a.OFFSET"
        contains "a.*"
        notContains "a.*.OFFSET"
    }

    explain {
        sql """
            SELECT cardinality(array_map(x -> x, element_at(s, 'arr')))
            FROM array_map_container_metadata_pruning_tbl
        """
        contains "s.arr.OFFSET"
        notContains "s.arr.*.OFFSET"
    }

    explain {
        sql """
            SELECT cardinality(array_map(x -> x, element_at(m, 'k')))
            FROM array_map_container_metadata_pruning_tbl
        """
        contains "m.VALUES.OFFSET"
        notContains "m.VALUES.*.OFFSET"
    }

    explain {
        sql """
            SELECT array_map(x -> x, a) IS NULL
            FROM array_map_container_metadata_pruning_tbl
        """
        contains "a.NULL"
        notContains "a.*.NULL"
    }

    explain {
        sql """
            SELECT cardinality(array_map(x -> x.v + 1, a))
            FROM array_map_container_metadata_pruning_tbl
        """
        contains "a.*.v"
        contains "a.OFFSET"
        notContains "a.*.OFFSET"
    }
}
