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

suite("test_variant_sub_path_order", "p0") {
    sql "DROP TABLE IF EXISTS variant_sub_path_order"
    sql """
        CREATE TABLE variant_sub_path_order (
            id INT NOT NULL,
            v VARIANT<PROPERTIES ("variant_max_subcolumns_count" = "0")> NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_sub_path_order VALUES
            (1, parse_to_variant('{"a":{"b":1},"b":{"a":2}}'))
    """

    sql "SET experimental_enable_prune_nested_column = false"

    order_qt_union_constant_sub_path """
        WITH u AS (
            SELECT id, 'table' AS branch_name, v AS c
            FROM variant_sub_path_order
            UNION ALL
            SELECT 2 AS id, 'constant' AS branch_name,
                    parse_to_variant('{"a":{"b":1},"b":{"a":2}}') AS c
        )
        SELECT id, branch_name, CAST(c['a']['b'] AS INT) AS value
        FROM u
        ORDER BY id
    """

    order_qt_project_sub_path """
        SELECT id, CAST(c['a']['b'] AS INT) AS value
        FROM (
            SELECT id, IF(id > 0, v, parse_to_variant('{}')) AS c
            FROM variant_sub_path_order
        ) projected
        ORDER BY id
    """

    sql "SET experimental_enable_prune_nested_column = true"

    order_qt_union_constant_sub_path_with_nested_pruning """
        WITH u AS (
            SELECT id, 'table' AS branch_name, v AS c
            FROM variant_sub_path_order
            UNION ALL
            SELECT 2 AS id, 'constant' AS branch_name,
                    parse_to_variant('{"a":{"b":1},"b":{"a":2}}') AS c
        )
        SELECT id, branch_name, CAST(c['a']['b'] AS INT) AS value
        FROM u
        ORDER BY id
    """
}
