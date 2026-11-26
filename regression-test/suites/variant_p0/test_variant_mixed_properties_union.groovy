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

suite("test_variant_mixed_properties_union", "p0") {
    sql "DROP TABLE IF EXISTS variant_mixed_properties_union"
    sql """
        CREATE TABLE variant_mixed_properties_union (
            id INT NOT NULL,
            typed_paths VARIANT<'nested.b': INT, 'kind': STRING, PROPERTIES (
                "variant_max_subcolumns_count" = "0",
                "variant_enable_typed_paths_to_sparse" = "false")>,
            encoded_paths VARIANT<PROPERTIES (
                "variant_max_subcolumns_count" = "1",
                "variant_enable_typed_paths_to_sparse" = "true")>
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO variant_mixed_properties_union VALUES
            (1,
             parse_to_variant('{"nested":{"b":1},"a":{"b":101},"kind":"typed_object"}'),
             parse_to_variant('{"nested":{"b":"2"},"a":{"b":"102"},"kind":"encoded_object","overflow":{"x":1}}')),
            (2,
             parse_to_variant('[1,"two",null]'),
             parse_to_variant('"encoded_scalar"'))
    """

    sql "SET experimental_enable_prune_nested_column = false"

    order_qt_mixed_storage_properties """
        WITH mixed AS (
            SELECT 1 AS branch_id, id, typed_paths AS v
            FROM variant_mixed_properties_union
            UNION ALL
            SELECT 2 AS branch_id, id, encoded_paths AS v
            FROM variant_mixed_properties_union
        )
        SELECT branch_id, id, CAST(v AS STRING) AS full_value, variant_type(v) AS value_type,
               CAST(v['nested']['b'] AS INT) AS nested_value,
               CAST(v['a']['b'] AS INT) AS dynamic_path_value
        FROM mixed
        ORDER BY branch_id, id
    """

    order_qt_encoded_and_typed_runtime_states """
        WITH mixed AS (
            SELECT 1 AS branch_id, typed_paths AS v
            FROM variant_mixed_properties_union
            WHERE id = 1
            UNION ALL
            SELECT 2 AS branch_id, parse_to_variant('{"encoded":3}') AS v
            UNION ALL
            SELECT 3 AS branch_id, CAST(CAST(7 AS BIGINT) AS VARIANT) AS v
        )
        SELECT branch_id, CAST(v AS STRING) AS full_value, variant_type(v) AS value_type
        FROM mixed
        ORDER BY branch_id
    """

    order_qt_conditional_mixed_property_variants """
        SELECT id,
               CAST(IF(id = 1, typed_paths, encoded_paths) AS STRING) AS full_value,
               variant_type(IF(id = 1, typed_paths, encoded_paths)) AS value_type
        FROM variant_mixed_properties_union
        ORDER BY id
    """

    sql "SET experimental_enable_prune_nested_column = true"

    order_qt_mixed_storage_properties_with_pruning """
        WITH mixed AS (
            SELECT 1 AS branch_id, id, typed_paths AS v
            FROM variant_mixed_properties_union
            UNION ALL
            SELECT 2 AS branch_id, id, encoded_paths AS v
            FROM variant_mixed_properties_union
        )
        SELECT branch_id, id,
               CAST(v['nested']['b'] AS INT) AS nested_value,
               CAST(v['a']['b'] AS INT) AS dynamic_path_value
        FROM mixed
        ORDER BY branch_id, id
    """
}
