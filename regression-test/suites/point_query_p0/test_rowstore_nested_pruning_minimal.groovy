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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

suite("test_rowstore_nested_pruning_minimal", "p0,nonConcurrent") {
    sql "DROP TABLE IF EXISTS rowstore_nested_pruning_minimal"
    sql """
        CREATE TABLE rowstore_nested_pruning_minimal (
            pk INT,
            typed STRUCT<string_leaf:STRING,typed_map:MAP<STRING,STRUCT<string_leaf:STRING>>>
        ) UNIQUE KEY(pk)
        DISTRIBUTED BY HASH(pk) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "store_row_column" = "true"
        )
    """
    sql """
        INSERT INTO rowstore_nested_pruning_minimal VALUES
        (2, NAMED_STRUCT('string_leaf', 'b-typed-2',
            'typed_map', MAP('b_key', NAMED_STRUCT('string_leaf', 'b-map-typed-2'))))
    """
    sql "SYNC"
    sql "SET enable_sql_cache=false"
    sql "SET query_timeout=10"

    sql "SET enable_short_circuit_query=false"
    sql "SET enable_prune_nested_column=true"
    order_qt_without_short_circuit """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """

    sql "SET enable_short_circuit_query=true"
    sql "SET enable_prune_nested_column=false"
    order_qt_without_pruning """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """

    sql "SET enable_prune_nested_column=true"
    explain {
        sql """
            SELECT CHARACTER_LENGTH(struct_element(element_at(
                struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
            FROM rowstore_nested_pruning_minimal WHERE pk=2
        """
        contains "SHORT-CIRCUIT"
    }
    order_qt_short_circuit_with_pruning """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """
    order_qt_short_circuit_with_pruning_cached """
        SELECT CHARACTER_LENGTH(struct_element(element_at(
            struct_element(typed, 'typed_map'), 'tail'), 'string_leaf')) AS character_len
        FROM rowstore_nested_pruning_minimal WHERE pk=2
    """
}
