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

suite("nested_lambda_outer_argument_pruning") {
    sql """ DROP TABLE IF EXISTS nested_lambda_outer_argument_pruning_tbl """
    sql """
        CREATE TABLE nested_lambda_outer_argument_pruning_tbl (
            id      INT,
            maps    ARRAY<MAP<INT, STRUCT<a: INT, b: INT>>> NULL
        ) ENGINE = OLAP
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_allocation" = "tag.location.default: 1")
    """
    sql """
        INSERT INTO nested_lambda_outer_argument_pruning_tbl VALUES
            (1, array(map(1, named_struct('a', 10, 'b', 100)))),
            (2, array(map(2, named_struct('a', 20, 'b', 200)))),
            (3, array()),
            (4, NULL)
    """

    // The inner lambda captures m from the immediately enclosing lambda.
    explain {
        sql """
            SELECT array_map(
                       m -> array_map(x -> element_at(map_values(m)[1], 'a') + x, [1]),
                       maps)
            FROM nested_lambda_outer_argument_pruning_tbl
        """
        contains "nested columns"
        contains "maps.*.VALUES.a"
        notContains "maps.*.VALUES.b"
    }

    order_qt_capture_parent """
        SELECT id,
               array_map(
                   m -> array_map(x -> element_at(map_values(m)[1], 'a') + x, [1]),
                   maps)
        FROM nested_lambda_outer_argument_pruning_tbl
        ORDER BY id
    """

    // Two sibling lambdas both capture m. This also verifies that entering and
    // leaving one inner lambda does not leak or discard the outer scope.
    explain {
        sql """
            SELECT array_map(
                       m -> array_concat(
                           array_map(x -> element_at(map_values(m)[1], 'a') + x, [1]),
                           array_map(y -> element_at(map_values(m)[1], 'b') + y, [1])),
                       maps)
            FROM nested_lambda_outer_argument_pruning_tbl
        """
        contains "nested columns"
        contains "maps.*.VALUES.a"
        contains "maps.*.VALUES.b"
    }

    order_qt_capture_sibling_lambdas """
        SELECT id,
               array_map(
                   m -> array_concat(
                       array_map(x -> element_at(map_values(m)[1], 'a') + x, [1]),
                       array_map(y -> element_at(map_values(m)[1], 'b') + y, [1])),
                   maps)
        FROM nested_lambda_outer_argument_pruning_tbl
        ORDER BY id
    """
}
