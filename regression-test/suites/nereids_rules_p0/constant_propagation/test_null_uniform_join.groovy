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

suite("test_null_uniform_join") {
    sql "SET disable_nereids_rules='INFER_JOIN_NOT_NULL'"

    sql "DROP TABLE IF EXISTS test_null_uniform_join_left"
    sql "DROP TABLE IF EXISTS test_null_uniform_join_right"

    sql """
        CREATE TABLE test_null_uniform_join_left (
            id INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        CREATE TABLE test_null_uniform_join_right (
            id INT
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    sql "INSERT INTO test_null_uniform_join_left VALUES (1), (2)"
    sql "INSERT INTO test_null_uniform_join_right VALUES (3), (4)"

    qt_null_padded_equality_plan """
        EXPLAIN SHAPE PLAN
        SELECT left_input.preserved_id, right_input.preserved_id
        FROM (
            SELECT l.id AS preserved_id, r.id AS padded_id
            FROM test_null_uniform_join_left l
            LEFT JOIN test_null_uniform_join_right r ON FALSE
        ) left_input
        INNER JOIN (
            SELECT l.id AS preserved_id, r.id AS padded_id
            FROM test_null_uniform_join_right l
            LEFT JOIN test_null_uniform_join_left r ON FALSE
        ) right_input
        ON left_input.padded_id = right_input.padded_id
    """

    qt_cast_null_equality_plan """
        EXPLAIN SHAPE PLAN
        SELECT left_input.id, right_input.id
        FROM (
            SELECT id, CAST(NULL AS INT) AS null_key
            FROM test_null_uniform_join_left
        ) left_input
        INNER JOIN (
            SELECT id, CAST(NULL AS INT) AS null_key
            FROM test_null_uniform_join_right
        ) right_input
        ON left_input.null_key = right_input.null_key
    """

}
