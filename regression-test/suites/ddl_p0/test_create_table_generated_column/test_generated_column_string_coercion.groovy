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

suite("test_generated_column_string_coercion") {
    sql "DROP TABLE IF EXISTS test_generated_string_chain"
    sql """
        CREATE TABLE test_generated_string_chain (
            id INT,
            a VARCHAR(10),
            c VARCHAR(2) AS (a),
            d INT AS (length(c))
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "INSERT INTO test_generated_string_chain(id, a) VALUES (1, 'abcd'), (2, 'a'), (3, NULL)"
    order_qt_generated_values "SELECT *, length(c) FROM test_generated_string_chain"
    sql """
        INSERT INTO test_generated_string_chain(id, a)
        SELECT id + 10, a FROM test_generated_string_chain
    """
    order_qt_generated_select "SELECT *, length(c) FROM test_generated_string_chain"

    sql "DROP TABLE IF EXISTS test_generated_string_input"
    sql """
        CREATE TABLE test_generated_string_input (
            id INT,
            a VARCHAR(2),
            d INT AS (length(a))
        ) DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "INSERT INTO test_generated_string_input(id, a) VALUES (1, 'abcd'), (2, 'a'), (3, NULL)"
    order_qt_ordinary_values "SELECT *, length(a) FROM test_generated_string_input"
    sql """
        INSERT INTO test_generated_string_input(id, a)
        SELECT id + 20, a FROM test_generated_string_chain
    """
    order_qt_ordinary_select "SELECT *, length(a) FROM test_generated_string_input"
}
