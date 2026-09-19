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

suite("test_generated_column_like_session") {
    sql "DROP TABLE IF EXISTS test_gencol_like_decimal_src"
    sql "DROP TABLE IF EXISTS test_gencol_like_decimal_dst"
    sql "DROP TABLE IF EXISTS test_gencol_like_decimal_copy"
    sql "SET enable_decimal256 = true"
    sql """
        CREATE TABLE test_gencol_like_decimal_src (
            a DECIMAL(20,5), b DECIMAL(21,6),
            c DECIMAL(38,11) GENERATED ALWAYS AS (a * b) NOT NULL
        )
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "SET enable_decimal256 = false"
    sql "CREATE TABLE test_gencol_like_decimal_dst LIKE test_gencol_like_decimal_src"
    qt_session_after_like "SELECT @@enable_decimal256"
    sql "CREATE TABLE test_gencol_like_decimal_copy LIKE test_gencol_like_decimal_dst"
    sql "INSERT INTO test_gencol_like_decimal_src VALUES (1.12343, 1.123457, DEFAULT)"
    sql "INSERT INTO test_gencol_like_decimal_dst VALUES (1.12343, 1.123457, DEFAULT)"
    sql "INSERT INTO test_gencol_like_decimal_copy VALUES (1.12343, 1.123457, DEFAULT)"
    order_qt_decimal_true """
        SELECT 'src', a, b, c FROM test_gencol_like_decimal_src
        UNION ALL SELECT 'dst', a, b, c FROM test_gencol_like_decimal_dst
        UNION ALL SELECT 'copy', a, b, c FROM test_gencol_like_decimal_copy
    """

    sql "DROP TABLE IF EXISTS test_gencol_like_decimal_false_src"
    sql "DROP TABLE IF EXISTS test_gencol_like_decimal_false_dst"
    sql """
        CREATE TABLE test_gencol_like_decimal_false_src (
            a DECIMAL(20,5), b DECIMAL(21,6),
            c DECIMAL(38,11) GENERATED ALWAYS AS (a * b) NOT NULL
        )
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "SET enable_decimal256 = true"
    sql "CREATE TABLE test_gencol_like_decimal_false_dst LIKE test_gencol_like_decimal_false_src"
    qt_session_after_reverse_like "SELECT @@enable_decimal256"
    sql "INSERT INTO test_gencol_like_decimal_false_src VALUES (1.12343, 1.123457, DEFAULT)"
    sql "INSERT INTO test_gencol_like_decimal_false_dst VALUES (1.12343, 1.123457, DEFAULT)"
    order_qt_decimal_false """
        SELECT 'src', a, b, c FROM test_gencol_like_decimal_false_src
        UNION ALL SELECT 'dst', a, b, c FROM test_gencol_like_decimal_false_dst
    """

    sql "DROP TABLE IF EXISTS test_gencol_like_sql_mode_src"
    sql "DROP TABLE IF EXISTS test_gencol_like_sql_mode_dst"
    sql "SET sql_mode = 'PIPES_AS_CONCAT'"
    sql """
        CREATE TABLE test_gencol_like_sql_mode_src (
            a VARCHAR(10), c VARCHAR(20) GENERATED ALWAYS AS (a || 'x')
        )
        DISTRIBUTED BY HASH(a) BUCKETS 1
        PROPERTIES("replication_num" = "1")
    """
    sql "SET sql_mode = ''"
    sql "CREATE TABLE test_gencol_like_sql_mode_dst LIKE test_gencol_like_sql_mode_src"
    qt_sql_mode_after_like "SELECT @@sql_mode"
    sql "INSERT INTO test_gencol_like_sql_mode_src VALUES ('a', DEFAULT), (NULL, DEFAULT)"
    sql "INSERT INTO test_gencol_like_sql_mode_dst VALUES ('a', DEFAULT), (NULL, DEFAULT)"
    order_qt_sql_mode """
        SELECT 'src', a, c FROM test_gencol_like_sql_mode_src
        UNION ALL SELECT 'dst', a, c FROM test_gencol_like_sql_mode_dst
    """
}
