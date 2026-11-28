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

suite("test_left_truncated_utf8") {
    sql "DROP TABLE IF EXISTS test_left_truncated_utf8"
    sql """
        CREATE TABLE test_left_truncated_utf8 (id INT, s STRING, n INT)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        INSERT INTO test_left_truncated_utf8 VALUES
        (1, CAST(UNHEX('E4B8') AS STRING), 1), (2, 'Z', 1),
        (3, CAST(UNHEX('C2') AS STRING), 1), (4, 'Z', 1),
        (5, CAST(UNHEX('E4') AS STRING), 1), (6, 'Z', 1),
        (7, CAST(UNHEX('F0') AS STRING), 1), (8, 'Z', 1),
        (9, CAST(UNHEX('F09F') AS STRING), 1), (10, 'Z', 1),
        (11, CAST(UNHEX('F09F98') AS STRING), 1), (12, 'Z', 1),
        (13, CAST(UNHEX('61E4B8') AS STRING), 1),
        (14, CAST(UNHEX('61E4B8') AS STRING), 2),
        (15, CAST(UNHEX('E4B8ADE4B8') AS STRING), 2),
        (16, CAST(UNHEX('00E4B8') AS STRING), 2),
        (17, CAST(UNHEX('E4B8') AS STRING), 0),
        (18, CAST(UNHEX('E4B8') AS STRING), -1),
        (19, CAST(UNHEX('E4B8') AS STRING), 2147483647),
        (20, '', 1), (21, NULL, 1),
        (22, CAST(UNHEX('E4B8') AS STRING), NULL),
        (23, CAST(UNHEX('E4B8ADF09F9880') AS STRING), 2),
        (24, CAST(UNHEX('E4B8') AS STRING), 1)
    """

    sql "SET debug_skip_fold_constant = true"
    sql "SET enable_fold_constant_by_be = false"
    qt_vector """
        SELECT id, HEX(s), LENGTH(s), HEX(LEFT(s, 1)), HEX(LEFT(s, n)),
               HEX(STRLEFT(s, n)), HEX(SUBSTRING(s, 1, n))
        FROM test_left_truncated_utf8 ORDER BY id
    """
    qt_const_string """
        SELECT id, n, HEX(LEFT(CAST(UNHEX('E4B8') AS STRING), n))
        FROM test_left_truncated_utf8 ORDER BY id
    """
    order_qt_scalar """
        SELECT HEX(LEFT(CAST(UNHEX('C2') AS STRING), 1)),
               HEX(LEFT(CAST(UNHEX('E4') AS STRING), 1)),
               HEX(LEFT(CAST(UNHEX('E4B8') AS STRING), 1)),
               HEX(LEFT(CAST(UNHEX('F0') AS STRING), 1)),
               HEX(LEFT(CAST(UNHEX('F09F') AS STRING), 1)),
               HEX(LEFT(CAST(UNHEX('F09F98') AS STRING), 1))
    """

    sql "DROP TABLE IF EXISTS test_left_truncated_utf8_ctas"
    sql """
        CREATE TABLE test_left_truncated_utf8_ctas
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
        AS SELECT id, LEFT(s, n) AS s FROM test_left_truncated_utf8
    """
    qt_ctas "SELECT id, HEX(s), LENGTH(s) FROM test_left_truncated_utf8_ctas ORDER BY id"

    sql "DROP TABLE IF EXISTS test_left_truncated_utf8_insert"
    sql """
        CREATE TABLE test_left_truncated_utf8_insert (id INT, s STRING)
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ('replication_num' = '1')
    """
    sql """
        INSERT INTO test_left_truncated_utf8_insert
        SELECT id, LEFT(s, n) FROM test_left_truncated_utf8
    """
    qt_insert "SELECT id, HEX(s), LENGTH(s) FROM test_left_truncated_utf8_insert ORDER BY id"

    sql "SET debug_skip_fold_constant = false"
    sql "SET enable_fold_constant_by_be = true"
    order_qt_folded """
        SELECT HEX(LEFT(CAST(UNHEX('E4B8') AS STRING), 1)),
               HEX(STRLEFT(CAST(UNHEX('F09F98') AS STRING), 1)),
               HEX(LEFT(CAST(UNHEX('61E4B8') AS STRING), 2))
    """
}
