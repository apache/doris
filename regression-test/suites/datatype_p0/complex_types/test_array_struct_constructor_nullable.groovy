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

// Regression test for ARRAY/MAP constructors over STRUCT arguments whose common type must be
// computed. `struct(100, repeat('x', 2))` is nullable in the varchar field while it is bound, and
// constant folding replaces `repeat('x', 2)` with the non nullable literal 'xx'. The folded
// struct argument then no longer matched the common type kept by `array(...)`, and the backend
// failed with `Bad cast from type:ColumnStr<unsigned int> to ColumnNullable` while building the
// nested struct column. The frontend now casts such arguments back to the expected types.
suite("test_array_struct_constructor_nullable") {
    sql "DROP TABLE IF EXISTS test_array_struct_constructor_nullable"
    sql """
        CREATE TABLE test_array_struct_constructor_nullable (
            id INT,
            a  ARRAY<STRUCT<i: INT, s: VARCHAR(16)>>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    // a single argument needs no common type
    sql """
        INSERT INTO test_array_struct_constructor_nullable
        VALUES (1, array(struct(100, repeat('x', 2))))
    """
    // two arguments of different integer types need a common type
    sql """
        INSERT INTO test_array_struct_constructor_nullable
        VALUES (2, array(struct(100, repeat('x', 2)), struct(200, repeat('y', 2))))
    """
    // the same type only differs in the varchar width
    sql """
        INSERT INTO test_array_struct_constructor_nullable
        VALUES (3, array(struct(100, repeat('x', 2)), struct(200, repeat('y', 5))))
    """
    // several rows in one statement
    sql """
        INSERT INTO test_array_struct_constructor_nullable
        VALUES (4, array(struct(100, repeat('x', 2)), struct(300, repeat('z', 2)))),
               (5, array(struct(100, repeat('x', 2)), struct(400, repeat('w', 2))))
    """
    // named_struct uses the same struct constructor
    sql """
        INSERT INTO test_array_struct_constructor_nullable
        VALUES (6, array(named_struct('i', 100, 's', repeat('x', 2)),
                        named_struct('i', 200, 's', repeat('y', 2))))
    """
    // non constant arguments must keep working
    sql "DROP TABLE IF EXISTS test_array_struct_constructor_nullable_src"
    sql """
        CREATE TABLE test_array_struct_constructor_nullable_src (
            id INT,
            s  VARCHAR(16)
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """INSERT INTO test_array_struct_constructor_nullable_src VALUES (1, 'x'), (2, 'y')"""
    sql """
        INSERT INTO test_array_struct_constructor_nullable
        SELECT id + 100, array(struct(id, repeat(s, 2)), struct(id + 100, repeat(s, 2)))
        FROM test_array_struct_constructor_nullable_src
    """

    order_qt_select_array """
        SELECT id, a FROM test_array_struct_constructor_nullable ORDER BY id
    """

    sql "DROP TABLE IF EXISTS test_map_struct_constructor_nullable"
    sql """
        CREATE TABLE test_map_struct_constructor_nullable (
            id INT,
            m  MAP<VARCHAR(16), STRUCT<i: INT, s: VARCHAR(16)>>
        )
        DUPLICATE KEY(id)
        DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """
    sql """
        INSERT INTO test_map_struct_constructor_nullable
        VALUES (1, map('a', struct(100, repeat('x', 2)), 'b', struct(200, repeat('y', 2))))
    """
    sql """
        INSERT INTO test_map_struct_constructor_nullable
        VALUES (2, map('a', struct(100, 'xx'), 'b', struct(200, 'yy')))
    """

    order_qt_select_map """
        SELECT id, m FROM test_map_struct_constructor_nullable ORDER BY id
    """
}
