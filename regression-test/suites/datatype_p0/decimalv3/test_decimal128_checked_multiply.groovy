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

suite("test_decimal128_checked_multiply") {
    sql "set check_overflow_for_decimal = true"
    sql "set enable_decimal256 = false"
    // Keep constant expressions in the BE so constant multiplication paths are exercised.
    sql "set debug_skip_fold_constant = true"

    sql "drop table if exists test_decimal128_checked_multiply"
    sql """
        create table test_decimal128_checked_multiply (
            id int,
            a decimal(38, 0),
            b decimal(38, 0)
        ) distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_decimal128_checked_multiply values
            (1, 0, 99999999999999999999999999999999999999),
            (2, 1, 99999999999999999999999999999999999999),
            (3, -1, 99999999999999999999999999999999999999),
            (4, -99999999999999999999999999999999999999, -1),
            (5, 999999999999999999, 999999999999999999),
            (6, -999999999999999999, 999999999999999999),
            (7, 18446744073709551616, 4611686018427387904),
            (8, -18446744073709551616, 4611686018427387904),
            (9, 18446744073709551616, -4611686018427387904),
            (10, -18446744073709551616, -4611686018427387904),
            (11, null, 2),
            (12, 2, null)
    """

    qt_vector_vector """
        select id, a * b, b * a
        from test_decimal128_checked_multiply order by id
    """
    qt_vector_constant """
        select id, a * cast('0' as decimal(1, 0)), a * cast('-1' as decimal(1, 0))
        from test_decimal128_checked_multiply order by id
    """
    qt_constant_vector """
        select id, cast('0' as decimal(1, 0)) * a, cast('-1' as decimal(1, 0)) * a
        from test_decimal128_checked_multiply order by id
    """
    qt_constant_constant """
        select cast('-18446744073709551616' as decimal(20, 0))
                 * cast('4611686018427387904' as decimal(19, 0))
    """

    sql "drop table if exists test_decimal128_checked_multiply_overflow"
    sql """
        create table test_decimal128_checked_multiply_overflow (
            id int,
            a decimal(38, 0),
            b decimal(38, 0)
        ) distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_decimal128_checked_multiply_overflow values
            (1, 85070591730234615865843651857942052864, 2),
            (2, -85070591730234615865843651857942052864, -2),
            (3, -85070591730234615865843651857942052864, 3),
            (4, 18446744073709551616, 18446744073709551616),
            (5, -18446744073709551616, 18446744073709551616),
            (6, -18446744073709551616, -18446744073709551616),
            (7, 60000000000000000000000000000000000000, 2)
    """
    // Cover signed-range overflow, unsigned-128 overflow and Decimal(38,0) precision overflow.
    for (int id = 1; id <= 7; ++id) {
        test {
            sql "select a * b from test_decimal128_checked_multiply_overflow where id = ${id}"
            exception "Arithmetic overflow"
        }
    }
    test {
        sql """
            select a * cast('2' as decimal(1, 0))
            from test_decimal128_checked_multiply_overflow where id = 1
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast('2' as decimal(1, 0)) * a
            from test_decimal128_checked_multiply_overflow where id = 1
        """
        exception "Arithmetic overflow"
    }
    test {
        sql """
            select cast('85070591730234615865843651857942052864' as decimal(38, 0))
                     * cast('2' as decimal(1, 0))
        """
        exception "Arithmetic overflow"
    }

    sql "drop table if exists test_decimal128_checked_multiply_scaled"
    sql """
        create table test_decimal128_checked_multiply_scaled (
            id int,
            price decimal(15, 2),
            discount decimal(15, 2),
            tax decimal(15, 2),
            a decimal(38, 18),
            b decimal(38, 18)
        ) distributed by hash(id) buckets 1
        properties("replication_num" = "1")
    """
    sql """
        insert into test_decimal128_checked_multiply_scaled values
            (1, 12345.67, 0.06, 0.08, -15687.000000000000000000, 0.011000000000000000),
            (2, -12345.67, 0.06, 0.08, 15687.000000000000000000, 0.011000000000000000),
            (3, 0.00, 0.00, 0.00, -15687.000000000000000000, -0.011000000000000000),
            (4, null, 0.06, 0.08, null, 0.011000000000000000)
    """
    // The scaled products exercise the wider intermediate fallback used after int128 overflow.
    qt_scaled_products """
        select id, price * (1 - discount), price * (1 - discount) * (1 + tax), a * b
        from test_decimal128_checked_multiply_scaled order by id
    """
}
