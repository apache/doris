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

suite("test_printf") {
    sql "DROP TABLE IF EXISTS test_printf"
    sql """
        CREATE TABLE test_printf (
            id INT,
            fmt STRING,
            int_arg INT,
            bigint_arg BIGINT,
            double_arg DOUBLE,
            string_arg STRING,
            decimal_arg DECIMAL(18, 4)
        ) DISTRIBUTED BY HASH(id) BUCKETS 1
        PROPERTIES ("replication_num" = "1")
    """

    qt_empty "SELECT printf(fmt, int_arg, string_arg, double_arg) FROM test_printf ORDER BY id"
    sql """
        INSERT INTO test_printf VALUES
            (1, '%d-%s-%.2f', 100, 9223372036854775807, 3.14, 'test', 123.4567),
            (2, '%d-%s-%.2f', -8, -9223372036854775808, -1.25, '中文', -987.6543),
            (3, '%d-%s-%.2f', 0, 0, 0, '', 0),
            (4, NULL, 1, 1, 1, 'null format', 1),
            (5, '%d-%s-%.2f', NULL, 1, 1, 'null value', 1)
    """

    qt_format_only "SELECT printf('hello world'), printf(''), printf('100%%')"
    qt_mixed "SELECT printf('%d-%s-%.2f', 100, 'test', 3.14)"
    qt_padding "SELECT printf('[%5d][%-5d][%05d]', 8, 8, 8)"
    qt_integer_types """SELECT printf('%d %d %d %ld', CAST(-8 AS TINYINT),
            CAST(-16 AS SMALLINT), CAST(-32 AS INT), CAST(-64 AS BIGINT))"""
    qt_integer_formats "SELECT printf('%d %o %x %X', 123, 123, 123, 123)"
    qt_argument_positions "SELECT printf('%1\$d %1\$d %1\$d', 123)"
    qt_dynamic_width "SELECT printf('[%*.*f]', 7, 2, 1.25)"
    qt_string_padding "SELECT printf('[%10s][%-10s]', 'hello', 'hello')"
    qt_char_varchar """SELECT printf(CAST('%s' AS CHAR(2)), CAST('hello' AS VARCHAR(10))),
            printf(CAST('%s' AS VARCHAR(2)), CAST('world' AS CHAR(5)))"""
    qt_scientific "SELECT printf('%e %E', CAST(1.25 AS FLOAT), CAST(4.5 AS DOUBLE))"
    qt_decimal "SELECT printf('%.2f', CAST(123.456 AS DECIMAL(10, 3)))"
    qt_null "SELECT printf(NULL), printf('%s', NULL), printf('%d %s', 1, NULL)"
    order_qt_column_format "SELECT id, printf(fmt, int_arg, string_arg, double_arg) FROM test_printf ORDER BY id"
    order_qt_constant_format "SELECT id, printf('%d-%s-%.2f', int_arg, string_arg, double_arg) FROM test_printf ORDER BY id"
    order_qt_constant_args "SELECT id, printf(fmt, 100, 'test', 3.14) FROM test_printf ORDER BY id"
    order_qt_bigint "SELECT id, printf('%ld', bigint_arg) FROM test_printf ORDER BY id"
    order_qt_decimal_column "SELECT id, printf('%.2f', decimal_arg) FROM test_printf ORDER BY id"

    test {
        sql "SELECT printf('%d')"
        exception "failed to format string"
    }
    test {
        sql "SELECT printf('%', 1)"
        exception "failed to format string"
    }
    test {
        sql "SELECT printf('%d', CAST(1.25 AS DOUBLE))"
        exception "failed to format string"
    }
    test {
        sql "SELECT printf(1)"
        exception "must be string"
    }
    test {
        sql "SELECT printf('%d', CAST(1 AS LARGEINT))"
        exception "does not support printf type"
    }
    test {
        sql "SELECT printf('%s', ARRAY(1, 2))"
        exception "does not support printf type"
    }
}
