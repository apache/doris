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

suite("test_cast_float_decimal_rounding") {
    sql "set enable_decimal256 = true"
    sql "drop table if exists test_cast_float_decimal_rounding"
    sql """create table test_cast_float_decimal_rounding (
        id int, d double, f float
    ) distributed by hash(id) buckets 1 properties("replication_num" = "1")"""
    sql """insert into test_cast_float_decimal_rounding values
        (1, 9, 9), (2, -9, -9), (3, 9.25, 9.25), (4, -9.25, -9.25),
        (5, 0.49999999999999994, 0.49999997),
        (6, -0.49999999999999994, -0.49999997),
        (7, 0.5, 0.5), (8, -0.5, -0.5), (9, 2.5, 2.5), (10, -2.5, -2.5),
        (11, 9.5, 9.5), (12, -9.5, -9.5), (13, 10, 10), (14, -10, -10),
        (15, null, null),
        (16, 9.99, 0), (17, -9.99, 0),
        (18, 9.9921875, 0), (19, -9.9921875, 0),
        (20, 4503599627370497, 0), (21, -4503599627370497, 0),
        (22, 9007199254740991, 0), (23, -9007199254740991, 0),
        (24, 999999999, 0), (25, -999999999, 0),
        (26, 1000000000000000000, 0), (27, -1000000000000000000, 0),
        (28, cast('340282366920938539021238333346091630592' as double), 0),
        (29, cast('-340282366920938539021238333346091630592' as double), 0),
        (30, 4503599.627370497, 0), (31, -4503599.627370497, 0),
        (32, 1, 1), (33, -1, -1), (34, 0.15, 0), (35, -0.15, 0)
    """

    for (def strict : [false, true]) {
        sql "set enable_strict_cast = ${strict}"
        "qt_bounds_${strict}" """select id, cast(d as decimalv3(1,0)), cast(f as decimalv3(1,0))
            from test_cast_float_decimal_rounding where id <= 10 order by id"""
        "qt_scaled_bounds_${strict}" """select id, cast(d as decimalv3(3,2))
            from test_cast_float_decimal_rounding where id between 16 and 19 order by id"""
        "qt_large_${strict}" """select id, cast(d as decimalv3(16,0)), cast(d as decimalv3(38,0)),
            cast(d as decimalv3(39,1)) from test_cast_float_decimal_rounding
            where id between 20 and 23 order by id"""
        "qt_decimal32_${strict}" """select id, cast(d as decimalv3(9,0))
            from test_cast_float_decimal_rounding where id in (24,25) order by id"""
        "qt_decimal256_${strict}" """select id, cast(d as decimalv3(76,0))
            from test_cast_float_decimal_rounding where id in (28,29) order by id"""
        "qt_decimalv2_${strict}" """select id, cast(d as decimalv2(27,9))
            from test_cast_float_decimal_rounding where id in (30,31) order by id"""
        "qt_constants_${strict}" """select cast(cast('9.25' as double) as decimalv3(1,0)),
            cast(cast('-9' as float) as decimalv3(1,0)),
            cast(cast('4503599627370497' as double) as decimalv3(16,0)),
            cast(cast('9007199254740991' as double) as decimalv3(39,1))"""
        "qt_backing_width_${strict}" """select * from (
            select id, cast(d as decimalv3(38,1)), cast(d as decimalv3(39,1)),
                cast(cast('0.15' as double) as decimalv3(38,1)),
                cast(cast('0.15' as double) as decimalv3(39,1))
                from test_cast_float_decimal_rounding where id = 34
            union all
            select id, cast(d as decimalv3(38,1)), cast(d as decimalv3(39,1)),
                cast(cast('-0.15' as double) as decimalv3(38,1)),
                cast(cast('-0.15' as double) as decimalv3(39,1))
                from test_cast_float_decimal_rounding where id = 35
            ) t order by id"""
    }
    sql "set enable_strict_cast = false"
    qt_overflow """select id, cast(d as decimalv3(1,0)), cast(f as decimalv3(1,0))
        from test_cast_float_decimal_rounding where id between 11 and 15 order by id"""
    qt_rounded_bound """select id, cast(d as decimalv3(18,0))
        from test_cast_float_decimal_rounding where id in (26,27) order by id"""
    qt_high_scale_overflow """select id, cast(d as decimalv3(38,37)), cast(d as decimalv3(76,75))
        from test_cast_float_decimal_rounding where id in (13,14) order by id"""
    qt_full_scale_overflow """select id, cast(d as decimalv3(38,38)), cast(d as decimalv3(76,76))
        from test_cast_float_decimal_rounding where id in (32,33) order by id"""
    sql "set enable_strict_cast = true"
    for (def type : ["decimalv3(38,37)", "decimalv3(76,75)"]) {
        for (def id : [13,14]) {
            test {
                sql "select cast(d as ${type}) from test_cast_float_decimal_rounding where id = ${id}"
                exception "Arithmetic overflow"
            }
        }
    }
    for (def type : ["decimalv3(38,38)", "decimalv3(76,76)"]) {
        for (def id : [32,33]) {
            test {
                sql "select cast(d as ${type}) from test_cast_float_decimal_rounding where id = ${id}"
                exception "Arithmetic overflow"
            }
        }
    }
    for (def id : [11, 12, 13, 14]) {
        test {
            sql "select cast(d as decimalv3(1,0)) from test_cast_float_decimal_rounding where id = ${id}"
            exception "Arithmetic overflow"
        }
        test {
            sql "select cast(f as decimalv3(1,0)) from test_cast_float_decimal_rounding where id = ${id}"
            exception "Arithmetic overflow"
        }
    }
    for (def id : [26, 27]) {
        test {
            sql "select cast(d as decimalv3(18,0)) from test_cast_float_decimal_rounding where id = ${id}"
            exception "Arithmetic overflow"
        }
    }
}
