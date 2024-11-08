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

suite("test_template_two_args") {
    // this table has nothing todo. just make it eaiser to generate query
    sql " drop table if exists hits_two_args "
    sql """ create table hits_two_args(
                nothing boolean
            )
            properties("replication_num" = "1");
    """
    sql "insert into hits_two_args values(true);"

    sql " drop table if exists arg1_two_args"
    sql """
        create table arg1_two_args (
            k0 int,
            a double not null,
            b double null,
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select atan2(a, a) from arg1_two_args"
    order_qt_empty_not_nullable "select atan2(b, b) from arg1_two_args"
    order_qt_empty_partial_nullable "select atan2(a, b) from arg1_two_args"

    sql "insert into arg1_two_args values (1, 1, null), (1, 1, null), (1, 1, null)"
    order_qt_all_null "select atan2(b, b) from arg1_two_args"

    sql "truncate table arg1_two_args"
    sql """ insert into arg1_two_args values (1, 1e-100, 1e-100), (2, -1e100, -1e100), (3, 1e100, 1e100), (4, 1, 1), (5, -1, -1),
        (6, 0, 0), (7, -0, -0), (8, 123, 123),
        (9, 0.1, 0.1), (10, -0.1, -0.1), (11, 1e-15, 1e-15), (12, 0, null);
    """

    /// all values
    order_qt_nullable """
        SELECT atan2(t.arg1_two_args, t.ARG2) as result
        FROM (
            SELECT hits_two_args.nothing, TABLE1.arg1_two_args, TABLE1.order1, TABLE2.ARG2, TABLE2.order2
            FROM hits_two_args
            CROSS JOIN (
                SELECT b as arg1_two_args, k0 as order1
                FROM arg1_two_args
            ) as TABLE1
            CROSS JOIN (
                SELECT b as ARG2, k0 as order2
                FROM arg1_two_args
            ) as TABLE2
        )t;
    """

    /// nullables
    order_qt_not_nullable "select atan2(a, a) from arg1_two_args"
    order_qt_partial_nullable "select atan2(a, b) from arg1_two_args"
    order_qt_nullable_no_null "select atan2(a, nullable(a)) from arg1_two_args"
    /// if you set `use_default_implementation_for_nulls` to false, add:
    // order_qt_nullable1 " SELECT b as arg1_two_args...)as TABLE1 ... SELECT a as arg1_two_args...)as TABLE1
    // order_qt_nullable2 " SELECT a as arg1_two_args...)as TABLE1 ... SELECT b as arg1_two_args...)as TABLE1

    /// consts. most by BE-UT
    order_qt_const_nullable "select atan2(NULL, NULL) from arg1_two_args"
    order_qt_partial_const_nullable "select atan2(NULL, b) from arg1_two_args"
    order_qt_const_not_nullable "select atan2(0.5, 100) from arg1_two_args"
    order_qt_const_other_nullable "select atan2(10, b) from arg1_two_args"
    order_qt_const_other_not_nullable "select atan2(a, 10) from arg1_two_args"
    order_qt_const_nullable_no_null "select atan2(nullable(1e100), nullable(1e-10))"
    order_qt_const_nullable_no_null_multirows "select atan2(nullable(1e100), nullable(1e-10))"
    order_qt_const_partial_nullable_no_null "select atan2(1e100, nullable(1e-10))"

    /// folding
    def re_fe
    def re_be
    def re_no_fold
    def check_three_ways = { test_sql ->
        re_fe = order_sql "select/*+SET_VAR(enable_fold_constant_by_be=false)*/ ${test_sql}"
        re_be = order_sql "select/*+SET_VAR(enable_fold_constant_by_be=true)*/ ${test_sql}"
        re_no_fold = order_sql "select/*+SET_VAR(debug_skip_fold_constant=true)*/ ${test_sql}"
        logger.info("check on sql ${test_sql}")
        assertEquals(re_fe, re_be)
        assertEquals(re_fe, re_no_fold)
    }

    check_three_ways "atan2(-1, -2)"
    check_three_ways "atan2(-1e100, 3.14)"
    check_three_ways "atan2(0, 0)"
    check_three_ways "atan2(1e100, 1e100)"
    check_three_ways "atan2(-0.5, 0.5)"
}