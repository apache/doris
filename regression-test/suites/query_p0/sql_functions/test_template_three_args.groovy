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

suite("test_template_three_args") {
    // this table has nothing todo. just make it eaiser to generate query
    sql " drop table if exists hits_three_args "
    sql """ create table hits_three_args(
                nothing boolean
            )
            properties("replication_num" = "1");
    """
    sql "insert into hits_three_args values(true);"

    sql " drop table if exists arg1_three_args"
    sql """
        create table arg1_three_args (
            k0 int,
            a varchar not null,
            b varchar null,
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """

    order_qt_empty_nullable "select concat(a, a, a) from arg1_three_args"
    order_qt_empty_not_nullable "select concat(b, b, b) from arg1_three_args"
    order_qt_empty_partial_nullable "select concat(a, b, b) from arg1_three_args"

    sql "insert into arg1_three_args values (1, 1, null), (1, 1, null), (1, 1, null)"
    order_qt_all_null "select concat(b, b, b ,b) from arg1_three_args"

    sql "truncate table arg1_three_args"
    sql """ insert into arg1_three_args values (1, "", ""), (2, "中文", "中文"), (3, "123123", "123123"),
            (4, "\\\\a\\\\b\\\\c\\\\d", "\\\\a\\\\b\\\\c\\\\d"),
            (5, "!@#@#\$#^\$%%\$^", "!@#@#\$#^\$%%\$^"), (6, "   ", "   "),
            (7, "", NULL);
    """

    order_qt_nullable """
        SELECT concat(t.arg1_three_args, t.ARG2, t.ARG3) as result
        FROM (
            SELECT hits_three_args.nothing, TABLE1.arg1_three_args, TABLE1.order1, TABLE2.ARG2, TABLE2.order2, TABLE3.ARG3, TABLE3.order3
            FROM hits_three_args
            CROSS JOIN (
                SELECT b as arg1_three_args, k0 as order1
                FROM arg1_three_args
            ) as TABLE1
            CROSS JOIN (
                SELECT b as ARG2, k0 as order2
                FROM arg1_three_args
            ) as TABLE2
            CROSS JOIN (
                SELECT b as ARG3, k0 as order3
                FROM arg1_three_args
            ) as TABLE3
        )t;
    """

    /// nullables
    order_qt_not_nullable "select concat(a, a, a) from arg1_three_args"
    order_qt_partial_nullable "select concat(a, b, b) from arg1_three_args"
    order_qt_nullable_no_null "select concat(a, nullable(a), nullable(a)) from arg1_three_args"
    /// if you set `use_default_implementation_for_nulls` to false, add:
    // order_qt_nullable1 " SELECT b as arg1_three_args...)as TABLE1 ... SELECT a as arg1_three_args...)as TABLE1 ...
    // order_qt_nullable2 " SELECT a as arg1_three_args...)as TABLE1 ... SELECT b as arg1_three_args...)as TABLE1 ...

    /// consts. most by BE-UT
    order_qt_const_nullable "select concat(NULL, NULL, NULL) from arg1_three_args"
    order_qt_partial_const_nullable "select concat(NULL, b, b) from arg1_three_args"
    order_qt_const_not_nullable "select concat('a', 'b', 'c') from arg1_three_args"
    order_qt_const_other_nullable "select concat('x', b, b) from arg1_three_args"
    order_qt_const_other_not_nullable "select concat('x', 'x', a) from arg1_three_args"
    order_qt_const_nullable_no_null "select concat(nullable('abc'), nullable('中文'), nullable('xxx'))"
    order_qt_const_partial_nullable_no_null "select concat('xyz', nullable('a'), nullable('a'))"
    order_qt_const1 "select concat('xyz', a, b) from arg1_three_args"
    order_qt_const12 "select concat('xyz', 'abc', b) from arg1_three_args"
    order_qt_const23 "select concat(a, 'xyz', 'abc') from arg1_three_args"
    order_qt_const3 "select concat(b, a, 'abc') from arg1_three_args"

    /// folding
    def re_fe
    def re_be
    def re_no_fold
    def check_three_ways = { test_sql ->
        re_fe = order_sql "select/*+SET_VAR(enable_fold_constant_by_be=false)*/ ${test_sql}"
        re_be = order_sql "select/*+SET_VAR(enable_fold_constant_by_be=true)*/ ${test_sql}"
        re_no_fold = order_sql "select/*+SET_VAR(debug_skip_fold_constant=true)*/ ${test_sql}"
        logger.info("check on sql \${test_sql}")
        assertEquals(re_fe, re_be)
        assertEquals(re_fe, re_no_fold)
    }

    check_three_ways "concat('', '', '')"
    check_three_ways "concat('\\t\\t', '\\t\\t', '\\t\\t')"
    check_three_ways "concat('中文', '中文', '中文')"
    check_three_ways "concat('abcde', 'abcde', 'abcde')"
}