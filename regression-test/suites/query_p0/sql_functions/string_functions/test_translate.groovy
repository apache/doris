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

suite("test_translate") {
    // this table has nothing todo. just make it eaiser to generate query
    sql " drop table if exists hits_three_args "
    sql """ create table hits_three_args(
                nothing boolean
            )
            properties("replication_num" = "1");
    """
    sql "insert into hits_three_args values(true);"

    sql " drop table if exists test_translate"
    sql """
        create table test_translate (
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

    order_qt_empty_nullable "select translate(a, a, a) from test_translate"
    order_qt_empty_not_nullable "select translate(b, b, b) from test_translate"
    order_qt_empty_partial_nullable "select translate(a, b, b) from test_translate"

    sql """ insert into test_translate values (1, "", ""), (2, "中文", "中文"), (3, "123123", "123123"),
            (4, "\\\\a\\\\b\\\\c\\\\d", "\\\\a\\\\b\\\\c\\\\d"),
            (5, "!@#@#\$#^\$%%\$^", "!@#@#\$#^\$%%\$^"), (6, "   ", "   "),
            (7, "", NULL);
    """

    order_qt_nullable """
        SELECT translate(t.test_translate, t.ARG2, t.ARG3) as result
        FROM (
            SELECT hits_three_args.nothing, TABLE1.test_translate, TABLE1.order1, TABLE2.ARG2, TABLE2.order2, TABLE3.ARG3, TABLE3.order3
            FROM hits_three_args
            CROSS JOIN (
                SELECT b as test_translate, k0 as order1
                FROM test_translate
            ) as TABLE1
            CROSS JOIN (
                SELECT b as ARG2, k0 as order2
                FROM test_translate
            ) as TABLE2
            CROSS JOIN (
                SELECT b as ARG3, k0 as order3
                FROM test_translate
            ) as TABLE3
        )t;
    """

    /// nullables
    order_qt_not_nullable "select translate(a, a, a) from test_translate"
    order_qt_partial_nullable "select translate(a, b, b) from test_translate"
    order_qt_nullable_no_null "select translate(a, nullable(a), nullable(a)) from test_translate"

    /// consts. most by BE-UT
    order_qt_const_nullable "select translate(NULL, NULL, NULL) from test_translate"
    order_qt_partial_const_nullable "select translate(NULL, b, b) from test_translate"
    order_qt_const_not_nullable "select translate('a', 'b', 'c') from test_translate"
    order_qt_const_other_nullable "select translate('x', b, b) from test_translate"
    order_qt_const_other_not_nullable "select translate('x', 'x', a) from test_translate"
    order_qt_const_nullable_no_null "select translate(nullable('abc'), nullable('中文'), nullable('xxx'))"
    order_qt_const_partial_nullable_no_null "select translate('xyz', nullable('a'), nullable('a'))"
    order_qt_const1 "select translate('xyz', a, b) from test_translate"
    order_qt_const12 "select translate('xyz', 'abc', b) from test_translate"
    order_qt_const23 "select translate(a, 'xyz', 'abc') from test_translate"
    order_qt_const3 "select translate(b, a, 'abc') from test_translate"

    /// folding
    def re_fe
    def re_be
    def re_no_fold
    def check_three_ways = { test_sql ->
        sql "set enable_fold_constant_by_be=false;"
        re_fe = order_sql "select ${test_sql}"
        sql "set enable_fold_constant_by_be=true;"
        re_be = order_sql "select ${test_sql}"
        sql "set debug_skip_fold_constant=true;"
        re_no_fold = order_sql "select ${test_sql}"
        logger.info("check on sql \${test_sql}")
        assertEquals(re_fe, re_be)
        assertEquals(re_fe, re_no_fold)
    }

    check_three_ways "translate('abcd', '', '');"
    check_three_ways "translate('abcda', 'a', 'z');"
    check_three_ways "translate('abcd', 'ac', 'z');"
    check_three_ways "translate('abcd', 'aac', 'zq');"
    check_three_ways "translate('abcd', 'aac', 'zqx');"
    check_three_ways "translate('abcd', 'aac', '中文x');"
    check_three_ways "translate('中文', '中', '文');"
    check_three_ways "translate('中文', '中', 'a');"
    check_three_ways "translate('\tt\tt\tt', '\t', 't');"

    order_qt_1 "select translate('abcd', '', '');"
    order_qt_2 "select translate('abcd', 'a', 'z')"
    order_qt_3 "select translate('abcda', 'a', 'z');"
    order_qt_4 "select translate('abcd', 'aac', 'zq');"
    order_qt_5 "select translate('abcd', 'aac', 'zqx');"
    order_qt_6 "select translate('abcd', 'aac', '中文x');"
    order_qt_7 "select translate('中文', '中', '文');"
    order_qt_8 "select translate('中文', '中', 'ab');"
    order_qt_9 "select translate('\tt\tt\tt', '\t', 't');"
}
