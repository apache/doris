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

suite("test_trim_in") {
    // this table has nothing todo. just make it eaiser to generate query
    sql " drop table if exists hits_two_args "
    sql """ create table hits_two_args(
                nothing boolean
            )
            properties("replication_num" = "1");
    """
    sql "insert into hits_two_args values(true);"

    sql " drop table if exists test_trim_in"
    sql """
        create table test_trim_in (
            k0 int,
            a varchar not null,
            b varchar null
        )
        DISTRIBUTED BY HASH(k0)
        PROPERTIES
        (
            "replication_num" = "1"
        );
    """
    order_qt_empty_nullable "select trim_in(a, 'x') from test_trim_in"
    order_qt_empty_not_nullable "select trim_in(b, 'x') from test_trim_in"

    sql "insert into test_trim_in values (1, '1', null), (1, '1', null), (1, '1', null)"
    order_qt_all_null "select trim_in(b, NULL) from test_trim_in"

    sql "truncate table test_trim_in"
    sql """ insert into test_trim_in values (1, "", ""), (2, "abcd", "ac"), (3, '  hello  ', 'he '),
                (4, ' hello world ', ' ehlowrd'),(5, ' hello world ', ' eh'),(6,'浙江省杭州市','余杭区'),(6,'c浙江省杭州市a','西湖区');
        """

    /// all values
    order_qt_nullable """
        SELECT atan2(t.arg1_two_args, t.ARG2) as result
        FROM (
            SELECT hits_two_args.nothing, TABLE1.arg1_two_args, TABLE1.order1, TABLE2.ARG2, TABLE2.order2
            FROM hits_two_args
            CROSS JOIN (
                SELECT b as arg1_two_args, k0 as order1
                FROM test_trim_in
            ) as TABLE1
            CROSS JOIN (
                SELECT b as ARG2, k0 as order2
                FROM test_trim_in
            ) as TABLE2
        )t;
    """

    /// nullables
    order_qt_not_nullable "select trim_in(a, 'he') from test_trim_in"
    order_qt_nullable "select trim_in(b, 'he') from test_trim_in"
    order_qt_not_nullable_null "select trim_in(a, NULL) from test_trim_in"
    order_qt_nullable_null "select trim_in(b, NULL) from test_trim_in"

    /// consts. most by BE-UT
    order_qt_const_nullable "select trim_in(NULL,NULL) from test_trim_in"
    order_qt_partial_const_nullable "select trim_in(NULL, 'he') from test_trim_in"
    order_qt_const_partial_nullable_no_null "select trim_in(nullable('abcd'), 'cde') from test_trim_in"
    order_qt_const_other_not_nullable "select trim_in(a, 'x') from test_trim_in"
    order_qt_const_not_nullable "select trim_in('abdc', 'df') from test_trim_in"



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

    check_three_ways "trim_in(' hello world ', ' ld')"
    check_three_ways "ltrim_in(' hello world ', ' ld')"
    check_three_ways "rtrim_in(' hello world ', ' ld')"
    check_three_ways "trim_in(' hello world ', ' ehlowrd')"
    check_three_ways "ltrim_in(' hello world ', ' ehlowrd')"
    check_three_ways "rtrim_in(' hello world ', ' ehlowrd')"
    check_three_ways "trim_in(' hello world ', '')"
    check_three_ways "ltrim_in(' hello world ', '')"
    check_three_ways "rtrim_in(' hello world ', '')"
    check_three_ways "trim_in(' hello world ', ' ')"
    check_three_ways "ltrim_in(' hello world ', ' ')"
    check_three_ways "rtrim_in(' hello world ', ' ')"

    order_qt_1 "SELECT ltrim_in('');"
    order_qt_2 "SELECT ltrim_in('   ');"
    order_qt_3 "SELECT ltrim_in('  hello  ');"
    order_qt_4 "SELECT ltrim_in('  hello');"
    order_qt_5 "SELECT ltrim_in('hello  ');"
    order_qt_6 "SELECT ltrim_in(' hello world ');"
    order_qt_7 "SELECT ltrim_in(CAST('' AS CHAR(20)));"
    order_qt_8 "SELECT ltrim_in(CAST('  hello  ' AS CHAR(9)));"
    order_qt_9 "SELECT ltrim_in(CAST('  hello' AS CHAR(7)));"
    order_qt_10 "SELECT ltrim_in(CAST('hello  ' AS CHAR(7)));"
    order_qt_11 "SELECT ltrim_in(CAST(' hello world ' AS CHAR(13)));"
    order_qt_12 "SELECT rtrim_in('');"
    order_qt_13 "SELECT rtrim_in('   ');"
    order_qt_14 "SELECT rtrim_in('  hello  ');"
    order_qt_15 "SELECT rtrim_in('  hello');"
    order_qt_16 "SELECT rtrim_in('hello  ');"
    order_qt_17 "SELECT rtrim_in(' hello world ');"
    order_qt_18 "SELECT rtrim_in(CAST('' AS CHAR(20)));"
    order_qt_19 "SELECT rtrim_in(CAST('  hello  ' AS CHAR(9)));"
    order_qt_20 "SELECT rtrim_in(CAST('  hello' AS CHAR(7)));"
    order_qt_21 "SELECT rtrim_in(CAST('hello  ' AS CHAR(7)));"
    order_qt_22 "SELECT rtrim_in(CAST(' hello world ' AS CHAR(13)));"
    order_qt_23 "SELECT ltrim_in('', '');"
    order_qt_24 "SELECT ltrim_in('   ', '');"
    order_qt_25 "SELECT ltrim_in('  hello  ', '');"
    order_qt_26 "SELECT ltrim_in('  hello  ', ' ');"
    order_qt_27 "SELECT ltrim_in('  hello  ', ' ');"
    order_qt_28 "SELECT ltrim_in('  hello  ', 'he ');"
    order_qt_29 "SELECT ltrim_in('  hello', ' ');"
    order_qt_30 "SELECT ltrim_in('  hello', 'e h');"
    order_qt_31 "SELECT ltrim_in('hello  ', 'l');"
    order_qt_32 "SELECT ltrim_in(' hello world ', ' ');"
    order_qt_33 "SELECT ltrim_in(' hello world ', ' eh');"
    order_qt_34 "SELECT ltrim_in(' hello world ', ' ehlowrd');"
    order_qt_35 "SELECT ltrim_in(' hello world ', ' x');"
    order_qt_36 "SELECT ltrim_in(CAST('' AS CHAR(1)), '');"
    order_qt_37 "SELECT ltrim_in(CAST('   ' AS CHAR(3)), '');"
    order_qt_38 "SELECT ltrim_in(CAST('  hello  ' AS CHAR(9)), '');"
    order_qt_39 "SELECT ltrim_in(CAST('  hello  ' AS CHAR(9)), ' ');"
    order_qt_40 "SELECT ltrim_in(CAST('  hello  ' AS CHAR(9)), 'he ');"
    order_qt_41 "SELECT ltrim_in(CAST('  hello' AS CHAR(7)), ' ');"
    order_qt_42 "SELECT ltrim_in(CAST('  hello' AS CHAR(7)), 'e h');"
    order_qt_43 "SELECT ltrim_in(CAST('hello  ' AS CHAR(7)), 'l');"
    order_qt_44 "SELECT ltrim_in(CAST(' hello world ' AS CHAR(13)), ' ');"
    order_qt_45 "SELECT ltrim_in(CAST(' hello world ' AS CHAR(13)), ' eh');"
    order_qt_46 "SELECT ltrim_in(CAST(' hello world ' AS CHAR(13)), ' ehlowrd');"
    order_qt_47 "SELECT ltrim_in(CAST(' hello world ' AS CHAR(13)), ' x');"
    order_qt_48 "SELECT rtrim_in('', '');"
    order_qt_49 "SELECT rtrim_in('   ', '');"
    order_qt_50 "SELECT rtrim_in('  hello  ', '');"
    order_qt_51 "SELECT rtrim_in('  hello  ', ' ');"
    order_qt_52 "SELECT rtrim_in('  hello  ', 'lo ');"
    order_qt_53 "SELECT rtrim_in('hello  ', ' ');"
    order_qt_54 "SELECT rtrim_in('hello  ', 'l o');"
    order_qt_55 "SELECT rtrim_in('hello  ', 'l');"
    order_qt_56 "SELECT rtrim_in(' hello world ', ' ');"
    order_qt_57 "SELECT rtrim_in(' hello world ', ' ld');"
    order_qt_58 "SELECT rtrim_in(' hello world ', ' ehlowrd');"
    order_qt_59 "SELECT rtrim_in(' hello world ', ' x');"
    order_qt_60 "SELECT rtrim_in(CAST('abc def' AS CHAR(7)), 'def');"
    order_qt_61 "SELECT rtrim_in(CAST('' AS CHAR(1)), '');"
    order_qt_62 "SELECT rtrim_in(CAST('   ' AS CHAR(3)), '');"
    order_qt_63 "SELECT rtrim_in(CAST('  hello  ' AS CHAR(9)), '');"
    order_qt_64 "SELECT rtrim_in(CAST('  hello  ' AS CHAR(9)), ' ');"
    order_qt_65 "SELECT rtrim_in(CAST('  hello  ' AS CHAR(9)), 'he ');"
    order_qt_66 "SELECT rtrim_in(CAST('  hello' AS CHAR(7)), ' ');"
    order_qt_67 "SELECT rtrim_in(CAST('  hello' AS CHAR(7)), 'e h');"
    order_qt_68 "SELECT rtrim_in(CAST('hello  ' AS CHAR(7)), 'l');"
    order_qt_69 "SELECT rtrim_in(CAST(' hello world ' AS CHAR(13)), ' ');"
    order_qt_70 "SELECT rtrim_in(CAST(' hello world ' AS CHAR(13)), ' eh');"
    order_qt_71 "SELECT rtrim_in(CAST(' hello world ' AS CHAR(13)), ' ehlowrd');"
    order_qt_72 "SELECT rtrim_in(CAST(' hello world ' AS CHAR(13)), ' x');"

    order_qt_73 "SELECT trim_in(a, ' eh') from test_trim_in;"
    order_qt_74 "SELECT ltrim_in(a, ' eh') from test_trim_in;"
    order_qt_75 "SELECT rtrim_in(a, ' eh') from test_trim_in;"
    order_qt_76 "SELECT trim_in(b, ' eh') from test_trim_in;"
    order_qt_77 "SELECT ltrim_in(b, ' eh') from test_trim_in;"
    order_qt_78 "SELECT rtrim_in(b, ' eh') from test_trim_in;"
    order_qt_79 "SELECT trim_in(a, NULL) from test_trim_in;"
    order_qt_80 "SELECT ltrim_in(a, NULL) from test_trim_in;"
    order_qt_81 "SELECT rtrim_in(a, NULL) from test_trim_in;"
    order_qt_82 "SELECT trim_in(b, NULL) from test_trim_in;"
    order_qt_83 "SELECT ltrim_in(b, NULL) from test_trim_in;"
    order_qt_84 "SELECT rtrim_in(b, NULL) from test_trim_in;"
    order_qt_85 "SELECT trim_in(a, '省市杭州') from test_trim_in;"
    order_qt_86 "SELECT ltrim_in(a, '省市杭州') from test_trim_in;"
    order_qt_87 "SELECT rtrim_in(a, '省市杭州') from test_trim_in;"
    order_qt_88 "SELECT trim_in(b, '杭余') from test_trim_in;"
    order_qt_89 "SELECT ltrim_in(b, '杭余') from test_trim_in;"
    order_qt_90 "SELECT rtrim_in(b, '杭余') from test_trim_in;"
    order_qt_91 "SELECT trim_in(a, '省市a杭州') from test_trim_in;"
    order_qt_92 "SELECT ltrim_in(a, '省市b杭州') from test_trim_in;"
    order_qt_93 "SELECT rtrim_in(a, '省市c杭州') from test_trim_in;"
}
