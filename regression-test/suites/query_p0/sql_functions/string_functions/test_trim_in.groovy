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
    /// consts. most by BE-UT
    order_qt_const_nullable "select trim_in(NULL,NULL) from test_trim_in"
    order_qt_partial_const_nullable "select trim_in(NULL, b) from test_trim_in"
    order_qt_const_not_nullable "select trim_in(a,b) from test_trim_in"
    order_qt_const_other_nullable "select trim_in('x',b) from test_trim_in"
    order_qt_const_other_not_nullable "select trim_in('x',a) from test_trim_in"
    order_qt_const_partial_nullable_no_null "select trim_in('abc', 'b')"
    //[INVALID_ARGUMENT]Argument at index 1 for function trim must be constant
    //order_qt_const_nullable_no_null "select trim_in(nullable('abc'),nullable('中文'))"
    //order_qt_const_partial_nullable_no_null "select trim_in('abc',nullable('a'))"
    //order_qt_const_nullable_no_null_multirows "select trim_in(nullable('abc'), nullable('a'))"



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
}
