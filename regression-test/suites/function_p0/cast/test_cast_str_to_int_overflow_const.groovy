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


suite("test_cast_str_to_int_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    def test_cast_str_to_int_overflow_strs = ["""2147483648""","""-2147483649""","""2147483649""","""-2147483650""","""2147483650""","""-2147483651""","""2147483651""","""-2147483652""","""2147483652""","""-2147483653""","""2147483653""","""-2147483654""","""2147483654""","""-2147483655""","""2147483655""","""-2147483656""","""2147483656""","""-2147483657""","""2147483657""","""-2147483658""",
        """4294967295""","""-4294967295""","""4294967294""","""-4294967294""","""4294967293""","""-4294967293""","""4294967292""","""-4294967292""","""4294967291""","""-4294967291""","""4294967290""","""-4294967290""","""4294967289""","""-4294967289""","""4294967288""","""-4294967288""","""4294967287""","""-4294967287""","""4294967286""","""-4294967286""",
        """9223372036854775807""","""-9223372036854775808""","""170141183460469231731687303715884105727""","""-170141183460469231731687303715884105728""","""57896044618658097711785492504343953926634992332820282019728792003956564819967""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968"""]
    sql "set enable_strict_cast=true;"
    for (test_str in test_cast_str_to_int_overflow_strs) {
        test {
            sql """select cast("${test_str}" as int);"""
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict """select "2147483648", cast("2147483648" as int);"""
    qt_sql_1_non_strict """select "-2147483649", cast("-2147483649" as int);"""
    qt_sql_2_non_strict """select "2147483649", cast("2147483649" as int);"""
    qt_sql_3_non_strict """select "-2147483650", cast("-2147483650" as int);"""
    qt_sql_4_non_strict """select "2147483650", cast("2147483650" as int);"""
    qt_sql_5_non_strict """select "-2147483651", cast("-2147483651" as int);"""
    qt_sql_6_non_strict """select "2147483651", cast("2147483651" as int);"""
    qt_sql_7_non_strict """select "-2147483652", cast("-2147483652" as int);"""
    qt_sql_8_non_strict """select "2147483652", cast("2147483652" as int);"""
    qt_sql_9_non_strict """select "-2147483653", cast("-2147483653" as int);"""
    qt_sql_10_non_strict """select "2147483653", cast("2147483653" as int);"""
    qt_sql_11_non_strict """select "-2147483654", cast("-2147483654" as int);"""
    qt_sql_12_non_strict """select "2147483654", cast("2147483654" as int);"""
    qt_sql_13_non_strict """select "-2147483655", cast("-2147483655" as int);"""
    qt_sql_14_non_strict """select "2147483655", cast("2147483655" as int);"""
    qt_sql_15_non_strict """select "-2147483656", cast("-2147483656" as int);"""
    qt_sql_16_non_strict """select "2147483656", cast("2147483656" as int);"""
    qt_sql_17_non_strict """select "-2147483657", cast("-2147483657" as int);"""
    qt_sql_18_non_strict """select "2147483657", cast("2147483657" as int);"""
    qt_sql_19_non_strict """select "-2147483658", cast("-2147483658" as int);"""
    qt_sql_20_non_strict """select "4294967295", cast("4294967295" as int);"""
    qt_sql_21_non_strict """select "-4294967295", cast("-4294967295" as int);"""
    qt_sql_22_non_strict """select "4294967294", cast("4294967294" as int);"""
    qt_sql_23_non_strict """select "-4294967294", cast("-4294967294" as int);"""
    qt_sql_24_non_strict """select "4294967293", cast("4294967293" as int);"""
    qt_sql_25_non_strict """select "-4294967293", cast("-4294967293" as int);"""
    qt_sql_26_non_strict """select "4294967292", cast("4294967292" as int);"""
    qt_sql_27_non_strict """select "-4294967292", cast("-4294967292" as int);"""
    qt_sql_28_non_strict """select "4294967291", cast("4294967291" as int);"""
    qt_sql_29_non_strict """select "-4294967291", cast("-4294967291" as int);"""
    qt_sql_30_non_strict """select "4294967290", cast("4294967290" as int);"""
    qt_sql_31_non_strict """select "-4294967290", cast("-4294967290" as int);"""
    qt_sql_32_non_strict """select "4294967289", cast("4294967289" as int);"""
    qt_sql_33_non_strict """select "-4294967289", cast("-4294967289" as int);"""
    qt_sql_34_non_strict """select "4294967288", cast("4294967288" as int);"""
    qt_sql_35_non_strict """select "-4294967288", cast("-4294967288" as int);"""
    qt_sql_36_non_strict """select "4294967287", cast("4294967287" as int);"""
    qt_sql_37_non_strict """select "-4294967287", cast("-4294967287" as int);"""
    qt_sql_38_non_strict """select "4294967286", cast("4294967286" as int);"""
    qt_sql_39_non_strict """select "-4294967286", cast("-4294967286" as int);"""
    qt_sql_40_non_strict """select "9223372036854775807", cast("9223372036854775807" as int);"""
    qt_sql_41_non_strict """select "-9223372036854775808", cast("-9223372036854775808" as int);"""
    qt_sql_42_non_strict """select "170141183460469231731687303715884105727", cast("170141183460469231731687303715884105727" as int);"""
    qt_sql_43_non_strict """select "-170141183460469231731687303715884105728", cast("-170141183460469231731687303715884105728" as int);"""
    qt_sql_44_non_strict """select "57896044618658097711785492504343953926634992332820282019728792003956564819967", cast("57896044618658097711785492504343953926634992332820282019728792003956564819967" as int);"""
    qt_sql_45_non_strict """select "-57896044618658097711785492504343953926634992332820282019728792003956564819968", cast("-57896044618658097711785492504343953926634992332820282019728792003956564819968" as int);"""
}