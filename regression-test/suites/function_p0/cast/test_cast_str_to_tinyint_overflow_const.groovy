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


suite("test_cast_str_to_tinyint_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    def test_cast_str_to_tinyint_overflow_strs = ["""128""","""-129""","""129""","""-130""","""130""","""-131""","""131""","""-132""","""132""","""-133""","""133""","""-134""","""134""","""-135""","""135""","""-136""","""136""","""-137""","""137""","""-138""",
        """255""","""-255""","""254""","""-254""","""253""","""-253""","""252""","""-252""","""251""","""-251""","""250""","""-250""","""249""","""-249""","""248""","""-248""","""247""","""-247""","""246""","""-246""",
        """32767""","""-32768""","""2147483647""","""-2147483648""","""9223372036854775807""","""-9223372036854775808""","""170141183460469231731687303715884105727""","""-170141183460469231731687303715884105728""","""57896044618658097711785492504343953926634992332820282019728792003956564819967""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968"""]
    sql "set enable_strict_cast=true;"
    for (test_str in test_cast_str_to_tinyint_overflow_strs) {
        test {
            sql """select cast("${test_str}" as tinyint);"""
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict """select "128", cast("128" as tinyint);"""
    qt_sql_1_non_strict """select "-129", cast("-129" as tinyint);"""
    qt_sql_2_non_strict """select "129", cast("129" as tinyint);"""
    qt_sql_3_non_strict """select "-130", cast("-130" as tinyint);"""
    qt_sql_4_non_strict """select "130", cast("130" as tinyint);"""
    qt_sql_5_non_strict """select "-131", cast("-131" as tinyint);"""
    qt_sql_6_non_strict """select "131", cast("131" as tinyint);"""
    qt_sql_7_non_strict """select "-132", cast("-132" as tinyint);"""
    qt_sql_8_non_strict """select "132", cast("132" as tinyint);"""
    qt_sql_9_non_strict """select "-133", cast("-133" as tinyint);"""
    qt_sql_10_non_strict """select "133", cast("133" as tinyint);"""
    qt_sql_11_non_strict """select "-134", cast("-134" as tinyint);"""
    qt_sql_12_non_strict """select "134", cast("134" as tinyint);"""
    qt_sql_13_non_strict """select "-135", cast("-135" as tinyint);"""
    qt_sql_14_non_strict """select "135", cast("135" as tinyint);"""
    qt_sql_15_non_strict """select "-136", cast("-136" as tinyint);"""
    qt_sql_16_non_strict """select "136", cast("136" as tinyint);"""
    qt_sql_17_non_strict """select "-137", cast("-137" as tinyint);"""
    qt_sql_18_non_strict """select "137", cast("137" as tinyint);"""
    qt_sql_19_non_strict """select "-138", cast("-138" as tinyint);"""
    qt_sql_20_non_strict """select "255", cast("255" as tinyint);"""
    qt_sql_21_non_strict """select "-255", cast("-255" as tinyint);"""
    qt_sql_22_non_strict """select "254", cast("254" as tinyint);"""
    qt_sql_23_non_strict """select "-254", cast("-254" as tinyint);"""
    qt_sql_24_non_strict """select "253", cast("253" as tinyint);"""
    qt_sql_25_non_strict """select "-253", cast("-253" as tinyint);"""
    qt_sql_26_non_strict """select "252", cast("252" as tinyint);"""
    qt_sql_27_non_strict """select "-252", cast("-252" as tinyint);"""
    qt_sql_28_non_strict """select "251", cast("251" as tinyint);"""
    qt_sql_29_non_strict """select "-251", cast("-251" as tinyint);"""
    qt_sql_30_non_strict """select "250", cast("250" as tinyint);"""
    qt_sql_31_non_strict """select "-250", cast("-250" as tinyint);"""
    qt_sql_32_non_strict """select "249", cast("249" as tinyint);"""
    qt_sql_33_non_strict """select "-249", cast("-249" as tinyint);"""
    qt_sql_34_non_strict """select "248", cast("248" as tinyint);"""
    qt_sql_35_non_strict """select "-248", cast("-248" as tinyint);"""
    qt_sql_36_non_strict """select "247", cast("247" as tinyint);"""
    qt_sql_37_non_strict """select "-247", cast("-247" as tinyint);"""
    qt_sql_38_non_strict """select "246", cast("246" as tinyint);"""
    qt_sql_39_non_strict """select "-246", cast("-246" as tinyint);"""
    qt_sql_40_non_strict """select "32767", cast("32767" as tinyint);"""
    qt_sql_41_non_strict """select "-32768", cast("-32768" as tinyint);"""
    qt_sql_42_non_strict """select "2147483647", cast("2147483647" as tinyint);"""
    qt_sql_43_non_strict """select "-2147483648", cast("-2147483648" as tinyint);"""
    qt_sql_44_non_strict """select "9223372036854775807", cast("9223372036854775807" as tinyint);"""
    qt_sql_45_non_strict """select "-9223372036854775808", cast("-9223372036854775808" as tinyint);"""
    qt_sql_46_non_strict """select "170141183460469231731687303715884105727", cast("170141183460469231731687303715884105727" as tinyint);"""
    qt_sql_47_non_strict """select "-170141183460469231731687303715884105728", cast("-170141183460469231731687303715884105728" as tinyint);"""
    qt_sql_48_non_strict """select "57896044618658097711785492504343953926634992332820282019728792003956564819967", cast("57896044618658097711785492504343953926634992332820282019728792003956564819967" as tinyint);"""
    qt_sql_49_non_strict """select "-57896044618658097711785492504343953926634992332820282019728792003956564819968", cast("-57896044618658097711785492504343953926634992332820282019728792003956564819968" as tinyint);"""
}