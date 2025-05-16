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


suite("test_cast_str_to_smallint_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    def test_cast_str_to_smallint_overflow_strs = ["""32768""","""-32769""","""32769""","""-32770""","""32770""","""-32771""","""32771""","""-32772""","""32772""","""-32773""","""32773""","""-32774""","""32774""","""-32775""","""32775""","""-32776""","""32776""","""-32777""","""32777""","""-32778""",
        """65535""","""-65535""","""65534""","""-65534""","""65533""","""-65533""","""65532""","""-65532""","""65531""","""-65531""","""65530""","""-65530""","""65529""","""-65529""","""65528""","""-65528""","""65527""","""-65527""","""65526""","""-65526""",
        """2147483647""","""-2147483648""","""9223372036854775807""","""-9223372036854775808""","""170141183460469231731687303715884105727""","""-170141183460469231731687303715884105728""","""57896044618658097711785492504343953926634992332820282019728792003956564819967""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968"""]
    sql "set enable_strict_cast=true;"
    for (test_str in test_cast_str_to_smallint_overflow_strs) {
        test {
            sql """select cast("${test_str}" as smallint);"""
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict """select "32768", cast("32768" as smallint);"""
    qt_sql_1_non_strict """select "-32769", cast("-32769" as smallint);"""
    qt_sql_2_non_strict """select "32769", cast("32769" as smallint);"""
    qt_sql_3_non_strict """select "-32770", cast("-32770" as smallint);"""
    qt_sql_4_non_strict """select "32770", cast("32770" as smallint);"""
    qt_sql_5_non_strict """select "-32771", cast("-32771" as smallint);"""
    qt_sql_6_non_strict """select "32771", cast("32771" as smallint);"""
    qt_sql_7_non_strict """select "-32772", cast("-32772" as smallint);"""
    qt_sql_8_non_strict """select "32772", cast("32772" as smallint);"""
    qt_sql_9_non_strict """select "-32773", cast("-32773" as smallint);"""
    qt_sql_10_non_strict """select "32773", cast("32773" as smallint);"""
    qt_sql_11_non_strict """select "-32774", cast("-32774" as smallint);"""
    qt_sql_12_non_strict """select "32774", cast("32774" as smallint);"""
    qt_sql_13_non_strict """select "-32775", cast("-32775" as smallint);"""
    qt_sql_14_non_strict """select "32775", cast("32775" as smallint);"""
    qt_sql_15_non_strict """select "-32776", cast("-32776" as smallint);"""
    qt_sql_16_non_strict """select "32776", cast("32776" as smallint);"""
    qt_sql_17_non_strict """select "-32777", cast("-32777" as smallint);"""
    qt_sql_18_non_strict """select "32777", cast("32777" as smallint);"""
    qt_sql_19_non_strict """select "-32778", cast("-32778" as smallint);"""
    qt_sql_20_non_strict """select "65535", cast("65535" as smallint);"""
    qt_sql_21_non_strict """select "-65535", cast("-65535" as smallint);"""
    qt_sql_22_non_strict """select "65534", cast("65534" as smallint);"""
    qt_sql_23_non_strict """select "-65534", cast("-65534" as smallint);"""
    qt_sql_24_non_strict """select "65533", cast("65533" as smallint);"""
    qt_sql_25_non_strict """select "-65533", cast("-65533" as smallint);"""
    qt_sql_26_non_strict """select "65532", cast("65532" as smallint);"""
    qt_sql_27_non_strict """select "-65532", cast("-65532" as smallint);"""
    qt_sql_28_non_strict """select "65531", cast("65531" as smallint);"""
    qt_sql_29_non_strict """select "-65531", cast("-65531" as smallint);"""
    qt_sql_30_non_strict """select "65530", cast("65530" as smallint);"""
    qt_sql_31_non_strict """select "-65530", cast("-65530" as smallint);"""
    qt_sql_32_non_strict """select "65529", cast("65529" as smallint);"""
    qt_sql_33_non_strict """select "-65529", cast("-65529" as smallint);"""
    qt_sql_34_non_strict """select "65528", cast("65528" as smallint);"""
    qt_sql_35_non_strict """select "-65528", cast("-65528" as smallint);"""
    qt_sql_36_non_strict """select "65527", cast("65527" as smallint);"""
    qt_sql_37_non_strict """select "-65527", cast("-65527" as smallint);"""
    qt_sql_38_non_strict """select "65526", cast("65526" as smallint);"""
    qt_sql_39_non_strict """select "-65526", cast("-65526" as smallint);"""
    qt_sql_40_non_strict """select "2147483647", cast("2147483647" as smallint);"""
    qt_sql_41_non_strict """select "-2147483648", cast("-2147483648" as smallint);"""
    qt_sql_42_non_strict """select "9223372036854775807", cast("9223372036854775807" as smallint);"""
    qt_sql_43_non_strict """select "-9223372036854775808", cast("-9223372036854775808" as smallint);"""
    qt_sql_44_non_strict """select "170141183460469231731687303715884105727", cast("170141183460469231731687303715884105727" as smallint);"""
    qt_sql_45_non_strict """select "-170141183460469231731687303715884105728", cast("-170141183460469231731687303715884105728" as smallint);"""
    qt_sql_46_non_strict """select "57896044618658097711785492504343953926634992332820282019728792003956564819967", cast("57896044618658097711785492504343953926634992332820282019728792003956564819967" as smallint);"""
    qt_sql_47_non_strict """select "-57896044618658097711785492504343953926634992332820282019728792003956564819968", cast("-57896044618658097711785492504343953926634992332820282019728792003956564819968" as smallint);"""
}