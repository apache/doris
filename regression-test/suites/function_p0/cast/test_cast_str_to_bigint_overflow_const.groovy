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


suite("test_cast_str_to_bigint_overflow_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.test_from_string --gen_regression_case
    def test_cast_str_to_bigint_overflow_strs = ["""9223372036854775808""","""-9223372036854775809""","""9223372036854775809""","""-9223372036854775810""","""9223372036854775810""","""-9223372036854775811""","""9223372036854775811""","""-9223372036854775812""","""9223372036854775812""","""-9223372036854775813""","""9223372036854775813""","""-9223372036854775814""","""9223372036854775814""","""-9223372036854775815""","""9223372036854775815""","""-9223372036854775816""","""9223372036854775816""","""-9223372036854775817""","""9223372036854775817""","""-9223372036854775818""",
        """18446744073709551615""","""-18446744073709551615""","""18446744073709551614""","""-18446744073709551614""","""18446744073709551613""","""-18446744073709551613""","""18446744073709551612""","""-18446744073709551612""","""18446744073709551611""","""-18446744073709551611""","""18446744073709551610""","""-18446744073709551610""","""18446744073709551609""","""-18446744073709551609""","""18446744073709551608""","""-18446744073709551608""","""18446744073709551607""","""-18446744073709551607""","""18446744073709551606""","""-18446744073709551606""",
        """170141183460469231731687303715884105727""","""-170141183460469231731687303715884105728""","""57896044618658097711785492504343953926634992332820282019728792003956564819967""","""-57896044618658097711785492504343953926634992332820282019728792003956564819968"""]
    sql "set enable_strict_cast=true;"
    for (test_str in test_cast_str_to_bigint_overflow_strs) {
        test {
            sql """select cast("${test_str}" as bigint);"""
            exception ""
        }
    }
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict """select "9223372036854775808", cast("9223372036854775808" as bigint);"""
    qt_sql_1_non_strict """select "-9223372036854775809", cast("-9223372036854775809" as bigint);"""
    qt_sql_2_non_strict """select "9223372036854775809", cast("9223372036854775809" as bigint);"""
    qt_sql_3_non_strict """select "-9223372036854775810", cast("-9223372036854775810" as bigint);"""
    qt_sql_4_non_strict """select "9223372036854775810", cast("9223372036854775810" as bigint);"""
    qt_sql_5_non_strict """select "-9223372036854775811", cast("-9223372036854775811" as bigint);"""
    qt_sql_6_non_strict """select "9223372036854775811", cast("9223372036854775811" as bigint);"""
    qt_sql_7_non_strict """select "-9223372036854775812", cast("-9223372036854775812" as bigint);"""
    qt_sql_8_non_strict """select "9223372036854775812", cast("9223372036854775812" as bigint);"""
    qt_sql_9_non_strict """select "-9223372036854775813", cast("-9223372036854775813" as bigint);"""
    qt_sql_10_non_strict """select "9223372036854775813", cast("9223372036854775813" as bigint);"""
    qt_sql_11_non_strict """select "-9223372036854775814", cast("-9223372036854775814" as bigint);"""
    qt_sql_12_non_strict """select "9223372036854775814", cast("9223372036854775814" as bigint);"""
    qt_sql_13_non_strict """select "-9223372036854775815", cast("-9223372036854775815" as bigint);"""
    qt_sql_14_non_strict """select "9223372036854775815", cast("9223372036854775815" as bigint);"""
    qt_sql_15_non_strict """select "-9223372036854775816", cast("-9223372036854775816" as bigint);"""
    qt_sql_16_non_strict """select "9223372036854775816", cast("9223372036854775816" as bigint);"""
    qt_sql_17_non_strict """select "-9223372036854775817", cast("-9223372036854775817" as bigint);"""
    qt_sql_18_non_strict """select "9223372036854775817", cast("9223372036854775817" as bigint);"""
    qt_sql_19_non_strict """select "-9223372036854775818", cast("-9223372036854775818" as bigint);"""
    qt_sql_20_non_strict """select "18446744073709551615", cast("18446744073709551615" as bigint);"""
    qt_sql_21_non_strict """select "-18446744073709551615", cast("-18446744073709551615" as bigint);"""
    qt_sql_22_non_strict """select "18446744073709551614", cast("18446744073709551614" as bigint);"""
    qt_sql_23_non_strict """select "-18446744073709551614", cast("-18446744073709551614" as bigint);"""
    qt_sql_24_non_strict """select "18446744073709551613", cast("18446744073709551613" as bigint);"""
    qt_sql_25_non_strict """select "-18446744073709551613", cast("-18446744073709551613" as bigint);"""
    qt_sql_26_non_strict """select "18446744073709551612", cast("18446744073709551612" as bigint);"""
    qt_sql_27_non_strict """select "-18446744073709551612", cast("-18446744073709551612" as bigint);"""
    qt_sql_28_non_strict """select "18446744073709551611", cast("18446744073709551611" as bigint);"""
    qt_sql_29_non_strict """select "-18446744073709551611", cast("-18446744073709551611" as bigint);"""
    qt_sql_30_non_strict """select "18446744073709551610", cast("18446744073709551610" as bigint);"""
    qt_sql_31_non_strict """select "-18446744073709551610", cast("-18446744073709551610" as bigint);"""
    qt_sql_32_non_strict """select "18446744073709551609", cast("18446744073709551609" as bigint);"""
    qt_sql_33_non_strict """select "-18446744073709551609", cast("-18446744073709551609" as bigint);"""
    qt_sql_34_non_strict """select "18446744073709551608", cast("18446744073709551608" as bigint);"""
    qt_sql_35_non_strict """select "-18446744073709551608", cast("-18446744073709551608" as bigint);"""
    qt_sql_36_non_strict """select "18446744073709551607", cast("18446744073709551607" as bigint);"""
    qt_sql_37_non_strict """select "-18446744073709551607", cast("-18446744073709551607" as bigint);"""
    qt_sql_38_non_strict """select "18446744073709551606", cast("18446744073709551606" as bigint);"""
    qt_sql_39_non_strict """select "-18446744073709551606", cast("-18446744073709551606" as bigint);"""
    qt_sql_40_non_strict """select "170141183460469231731687303715884105727", cast("170141183460469231731687303715884105727" as bigint);"""
    qt_sql_41_non_strict """select "-170141183460469231731687303715884105728", cast("-170141183460469231731687303715884105728" as bigint);"""
    qt_sql_42_non_strict """select "57896044618658097711785492504343953926634992332820282019728792003956564819967", cast("57896044618658097711785492504343953926634992332820282019728792003956564819967" as bigint);"""
    qt_sql_43_non_strict """select "-57896044618658097711785492504343953926634992332820282019728792003956564819968", cast("-57896044618658097711785492504343953926634992332820282019728792003956564819968" as bigint);"""
}