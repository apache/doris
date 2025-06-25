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


suite("test_cast_to_decimal64_10_9_from_decimal128i_const") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "set debug_skip_fold_constant = true;"
    sql "set enable_strict_cast=true;"
    def const_sql_0 = """select cast(cast("0" as decimalv3(19,0)) as decimalv3(10,9));"""
    qt_sql_0_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    def const_sql_1 = """select cast(cast("8" as decimalv3(19,0)) as decimalv3(10,9));"""
    qt_sql_1_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    def const_sql_2 = """select cast(cast("9" as decimalv3(19,0)) as decimalv3(10,9));"""
    qt_sql_2_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict "${const_sql_0}"
    testFoldConst("${const_sql_0}")
    qt_sql_1_non_strict "${const_sql_1}"
    testFoldConst("${const_sql_1}")
    qt_sql_2_non_strict "${const_sql_2}"
    testFoldConst("${const_sql_2}")
    sql "set enable_strict_cast=true;"
    def const_sql_3 = """select cast(cast("0.0" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_3_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
    def const_sql_4 = """select cast(cast("0.1" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_4_strict "${const_sql_4}"
    testFoldConst("${const_sql_4}")
    def const_sql_5 = """select cast(cast("0.8" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_5_strict "${const_sql_5}"
    testFoldConst("${const_sql_5}")
    def const_sql_6 = """select cast(cast("0.9" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_6_strict "${const_sql_6}"
    testFoldConst("${const_sql_6}")
    def const_sql_7 = """select cast(cast("8.0" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_7_strict "${const_sql_7}"
    testFoldConst("${const_sql_7}")
    def const_sql_8 = """select cast(cast("8.1" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_8_strict "${const_sql_8}"
    testFoldConst("${const_sql_8}")
    def const_sql_9 = """select cast(cast("8.8" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_9_strict "${const_sql_9}"
    testFoldConst("${const_sql_9}")
    def const_sql_10 = """select cast(cast("8.9" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_10_strict "${const_sql_10}"
    testFoldConst("${const_sql_10}")
    def const_sql_11 = """select cast(cast("9.0" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_11_strict "${const_sql_11}"
    testFoldConst("${const_sql_11}")
    def const_sql_12 = """select cast(cast("9.1" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_12_strict "${const_sql_12}"
    testFoldConst("${const_sql_12}")
    def const_sql_13 = """select cast(cast("9.8" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_13_strict "${const_sql_13}"
    testFoldConst("${const_sql_13}")
    def const_sql_14 = """select cast(cast("9.9" as decimalv3(19,1)) as decimalv3(10,9));"""
    qt_sql_14_strict "${const_sql_14}"
    testFoldConst("${const_sql_14}")
    sql "set enable_strict_cast=false;"
    qt_sql_3_non_strict "${const_sql_3}"
    testFoldConst("${const_sql_3}")
    qt_sql_4_non_strict "${const_sql_4}"
    testFoldConst("${const_sql_4}")
    qt_sql_5_non_strict "${const_sql_5}"
    testFoldConst("${const_sql_5}")
    qt_sql_6_non_strict "${const_sql_6}"
    testFoldConst("${const_sql_6}")
    qt_sql_7_non_strict "${const_sql_7}"
    testFoldConst("${const_sql_7}")
    qt_sql_8_non_strict "${const_sql_8}"
    testFoldConst("${const_sql_8}")
    qt_sql_9_non_strict "${const_sql_9}"
    testFoldConst("${const_sql_9}")
    qt_sql_10_non_strict "${const_sql_10}"
    testFoldConst("${const_sql_10}")
    qt_sql_11_non_strict "${const_sql_11}"
    testFoldConst("${const_sql_11}")
    qt_sql_12_non_strict "${const_sql_12}"
    testFoldConst("${const_sql_12}")
    qt_sql_13_non_strict "${const_sql_13}"
    testFoldConst("${const_sql_13}")
    qt_sql_14_non_strict "${const_sql_14}"
    testFoldConst("${const_sql_14}")
    sql "set enable_strict_cast=true;"
    def const_sql_15 = """select cast(cast("0.000000000" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_15_strict "${const_sql_15}"
    testFoldConst("${const_sql_15}")
    def const_sql_16 = """select cast(cast("0.000000001" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_16_strict "${const_sql_16}"
    testFoldConst("${const_sql_16}")
    def const_sql_17 = """select cast(cast("0.000000009" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_17_strict "${const_sql_17}"
    testFoldConst("${const_sql_17}")
    def const_sql_18 = """select cast(cast("0.099999999" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_18_strict "${const_sql_18}"
    testFoldConst("${const_sql_18}")
    def const_sql_19 = """select cast(cast("0.900000000" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_19_strict "${const_sql_19}"
    testFoldConst("${const_sql_19}")
    def const_sql_20 = """select cast(cast("0.900000001" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_20_strict "${const_sql_20}"
    testFoldConst("${const_sql_20}")
    def const_sql_21 = """select cast(cast("0.999999998" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_21_strict "${const_sql_21}"
    testFoldConst("${const_sql_21}")
    def const_sql_22 = """select cast(cast("0.999999999" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_22_strict "${const_sql_22}"
    testFoldConst("${const_sql_22}")
    def const_sql_23 = """select cast(cast("8.000000000" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_23_strict "${const_sql_23}"
    testFoldConst("${const_sql_23}")
    def const_sql_24 = """select cast(cast("8.000000001" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_24_strict "${const_sql_24}"
    testFoldConst("${const_sql_24}")
    def const_sql_25 = """select cast(cast("8.000000009" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_25_strict "${const_sql_25}"
    testFoldConst("${const_sql_25}")
    def const_sql_26 = """select cast(cast("8.099999999" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_26_strict "${const_sql_26}"
    testFoldConst("${const_sql_26}")
    def const_sql_27 = """select cast(cast("8.900000000" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_27_strict "${const_sql_27}"
    testFoldConst("${const_sql_27}")
    def const_sql_28 = """select cast(cast("8.900000001" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_28_strict "${const_sql_28}"
    testFoldConst("${const_sql_28}")
    def const_sql_29 = """select cast(cast("8.999999998" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_29_strict "${const_sql_29}"
    testFoldConst("${const_sql_29}")
    def const_sql_30 = """select cast(cast("8.999999999" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_30_strict "${const_sql_30}"
    testFoldConst("${const_sql_30}")
    def const_sql_31 = """select cast(cast("9.000000000" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_31_strict "${const_sql_31}"
    testFoldConst("${const_sql_31}")
    def const_sql_32 = """select cast(cast("9.000000001" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_32_strict "${const_sql_32}"
    testFoldConst("${const_sql_32}")
    def const_sql_33 = """select cast(cast("9.000000009" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_33_strict "${const_sql_33}"
    testFoldConst("${const_sql_33}")
    def const_sql_34 = """select cast(cast("9.099999999" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_34_strict "${const_sql_34}"
    testFoldConst("${const_sql_34}")
    def const_sql_35 = """select cast(cast("9.900000000" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_35_strict "${const_sql_35}"
    testFoldConst("${const_sql_35}")
    def const_sql_36 = """select cast(cast("9.900000001" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_36_strict "${const_sql_36}"
    testFoldConst("${const_sql_36}")
    def const_sql_37 = """select cast(cast("9.999999998" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_37_strict "${const_sql_37}"
    testFoldConst("${const_sql_37}")
    def const_sql_38 = """select cast(cast("9.999999999" as decimalv3(19,9)) as decimalv3(10,9));"""
    qt_sql_38_strict "${const_sql_38}"
    testFoldConst("${const_sql_38}")
    sql "set enable_strict_cast=false;"
    qt_sql_15_non_strict "${const_sql_15}"
    testFoldConst("${const_sql_15}")
    qt_sql_16_non_strict "${const_sql_16}"
    testFoldConst("${const_sql_16}")
    qt_sql_17_non_strict "${const_sql_17}"
    testFoldConst("${const_sql_17}")
    qt_sql_18_non_strict "${const_sql_18}"
    testFoldConst("${const_sql_18}")
    qt_sql_19_non_strict "${const_sql_19}"
    testFoldConst("${const_sql_19}")
    qt_sql_20_non_strict "${const_sql_20}"
    testFoldConst("${const_sql_20}")
    qt_sql_21_non_strict "${const_sql_21}"
    testFoldConst("${const_sql_21}")
    qt_sql_22_non_strict "${const_sql_22}"
    testFoldConst("${const_sql_22}")
    qt_sql_23_non_strict "${const_sql_23}"
    testFoldConst("${const_sql_23}")
    qt_sql_24_non_strict "${const_sql_24}"
    testFoldConst("${const_sql_24}")
    qt_sql_25_non_strict "${const_sql_25}"
    testFoldConst("${const_sql_25}")
    qt_sql_26_non_strict "${const_sql_26}"
    testFoldConst("${const_sql_26}")
    qt_sql_27_non_strict "${const_sql_27}"
    testFoldConst("${const_sql_27}")
    qt_sql_28_non_strict "${const_sql_28}"
    testFoldConst("${const_sql_28}")
    qt_sql_29_non_strict "${const_sql_29}"
    testFoldConst("${const_sql_29}")
    qt_sql_30_non_strict "${const_sql_30}"
    testFoldConst("${const_sql_30}")
    qt_sql_31_non_strict "${const_sql_31}"
    testFoldConst("${const_sql_31}")
    qt_sql_32_non_strict "${const_sql_32}"
    testFoldConst("${const_sql_32}")
    qt_sql_33_non_strict "${const_sql_33}"
    testFoldConst("${const_sql_33}")
    qt_sql_34_non_strict "${const_sql_34}"
    testFoldConst("${const_sql_34}")
    qt_sql_35_non_strict "${const_sql_35}"
    testFoldConst("${const_sql_35}")
    qt_sql_36_non_strict "${const_sql_36}"
    testFoldConst("${const_sql_36}")
    qt_sql_37_non_strict "${const_sql_37}"
    testFoldConst("${const_sql_37}")
    qt_sql_38_non_strict "${const_sql_38}"
    testFoldConst("${const_sql_38}")
    sql "set enable_strict_cast=true;"
    def const_sql_39 = """select cast(cast("0.000000000000000000" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_39_strict "${const_sql_39}"
    testFoldConst("${const_sql_39}")
    def const_sql_40 = """select cast(cast("0.000000000000000001" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_40_strict "${const_sql_40}"
    testFoldConst("${const_sql_40}")
    def const_sql_41 = """select cast(cast("0.000000000000000009" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_41_strict "${const_sql_41}"
    testFoldConst("${const_sql_41}")
    def const_sql_42 = """select cast(cast("0.099999999999999999" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_42_strict "${const_sql_42}"
    testFoldConst("${const_sql_42}")
    def const_sql_43 = """select cast(cast("0.900000000000000000" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_43_strict "${const_sql_43}"
    testFoldConst("${const_sql_43}")
    def const_sql_44 = """select cast(cast("0.900000000000000001" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_44_strict "${const_sql_44}"
    testFoldConst("${const_sql_44}")
    def const_sql_45 = """select cast(cast("0.999999999999999998" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_45_strict "${const_sql_45}"
    testFoldConst("${const_sql_45}")
    def const_sql_46 = """select cast(cast("0.999999999999999999" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_46_strict "${const_sql_46}"
    testFoldConst("${const_sql_46}")
    def const_sql_47 = """select cast(cast("8.000000000000000000" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_47_strict "${const_sql_47}"
    testFoldConst("${const_sql_47}")
    def const_sql_48 = """select cast(cast("8.000000000000000001" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_48_strict "${const_sql_48}"
    testFoldConst("${const_sql_48}")
    def const_sql_49 = """select cast(cast("8.000000000000000009" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_49_strict "${const_sql_49}"
    testFoldConst("${const_sql_49}")
    def const_sql_50 = """select cast(cast("8.099999999999999999" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_50_strict "${const_sql_50}"
    testFoldConst("${const_sql_50}")
    def const_sql_51 = """select cast(cast("8.900000000000000000" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_51_strict "${const_sql_51}"
    testFoldConst("${const_sql_51}")
    def const_sql_52 = """select cast(cast("8.900000000000000001" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_52_strict "${const_sql_52}"
    testFoldConst("${const_sql_52}")
    def const_sql_53 = """select cast(cast("8.999999999999999998" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_53_strict "${const_sql_53}"
    testFoldConst("${const_sql_53}")
    def const_sql_54 = """select cast(cast("8.999999999999999999" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_54_strict "${const_sql_54}"
    testFoldConst("${const_sql_54}")
    def const_sql_55 = """select cast(cast("9.000000000000000000" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_55_strict "${const_sql_55}"
    testFoldConst("${const_sql_55}")
    def const_sql_56 = """select cast(cast("9.000000000000000001" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_56_strict "${const_sql_56}"
    testFoldConst("${const_sql_56}")
    def const_sql_57 = """select cast(cast("9.000000000000000009" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_57_strict "${const_sql_57}"
    testFoldConst("${const_sql_57}")
    def const_sql_58 = """select cast(cast("9.099999999999999999" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_58_strict "${const_sql_58}"
    testFoldConst("${const_sql_58}")
    def const_sql_59 = """select cast(cast("9.900000000000000000" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_59_strict "${const_sql_59}"
    testFoldConst("${const_sql_59}")
    def const_sql_60 = """select cast(cast("9.900000000000000001" as decimalv3(19,18)) as decimalv3(10,9));"""
    qt_sql_60_strict "${const_sql_60}"
    testFoldConst("${const_sql_60}")
    sql "set enable_strict_cast=false;"
    qt_sql_39_non_strict "${const_sql_39}"
    testFoldConst("${const_sql_39}")
    qt_sql_40_non_strict "${const_sql_40}"
    testFoldConst("${const_sql_40}")
    qt_sql_41_non_strict "${const_sql_41}"
    testFoldConst("${const_sql_41}")
    qt_sql_42_non_strict "${const_sql_42}"
    testFoldConst("${const_sql_42}")
    qt_sql_43_non_strict "${const_sql_43}"
    testFoldConst("${const_sql_43}")
    qt_sql_44_non_strict "${const_sql_44}"
    testFoldConst("${const_sql_44}")
    qt_sql_45_non_strict "${const_sql_45}"
    testFoldConst("${const_sql_45}")
    qt_sql_46_non_strict "${const_sql_46}"
    testFoldConst("${const_sql_46}")
    qt_sql_47_non_strict "${const_sql_47}"
    testFoldConst("${const_sql_47}")
    qt_sql_48_non_strict "${const_sql_48}"
    testFoldConst("${const_sql_48}")
    qt_sql_49_non_strict "${const_sql_49}"
    testFoldConst("${const_sql_49}")
    qt_sql_50_non_strict "${const_sql_50}"
    testFoldConst("${const_sql_50}")
    qt_sql_51_non_strict "${const_sql_51}"
    testFoldConst("${const_sql_51}")
    qt_sql_52_non_strict "${const_sql_52}"
    testFoldConst("${const_sql_52}")
    qt_sql_53_non_strict "${const_sql_53}"
    testFoldConst("${const_sql_53}")
    qt_sql_54_non_strict "${const_sql_54}"
    testFoldConst("${const_sql_54}")
    qt_sql_55_non_strict "${const_sql_55}"
    testFoldConst("${const_sql_55}")
    qt_sql_56_non_strict "${const_sql_56}"
    testFoldConst("${const_sql_56}")
    qt_sql_57_non_strict "${const_sql_57}"
    testFoldConst("${const_sql_57}")
    qt_sql_58_non_strict "${const_sql_58}"
    testFoldConst("${const_sql_58}")
    qt_sql_59_non_strict "${const_sql_59}"
    testFoldConst("${const_sql_59}")
    qt_sql_60_non_strict "${const_sql_60}"
    testFoldConst("${const_sql_60}")
    sql "set enable_strict_cast=true;"
    def const_sql_61 = """select cast(cast("0.0000000000000000000" as decimalv3(19,19)) as decimalv3(10,9));"""
    qt_sql_61_strict "${const_sql_61}"
    testFoldConst("${const_sql_61}")
    def const_sql_62 = """select cast(cast("0.0000000000000000001" as decimalv3(19,19)) as decimalv3(10,9));"""
    qt_sql_62_strict "${const_sql_62}"
    testFoldConst("${const_sql_62}")
    def const_sql_63 = """select cast(cast("0.0000000000000000009" as decimalv3(19,19)) as decimalv3(10,9));"""
    qt_sql_63_strict "${const_sql_63}"
    testFoldConst("${const_sql_63}")
    def const_sql_64 = """select cast(cast("0.0999999999999999999" as decimalv3(19,19)) as decimalv3(10,9));"""
    qt_sql_64_strict "${const_sql_64}"
    testFoldConst("${const_sql_64}")
    def const_sql_65 = """select cast(cast("0.9000000000000000000" as decimalv3(19,19)) as decimalv3(10,9));"""
    qt_sql_65_strict "${const_sql_65}"
    testFoldConst("${const_sql_65}")
    def const_sql_66 = """select cast(cast("0.9000000000000000001" as decimalv3(19,19)) as decimalv3(10,9));"""
    qt_sql_66_strict "${const_sql_66}"
    testFoldConst("${const_sql_66}")
    def const_sql_67 = """select cast(cast("0.9999999999999999998" as decimalv3(19,19)) as decimalv3(10,9));"""
    qt_sql_67_strict "${const_sql_67}"
    testFoldConst("${const_sql_67}")
    def const_sql_68 = """select cast(cast("0.9999999999999999999" as decimalv3(19,19)) as decimalv3(10,9));"""
    qt_sql_68_strict "${const_sql_68}"
    testFoldConst("${const_sql_68}")
    sql "set enable_strict_cast=false;"
    qt_sql_61_non_strict "${const_sql_61}"
    testFoldConst("${const_sql_61}")
    qt_sql_62_non_strict "${const_sql_62}"
    testFoldConst("${const_sql_62}")
    qt_sql_63_non_strict "${const_sql_63}"
    testFoldConst("${const_sql_63}")
    qt_sql_64_non_strict "${const_sql_64}"
    testFoldConst("${const_sql_64}")
    qt_sql_65_non_strict "${const_sql_65}"
    testFoldConst("${const_sql_65}")
    qt_sql_66_non_strict "${const_sql_66}"
    testFoldConst("${const_sql_66}")
    qt_sql_67_non_strict "${const_sql_67}"
    testFoldConst("${const_sql_67}")
    qt_sql_68_non_strict "${const_sql_68}"
    testFoldConst("${const_sql_68}")
    sql "set enable_strict_cast=true;"
    def const_sql_69 = """select cast(cast("0" as decimalv3(37,0)) as decimalv3(10,9));"""
    qt_sql_69_strict "${const_sql_69}"
    testFoldConst("${const_sql_69}")
    def const_sql_70 = """select cast(cast("8" as decimalv3(37,0)) as decimalv3(10,9));"""
    qt_sql_70_strict "${const_sql_70}"
    testFoldConst("${const_sql_70}")
    def const_sql_71 = """select cast(cast("9" as decimalv3(37,0)) as decimalv3(10,9));"""
    qt_sql_71_strict "${const_sql_71}"
    testFoldConst("${const_sql_71}")
    sql "set enable_strict_cast=false;"
    qt_sql_69_non_strict "${const_sql_69}"
    testFoldConst("${const_sql_69}")
    qt_sql_70_non_strict "${const_sql_70}"
    testFoldConst("${const_sql_70}")
    qt_sql_71_non_strict "${const_sql_71}"
    testFoldConst("${const_sql_71}")
    sql "set enable_strict_cast=true;"
    def const_sql_72 = """select cast(cast("0.0" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_72_strict "${const_sql_72}"
    testFoldConst("${const_sql_72}")
    def const_sql_73 = """select cast(cast("0.1" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_73_strict "${const_sql_73}"
    testFoldConst("${const_sql_73}")
    def const_sql_74 = """select cast(cast("0.8" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_74_strict "${const_sql_74}"
    testFoldConst("${const_sql_74}")
    def const_sql_75 = """select cast(cast("0.9" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_75_strict "${const_sql_75}"
    testFoldConst("${const_sql_75}")
    def const_sql_76 = """select cast(cast("8.0" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_76_strict "${const_sql_76}"
    testFoldConst("${const_sql_76}")
    def const_sql_77 = """select cast(cast("8.1" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_77_strict "${const_sql_77}"
    testFoldConst("${const_sql_77}")
    def const_sql_78 = """select cast(cast("8.8" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_78_strict "${const_sql_78}"
    testFoldConst("${const_sql_78}")
    def const_sql_79 = """select cast(cast("8.9" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_79_strict "${const_sql_79}"
    testFoldConst("${const_sql_79}")
    def const_sql_80 = """select cast(cast("9.0" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_80_strict "${const_sql_80}"
    testFoldConst("${const_sql_80}")
    def const_sql_81 = """select cast(cast("9.1" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_81_strict "${const_sql_81}"
    testFoldConst("${const_sql_81}")
    def const_sql_82 = """select cast(cast("9.8" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_82_strict "${const_sql_82}"
    testFoldConst("${const_sql_82}")
    def const_sql_83 = """select cast(cast("9.9" as decimalv3(37,1)) as decimalv3(10,9));"""
    qt_sql_83_strict "${const_sql_83}"
    testFoldConst("${const_sql_83}")
    sql "set enable_strict_cast=false;"
    qt_sql_72_non_strict "${const_sql_72}"
    testFoldConst("${const_sql_72}")
    qt_sql_73_non_strict "${const_sql_73}"
    testFoldConst("${const_sql_73}")
    qt_sql_74_non_strict "${const_sql_74}"
    testFoldConst("${const_sql_74}")
    qt_sql_75_non_strict "${const_sql_75}"
    testFoldConst("${const_sql_75}")
    qt_sql_76_non_strict "${const_sql_76}"
    testFoldConst("${const_sql_76}")
    qt_sql_77_non_strict "${const_sql_77}"
    testFoldConst("${const_sql_77}")
    qt_sql_78_non_strict "${const_sql_78}"
    testFoldConst("${const_sql_78}")
    qt_sql_79_non_strict "${const_sql_79}"
    testFoldConst("${const_sql_79}")
    qt_sql_80_non_strict "${const_sql_80}"
    testFoldConst("${const_sql_80}")
    qt_sql_81_non_strict "${const_sql_81}"
    testFoldConst("${const_sql_81}")
    qt_sql_82_non_strict "${const_sql_82}"
    testFoldConst("${const_sql_82}")
    qt_sql_83_non_strict "${const_sql_83}"
    testFoldConst("${const_sql_83}")
    sql "set enable_strict_cast=true;"
    def const_sql_84 = """select cast(cast("0.000000000000000000" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_84_strict "${const_sql_84}"
    testFoldConst("${const_sql_84}")
    def const_sql_85 = """select cast(cast("0.000000000000000001" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_85_strict "${const_sql_85}"
    testFoldConst("${const_sql_85}")
    def const_sql_86 = """select cast(cast("0.000000000000000009" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_86_strict "${const_sql_86}"
    testFoldConst("${const_sql_86}")
    def const_sql_87 = """select cast(cast("0.099999999999999999" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_87_strict "${const_sql_87}"
    testFoldConst("${const_sql_87}")
    def const_sql_88 = """select cast(cast("0.900000000000000000" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_88_strict "${const_sql_88}"
    testFoldConst("${const_sql_88}")
    def const_sql_89 = """select cast(cast("0.900000000000000001" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_89_strict "${const_sql_89}"
    testFoldConst("${const_sql_89}")
    def const_sql_90 = """select cast(cast("0.999999999999999998" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_90_strict "${const_sql_90}"
    testFoldConst("${const_sql_90}")
    def const_sql_91 = """select cast(cast("0.999999999999999999" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_91_strict "${const_sql_91}"
    testFoldConst("${const_sql_91}")
    def const_sql_92 = """select cast(cast("8.000000000000000000" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_92_strict "${const_sql_92}"
    testFoldConst("${const_sql_92}")
    def const_sql_93 = """select cast(cast("8.000000000000000001" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_93_strict "${const_sql_93}"
    testFoldConst("${const_sql_93}")
    def const_sql_94 = """select cast(cast("8.000000000000000009" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_94_strict "${const_sql_94}"
    testFoldConst("${const_sql_94}")
    def const_sql_95 = """select cast(cast("8.099999999999999999" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_95_strict "${const_sql_95}"
    testFoldConst("${const_sql_95}")
    def const_sql_96 = """select cast(cast("8.900000000000000000" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_96_strict "${const_sql_96}"
    testFoldConst("${const_sql_96}")
    def const_sql_97 = """select cast(cast("8.900000000000000001" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_97_strict "${const_sql_97}"
    testFoldConst("${const_sql_97}")
    def const_sql_98 = """select cast(cast("8.999999999999999998" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_98_strict "${const_sql_98}"
    testFoldConst("${const_sql_98}")
    def const_sql_99 = """select cast(cast("8.999999999999999999" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_99_strict "${const_sql_99}"
    testFoldConst("${const_sql_99}")
    def const_sql_100 = """select cast(cast("9.000000000000000000" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_100_strict "${const_sql_100}"
    testFoldConst("${const_sql_100}")
    def const_sql_101 = """select cast(cast("9.000000000000000001" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_101_strict "${const_sql_101}"
    testFoldConst("${const_sql_101}")
    def const_sql_102 = """select cast(cast("9.000000000000000009" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_102_strict "${const_sql_102}"
    testFoldConst("${const_sql_102}")
    def const_sql_103 = """select cast(cast("9.099999999999999999" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_103_strict "${const_sql_103}"
    testFoldConst("${const_sql_103}")
    def const_sql_104 = """select cast(cast("9.900000000000000000" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_104_strict "${const_sql_104}"
    testFoldConst("${const_sql_104}")
    def const_sql_105 = """select cast(cast("9.900000000000000001" as decimalv3(37,18)) as decimalv3(10,9));"""
    qt_sql_105_strict "${const_sql_105}"
    testFoldConst("${const_sql_105}")
    sql "set enable_strict_cast=false;"
    qt_sql_84_non_strict "${const_sql_84}"
    testFoldConst("${const_sql_84}")
    qt_sql_85_non_strict "${const_sql_85}"
    testFoldConst("${const_sql_85}")
    qt_sql_86_non_strict "${const_sql_86}"
    testFoldConst("${const_sql_86}")
    qt_sql_87_non_strict "${const_sql_87}"
    testFoldConst("${const_sql_87}")
    qt_sql_88_non_strict "${const_sql_88}"
    testFoldConst("${const_sql_88}")
    qt_sql_89_non_strict "${const_sql_89}"
    testFoldConst("${const_sql_89}")
    qt_sql_90_non_strict "${const_sql_90}"
    testFoldConst("${const_sql_90}")
    qt_sql_91_non_strict "${const_sql_91}"
    testFoldConst("${const_sql_91}")
    qt_sql_92_non_strict "${const_sql_92}"
    testFoldConst("${const_sql_92}")
    qt_sql_93_non_strict "${const_sql_93}"
    testFoldConst("${const_sql_93}")
    qt_sql_94_non_strict "${const_sql_94}"
    testFoldConst("${const_sql_94}")
    qt_sql_95_non_strict "${const_sql_95}"
    testFoldConst("${const_sql_95}")
    qt_sql_96_non_strict "${const_sql_96}"
    testFoldConst("${const_sql_96}")
    qt_sql_97_non_strict "${const_sql_97}"
    testFoldConst("${const_sql_97}")
    qt_sql_98_non_strict "${const_sql_98}"
    testFoldConst("${const_sql_98}")
    qt_sql_99_non_strict "${const_sql_99}"
    testFoldConst("${const_sql_99}")
    qt_sql_100_non_strict "${const_sql_100}"
    testFoldConst("${const_sql_100}")
    qt_sql_101_non_strict "${const_sql_101}"
    testFoldConst("${const_sql_101}")
    qt_sql_102_non_strict "${const_sql_102}"
    testFoldConst("${const_sql_102}")
    qt_sql_103_non_strict "${const_sql_103}"
    testFoldConst("${const_sql_103}")
    qt_sql_104_non_strict "${const_sql_104}"
    testFoldConst("${const_sql_104}")
    qt_sql_105_non_strict "${const_sql_105}"
    testFoldConst("${const_sql_105}")
    sql "set enable_strict_cast=true;"
    def const_sql_106 = """select cast(cast("0.000000000000000000000000000000000000" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_106_strict "${const_sql_106}"
    testFoldConst("${const_sql_106}")
    def const_sql_107 = """select cast(cast("0.000000000000000000000000000000000001" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_107_strict "${const_sql_107}"
    testFoldConst("${const_sql_107}")
    def const_sql_108 = """select cast(cast("0.000000000000000000000000000000000009" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_108_strict "${const_sql_108}"
    testFoldConst("${const_sql_108}")
    def const_sql_109 = """select cast(cast("0.099999999999999999999999999999999999" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_109_strict "${const_sql_109}"
    testFoldConst("${const_sql_109}")
    def const_sql_110 = """select cast(cast("0.900000000000000000000000000000000000" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_110_strict "${const_sql_110}"
    testFoldConst("${const_sql_110}")
    def const_sql_111 = """select cast(cast("0.900000000000000000000000000000000001" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_111_strict "${const_sql_111}"
    testFoldConst("${const_sql_111}")
    def const_sql_112 = """select cast(cast("0.999999999999999999999999999999999998" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_112_strict "${const_sql_112}"
    testFoldConst("${const_sql_112}")
    def const_sql_113 = """select cast(cast("0.999999999999999999999999999999999999" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_113_strict "${const_sql_113}"
    testFoldConst("${const_sql_113}")
    def const_sql_114 = """select cast(cast("8.000000000000000000000000000000000000" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_114_strict "${const_sql_114}"
    testFoldConst("${const_sql_114}")
    def const_sql_115 = """select cast(cast("8.000000000000000000000000000000000001" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_115_strict "${const_sql_115}"
    testFoldConst("${const_sql_115}")
    def const_sql_116 = """select cast(cast("8.000000000000000000000000000000000009" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_116_strict "${const_sql_116}"
    testFoldConst("${const_sql_116}")
    def const_sql_117 = """select cast(cast("8.099999999999999999999999999999999999" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_117_strict "${const_sql_117}"
    testFoldConst("${const_sql_117}")
    def const_sql_118 = """select cast(cast("8.900000000000000000000000000000000000" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_118_strict "${const_sql_118}"
    testFoldConst("${const_sql_118}")
    def const_sql_119 = """select cast(cast("8.900000000000000000000000000000000001" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_119_strict "${const_sql_119}"
    testFoldConst("${const_sql_119}")
    def const_sql_120 = """select cast(cast("8.999999999999999999999999999999999998" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_120_strict "${const_sql_120}"
    testFoldConst("${const_sql_120}")
    def const_sql_121 = """select cast(cast("8.999999999999999999999999999999999999" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_121_strict "${const_sql_121}"
    testFoldConst("${const_sql_121}")
    def const_sql_122 = """select cast(cast("9.000000000000000000000000000000000000" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_122_strict "${const_sql_122}"
    testFoldConst("${const_sql_122}")
    def const_sql_123 = """select cast(cast("9.000000000000000000000000000000000001" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_123_strict "${const_sql_123}"
    testFoldConst("${const_sql_123}")
    def const_sql_124 = """select cast(cast("9.000000000000000000000000000000000009" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_124_strict "${const_sql_124}"
    testFoldConst("${const_sql_124}")
    def const_sql_125 = """select cast(cast("9.099999999999999999999999999999999999" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_125_strict "${const_sql_125}"
    testFoldConst("${const_sql_125}")
    def const_sql_126 = """select cast(cast("9.900000000000000000000000000000000000" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_126_strict "${const_sql_126}"
    testFoldConst("${const_sql_126}")
    def const_sql_127 = """select cast(cast("9.900000000000000000000000000000000001" as decimalv3(37,36)) as decimalv3(10,9));"""
    qt_sql_127_strict "${const_sql_127}"
    testFoldConst("${const_sql_127}")
    sql "set enable_strict_cast=false;"
    qt_sql_106_non_strict "${const_sql_106}"
    testFoldConst("${const_sql_106}")
    qt_sql_107_non_strict "${const_sql_107}"
    testFoldConst("${const_sql_107}")
    qt_sql_108_non_strict "${const_sql_108}"
    testFoldConst("${const_sql_108}")
    qt_sql_109_non_strict "${const_sql_109}"
    testFoldConst("${const_sql_109}")
    qt_sql_110_non_strict "${const_sql_110}"
    testFoldConst("${const_sql_110}")
    qt_sql_111_non_strict "${const_sql_111}"
    testFoldConst("${const_sql_111}")
    qt_sql_112_non_strict "${const_sql_112}"
    testFoldConst("${const_sql_112}")
    qt_sql_113_non_strict "${const_sql_113}"
    testFoldConst("${const_sql_113}")
    qt_sql_114_non_strict "${const_sql_114}"
    testFoldConst("${const_sql_114}")
    qt_sql_115_non_strict "${const_sql_115}"
    testFoldConst("${const_sql_115}")
    qt_sql_116_non_strict "${const_sql_116}"
    testFoldConst("${const_sql_116}")
    qt_sql_117_non_strict "${const_sql_117}"
    testFoldConst("${const_sql_117}")
    qt_sql_118_non_strict "${const_sql_118}"
    testFoldConst("${const_sql_118}")
    qt_sql_119_non_strict "${const_sql_119}"
    testFoldConst("${const_sql_119}")
    qt_sql_120_non_strict "${const_sql_120}"
    testFoldConst("${const_sql_120}")
    qt_sql_121_non_strict "${const_sql_121}"
    testFoldConst("${const_sql_121}")
    qt_sql_122_non_strict "${const_sql_122}"
    testFoldConst("${const_sql_122}")
    qt_sql_123_non_strict "${const_sql_123}"
    testFoldConst("${const_sql_123}")
    qt_sql_124_non_strict "${const_sql_124}"
    testFoldConst("${const_sql_124}")
    qt_sql_125_non_strict "${const_sql_125}"
    testFoldConst("${const_sql_125}")
    qt_sql_126_non_strict "${const_sql_126}"
    testFoldConst("${const_sql_126}")
    qt_sql_127_non_strict "${const_sql_127}"
    testFoldConst("${const_sql_127}")
    sql "set enable_strict_cast=true;"
    def const_sql_128 = """select cast(cast("0.0000000000000000000000000000000000000" as decimalv3(37,37)) as decimalv3(10,9));"""
    qt_sql_128_strict "${const_sql_128}"
    testFoldConst("${const_sql_128}")
    def const_sql_129 = """select cast(cast("0.0000000000000000000000000000000000001" as decimalv3(37,37)) as decimalv3(10,9));"""
    qt_sql_129_strict "${const_sql_129}"
    testFoldConst("${const_sql_129}")
    def const_sql_130 = """select cast(cast("0.0000000000000000000000000000000000009" as decimalv3(37,37)) as decimalv3(10,9));"""
    qt_sql_130_strict "${const_sql_130}"
    testFoldConst("${const_sql_130}")
    def const_sql_131 = """select cast(cast("0.0999999999999999999999999999999999999" as decimalv3(37,37)) as decimalv3(10,9));"""
    qt_sql_131_strict "${const_sql_131}"
    testFoldConst("${const_sql_131}")
    def const_sql_132 = """select cast(cast("0.9000000000000000000000000000000000000" as decimalv3(37,37)) as decimalv3(10,9));"""
    qt_sql_132_strict "${const_sql_132}"
    testFoldConst("${const_sql_132}")
    def const_sql_133 = """select cast(cast("0.9000000000000000000000000000000000001" as decimalv3(37,37)) as decimalv3(10,9));"""
    qt_sql_133_strict "${const_sql_133}"
    testFoldConst("${const_sql_133}")
    def const_sql_134 = """select cast(cast("0.9999999999999999999999999999999999998" as decimalv3(37,37)) as decimalv3(10,9));"""
    qt_sql_134_strict "${const_sql_134}"
    testFoldConst("${const_sql_134}")
    def const_sql_135 = """select cast(cast("0.9999999999999999999999999999999999999" as decimalv3(37,37)) as decimalv3(10,9));"""
    qt_sql_135_strict "${const_sql_135}"
    testFoldConst("${const_sql_135}")
    sql "set enable_strict_cast=false;"
    qt_sql_128_non_strict "${const_sql_128}"
    testFoldConst("${const_sql_128}")
    qt_sql_129_non_strict "${const_sql_129}"
    testFoldConst("${const_sql_129}")
    qt_sql_130_non_strict "${const_sql_130}"
    testFoldConst("${const_sql_130}")
    qt_sql_131_non_strict "${const_sql_131}"
    testFoldConst("${const_sql_131}")
    qt_sql_132_non_strict "${const_sql_132}"
    testFoldConst("${const_sql_132}")
    qt_sql_133_non_strict "${const_sql_133}"
    testFoldConst("${const_sql_133}")
    qt_sql_134_non_strict "${const_sql_134}"
    testFoldConst("${const_sql_134}")
    qt_sql_135_non_strict "${const_sql_135}"
    testFoldConst("${const_sql_135}")
    sql "set enable_strict_cast=true;"
    def const_sql_136 = """select cast(cast("0" as decimalv3(38,0)) as decimalv3(10,9));"""
    qt_sql_136_strict "${const_sql_136}"
    testFoldConst("${const_sql_136}")
    def const_sql_137 = """select cast(cast("8" as decimalv3(38,0)) as decimalv3(10,9));"""
    qt_sql_137_strict "${const_sql_137}"
    testFoldConst("${const_sql_137}")
    def const_sql_138 = """select cast(cast("9" as decimalv3(38,0)) as decimalv3(10,9));"""
    qt_sql_138_strict "${const_sql_138}"
    testFoldConst("${const_sql_138}")
    sql "set enable_strict_cast=false;"
    qt_sql_136_non_strict "${const_sql_136}"
    testFoldConst("${const_sql_136}")
    qt_sql_137_non_strict "${const_sql_137}"
    testFoldConst("${const_sql_137}")
    qt_sql_138_non_strict "${const_sql_138}"
    testFoldConst("${const_sql_138}")
    sql "set enable_strict_cast=true;"
    def const_sql_139 = """select cast(cast("0.0" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_139_strict "${const_sql_139}"
    testFoldConst("${const_sql_139}")
    def const_sql_140 = """select cast(cast("0.1" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_140_strict "${const_sql_140}"
    testFoldConst("${const_sql_140}")
    def const_sql_141 = """select cast(cast("0.8" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_141_strict "${const_sql_141}"
    testFoldConst("${const_sql_141}")
    def const_sql_142 = """select cast(cast("0.9" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_142_strict "${const_sql_142}"
    testFoldConst("${const_sql_142}")
    def const_sql_143 = """select cast(cast("8.0" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_143_strict "${const_sql_143}"
    testFoldConst("${const_sql_143}")
    def const_sql_144 = """select cast(cast("8.1" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_144_strict "${const_sql_144}"
    testFoldConst("${const_sql_144}")
    def const_sql_145 = """select cast(cast("8.8" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_145_strict "${const_sql_145}"
    testFoldConst("${const_sql_145}")
    def const_sql_146 = """select cast(cast("8.9" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_146_strict "${const_sql_146}"
    testFoldConst("${const_sql_146}")
    def const_sql_147 = """select cast(cast("9.0" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_147_strict "${const_sql_147}"
    testFoldConst("${const_sql_147}")
    def const_sql_148 = """select cast(cast("9.1" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_148_strict "${const_sql_148}"
    testFoldConst("${const_sql_148}")
    def const_sql_149 = """select cast(cast("9.8" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_149_strict "${const_sql_149}"
    testFoldConst("${const_sql_149}")
    def const_sql_150 = """select cast(cast("9.9" as decimalv3(38,1)) as decimalv3(10,9));"""
    qt_sql_150_strict "${const_sql_150}"
    testFoldConst("${const_sql_150}")
    sql "set enable_strict_cast=false;"
    qt_sql_139_non_strict "${const_sql_139}"
    testFoldConst("${const_sql_139}")
    qt_sql_140_non_strict "${const_sql_140}"
    testFoldConst("${const_sql_140}")
    qt_sql_141_non_strict "${const_sql_141}"
    testFoldConst("${const_sql_141}")
    qt_sql_142_non_strict "${const_sql_142}"
    testFoldConst("${const_sql_142}")
    qt_sql_143_non_strict "${const_sql_143}"
    testFoldConst("${const_sql_143}")
    qt_sql_144_non_strict "${const_sql_144}"
    testFoldConst("${const_sql_144}")
    qt_sql_145_non_strict "${const_sql_145}"
    testFoldConst("${const_sql_145}")
    qt_sql_146_non_strict "${const_sql_146}"
    testFoldConst("${const_sql_146}")
    qt_sql_147_non_strict "${const_sql_147}"
    testFoldConst("${const_sql_147}")
    qt_sql_148_non_strict "${const_sql_148}"
    testFoldConst("${const_sql_148}")
    qt_sql_149_non_strict "${const_sql_149}"
    testFoldConst("${const_sql_149}")
    qt_sql_150_non_strict "${const_sql_150}"
    testFoldConst("${const_sql_150}")
    sql "set enable_strict_cast=true;"
    def const_sql_151 = """select cast(cast("0.0000000000000000000" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_151_strict "${const_sql_151}"
    testFoldConst("${const_sql_151}")
    def const_sql_152 = """select cast(cast("0.0000000000000000001" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_152_strict "${const_sql_152}"
    testFoldConst("${const_sql_152}")
    def const_sql_153 = """select cast(cast("0.0000000000000000009" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_153_strict "${const_sql_153}"
    testFoldConst("${const_sql_153}")
    def const_sql_154 = """select cast(cast("0.0999999999999999999" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_154_strict "${const_sql_154}"
    testFoldConst("${const_sql_154}")
    def const_sql_155 = """select cast(cast("0.9000000000000000000" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_155_strict "${const_sql_155}"
    testFoldConst("${const_sql_155}")
    def const_sql_156 = """select cast(cast("0.9000000000000000001" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_156_strict "${const_sql_156}"
    testFoldConst("${const_sql_156}")
    def const_sql_157 = """select cast(cast("0.9999999999999999998" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_157_strict "${const_sql_157}"
    testFoldConst("${const_sql_157}")
    def const_sql_158 = """select cast(cast("0.9999999999999999999" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_158_strict "${const_sql_158}"
    testFoldConst("${const_sql_158}")
    def const_sql_159 = """select cast(cast("8.0000000000000000000" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_159_strict "${const_sql_159}"
    testFoldConst("${const_sql_159}")
    def const_sql_160 = """select cast(cast("8.0000000000000000001" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_160_strict "${const_sql_160}"
    testFoldConst("${const_sql_160}")
    def const_sql_161 = """select cast(cast("8.0000000000000000009" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_161_strict "${const_sql_161}"
    testFoldConst("${const_sql_161}")
    def const_sql_162 = """select cast(cast("8.0999999999999999999" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_162_strict "${const_sql_162}"
    testFoldConst("${const_sql_162}")
    def const_sql_163 = """select cast(cast("8.9000000000000000000" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_163_strict "${const_sql_163}"
    testFoldConst("${const_sql_163}")
    def const_sql_164 = """select cast(cast("8.9000000000000000001" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_164_strict "${const_sql_164}"
    testFoldConst("${const_sql_164}")
    def const_sql_165 = """select cast(cast("8.9999999999999999998" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_165_strict "${const_sql_165}"
    testFoldConst("${const_sql_165}")
    def const_sql_166 = """select cast(cast("8.9999999999999999999" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_166_strict "${const_sql_166}"
    testFoldConst("${const_sql_166}")
    def const_sql_167 = """select cast(cast("9.0000000000000000000" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_167_strict "${const_sql_167}"
    testFoldConst("${const_sql_167}")
    def const_sql_168 = """select cast(cast("9.0000000000000000001" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_168_strict "${const_sql_168}"
    testFoldConst("${const_sql_168}")
    def const_sql_169 = """select cast(cast("9.0000000000000000009" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_169_strict "${const_sql_169}"
    testFoldConst("${const_sql_169}")
    def const_sql_170 = """select cast(cast("9.0999999999999999999" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_170_strict "${const_sql_170}"
    testFoldConst("${const_sql_170}")
    def const_sql_171 = """select cast(cast("9.9000000000000000000" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_171_strict "${const_sql_171}"
    testFoldConst("${const_sql_171}")
    def const_sql_172 = """select cast(cast("9.9000000000000000001" as decimalv3(38,19)) as decimalv3(10,9));"""
    qt_sql_172_strict "${const_sql_172}"
    testFoldConst("${const_sql_172}")
    sql "set enable_strict_cast=false;"
    qt_sql_151_non_strict "${const_sql_151}"
    testFoldConst("${const_sql_151}")
    qt_sql_152_non_strict "${const_sql_152}"
    testFoldConst("${const_sql_152}")
    qt_sql_153_non_strict "${const_sql_153}"
    testFoldConst("${const_sql_153}")
    qt_sql_154_non_strict "${const_sql_154}"
    testFoldConst("${const_sql_154}")
    qt_sql_155_non_strict "${const_sql_155}"
    testFoldConst("${const_sql_155}")
    qt_sql_156_non_strict "${const_sql_156}"
    testFoldConst("${const_sql_156}")
    qt_sql_157_non_strict "${const_sql_157}"
    testFoldConst("${const_sql_157}")
    qt_sql_158_non_strict "${const_sql_158}"
    testFoldConst("${const_sql_158}")
    qt_sql_159_non_strict "${const_sql_159}"
    testFoldConst("${const_sql_159}")
    qt_sql_160_non_strict "${const_sql_160}"
    testFoldConst("${const_sql_160}")
    qt_sql_161_non_strict "${const_sql_161}"
    testFoldConst("${const_sql_161}")
    qt_sql_162_non_strict "${const_sql_162}"
    testFoldConst("${const_sql_162}")
    qt_sql_163_non_strict "${const_sql_163}"
    testFoldConst("${const_sql_163}")
    qt_sql_164_non_strict "${const_sql_164}"
    testFoldConst("${const_sql_164}")
    qt_sql_165_non_strict "${const_sql_165}"
    testFoldConst("${const_sql_165}")
    qt_sql_166_non_strict "${const_sql_166}"
    testFoldConst("${const_sql_166}")
    qt_sql_167_non_strict "${const_sql_167}"
    testFoldConst("${const_sql_167}")
    qt_sql_168_non_strict "${const_sql_168}"
    testFoldConst("${const_sql_168}")
    qt_sql_169_non_strict "${const_sql_169}"
    testFoldConst("${const_sql_169}")
    qt_sql_170_non_strict "${const_sql_170}"
    testFoldConst("${const_sql_170}")
    qt_sql_171_non_strict "${const_sql_171}"
    testFoldConst("${const_sql_171}")
    qt_sql_172_non_strict "${const_sql_172}"
    testFoldConst("${const_sql_172}")
    sql "set enable_strict_cast=true;"
    def const_sql_173 = """select cast(cast("0.0000000000000000000000000000000000000" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_173_strict "${const_sql_173}"
    testFoldConst("${const_sql_173}")
    def const_sql_174 = """select cast(cast("0.0000000000000000000000000000000000001" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_174_strict "${const_sql_174}"
    testFoldConst("${const_sql_174}")
    def const_sql_175 = """select cast(cast("0.0000000000000000000000000000000000009" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_175_strict "${const_sql_175}"
    testFoldConst("${const_sql_175}")
    def const_sql_176 = """select cast(cast("0.0999999999999999999999999999999999999" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_176_strict "${const_sql_176}"
    testFoldConst("${const_sql_176}")
    def const_sql_177 = """select cast(cast("0.9000000000000000000000000000000000000" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_177_strict "${const_sql_177}"
    testFoldConst("${const_sql_177}")
    def const_sql_178 = """select cast(cast("0.9000000000000000000000000000000000001" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_178_strict "${const_sql_178}"
    testFoldConst("${const_sql_178}")
    def const_sql_179 = """select cast(cast("0.9999999999999999999999999999999999998" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_179_strict "${const_sql_179}"
    testFoldConst("${const_sql_179}")
    def const_sql_180 = """select cast(cast("0.9999999999999999999999999999999999999" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_180_strict "${const_sql_180}"
    testFoldConst("${const_sql_180}")
    def const_sql_181 = """select cast(cast("8.0000000000000000000000000000000000000" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_181_strict "${const_sql_181}"
    testFoldConst("${const_sql_181}")
    def const_sql_182 = """select cast(cast("8.0000000000000000000000000000000000001" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_182_strict "${const_sql_182}"
    testFoldConst("${const_sql_182}")
    def const_sql_183 = """select cast(cast("8.0000000000000000000000000000000000009" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_183_strict "${const_sql_183}"
    testFoldConst("${const_sql_183}")
    def const_sql_184 = """select cast(cast("8.0999999999999999999999999999999999999" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_184_strict "${const_sql_184}"
    testFoldConst("${const_sql_184}")
    def const_sql_185 = """select cast(cast("8.9000000000000000000000000000000000000" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_185_strict "${const_sql_185}"
    testFoldConst("${const_sql_185}")
    def const_sql_186 = """select cast(cast("8.9000000000000000000000000000000000001" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_186_strict "${const_sql_186}"
    testFoldConst("${const_sql_186}")
    def const_sql_187 = """select cast(cast("8.9999999999999999999999999999999999998" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_187_strict "${const_sql_187}"
    testFoldConst("${const_sql_187}")
    def const_sql_188 = """select cast(cast("8.9999999999999999999999999999999999999" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_188_strict "${const_sql_188}"
    testFoldConst("${const_sql_188}")
    def const_sql_189 = """select cast(cast("9.0000000000000000000000000000000000000" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_189_strict "${const_sql_189}"
    testFoldConst("${const_sql_189}")
    def const_sql_190 = """select cast(cast("9.0000000000000000000000000000000000001" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_190_strict "${const_sql_190}"
    testFoldConst("${const_sql_190}")
    def const_sql_191 = """select cast(cast("9.0000000000000000000000000000000000009" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_191_strict "${const_sql_191}"
    testFoldConst("${const_sql_191}")
    def const_sql_192 = """select cast(cast("9.0999999999999999999999999999999999999" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_192_strict "${const_sql_192}"
    testFoldConst("${const_sql_192}")
    def const_sql_193 = """select cast(cast("9.9000000000000000000000000000000000000" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_193_strict "${const_sql_193}"
    testFoldConst("${const_sql_193}")
    def const_sql_194 = """select cast(cast("9.9000000000000000000000000000000000001" as decimalv3(38,37)) as decimalv3(10,9));"""
    qt_sql_194_strict "${const_sql_194}"
    testFoldConst("${const_sql_194}")
    sql "set enable_strict_cast=false;"
    qt_sql_173_non_strict "${const_sql_173}"
    testFoldConst("${const_sql_173}")
    qt_sql_174_non_strict "${const_sql_174}"
    testFoldConst("${const_sql_174}")
    qt_sql_175_non_strict "${const_sql_175}"
    testFoldConst("${const_sql_175}")
    qt_sql_176_non_strict "${const_sql_176}"
    testFoldConst("${const_sql_176}")
    qt_sql_177_non_strict "${const_sql_177}"
    testFoldConst("${const_sql_177}")
    qt_sql_178_non_strict "${const_sql_178}"
    testFoldConst("${const_sql_178}")
    qt_sql_179_non_strict "${const_sql_179}"
    testFoldConst("${const_sql_179}")
    qt_sql_180_non_strict "${const_sql_180}"
    testFoldConst("${const_sql_180}")
    qt_sql_181_non_strict "${const_sql_181}"
    testFoldConst("${const_sql_181}")
    qt_sql_182_non_strict "${const_sql_182}"
    testFoldConst("${const_sql_182}")
    qt_sql_183_non_strict "${const_sql_183}"
    testFoldConst("${const_sql_183}")
    qt_sql_184_non_strict "${const_sql_184}"
    testFoldConst("${const_sql_184}")
    qt_sql_185_non_strict "${const_sql_185}"
    testFoldConst("${const_sql_185}")
    qt_sql_186_non_strict "${const_sql_186}"
    testFoldConst("${const_sql_186}")
    qt_sql_187_non_strict "${const_sql_187}"
    testFoldConst("${const_sql_187}")
    qt_sql_188_non_strict "${const_sql_188}"
    testFoldConst("${const_sql_188}")
    qt_sql_189_non_strict "${const_sql_189}"
    testFoldConst("${const_sql_189}")
    qt_sql_190_non_strict "${const_sql_190}"
    testFoldConst("${const_sql_190}")
    qt_sql_191_non_strict "${const_sql_191}"
    testFoldConst("${const_sql_191}")
    qt_sql_192_non_strict "${const_sql_192}"
    testFoldConst("${const_sql_192}")
    qt_sql_193_non_strict "${const_sql_193}"
    testFoldConst("${const_sql_193}")
    qt_sql_194_non_strict "${const_sql_194}"
    testFoldConst("${const_sql_194}")
    sql "set enable_strict_cast=true;"
    def const_sql_195 = """select cast(cast("0.00000000000000000000000000000000000000" as decimalv3(38,38)) as decimalv3(10,9));"""
    qt_sql_195_strict "${const_sql_195}"
    testFoldConst("${const_sql_195}")
    def const_sql_196 = """select cast(cast("0.00000000000000000000000000000000000001" as decimalv3(38,38)) as decimalv3(10,9));"""
    qt_sql_196_strict "${const_sql_196}"
    testFoldConst("${const_sql_196}")
    def const_sql_197 = """select cast(cast("0.00000000000000000000000000000000000009" as decimalv3(38,38)) as decimalv3(10,9));"""
    qt_sql_197_strict "${const_sql_197}"
    testFoldConst("${const_sql_197}")
    def const_sql_198 = """select cast(cast("0.09999999999999999999999999999999999999" as decimalv3(38,38)) as decimalv3(10,9));"""
    qt_sql_198_strict "${const_sql_198}"
    testFoldConst("${const_sql_198}")
    def const_sql_199 = """select cast(cast("0.90000000000000000000000000000000000000" as decimalv3(38,38)) as decimalv3(10,9));"""
    qt_sql_199_strict "${const_sql_199}"
    testFoldConst("${const_sql_199}")
    def const_sql_200 = """select cast(cast("0.90000000000000000000000000000000000001" as decimalv3(38,38)) as decimalv3(10,9));"""
    qt_sql_200_strict "${const_sql_200}"
    testFoldConst("${const_sql_200}")
    def const_sql_201 = """select cast(cast("0.99999999999999999999999999999999999998" as decimalv3(38,38)) as decimalv3(10,9));"""
    qt_sql_201_strict "${const_sql_201}"
    testFoldConst("${const_sql_201}")
    def const_sql_202 = """select cast(cast("0.99999999999999999999999999999999999999" as decimalv3(38,38)) as decimalv3(10,9));"""
    qt_sql_202_strict "${const_sql_202}"
    testFoldConst("${const_sql_202}")
    sql "set enable_strict_cast=false;"
    qt_sql_195_non_strict "${const_sql_195}"
    testFoldConst("${const_sql_195}")
    qt_sql_196_non_strict "${const_sql_196}"
    testFoldConst("${const_sql_196}")
    qt_sql_197_non_strict "${const_sql_197}"
    testFoldConst("${const_sql_197}")
    qt_sql_198_non_strict "${const_sql_198}"
    testFoldConst("${const_sql_198}")
    qt_sql_199_non_strict "${const_sql_199}"
    testFoldConst("${const_sql_199}")
    qt_sql_200_non_strict "${const_sql_200}"
    testFoldConst("${const_sql_200}")
    qt_sql_201_non_strict "${const_sql_201}"
    testFoldConst("${const_sql_201}")
    qt_sql_202_non_strict "${const_sql_202}"
    testFoldConst("${const_sql_202}")
}