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

suite("fe_constant_cast_to_int") {
    sql """set enable_nereids_planner=true"""
    sql """set enable_fallback_to_original_planner=false"""
    sql """set debug_skip_fold_constant=false"""
    sql """set enable_strict_cast=true"""

    // *******************TEST OF STRICT MODE*************************

    qt_tiny1("""select cast("1" as tinyint)""")
    qt_tiny2("""select cast("+1" as tinyint)""")
    qt_tiny3("""select cast("0" as tinyint)""")
    qt_tiny4("""select cast("-1" as tinyint)""")
    qt_tiny5("""select cast(" \t\r+100 \t\n " as tinyint)""")
    qt_tiny6("""select cast("127" as tinyint)""")
    qt_tiny7("""select cast("-128" as tinyint)""")

    qt_small1("""select cast("1" as smallint)""")
    qt_small2("""select cast("+1" as smallint)""")
    qt_small3("""select cast("0" as smallint)""")
    qt_small4("""select cast("-1" as smallint)""")
    qt_small5("""select cast(" \t\r+100 \t\n " as smallint)""")
    qt_small6("""select cast("32767" as smallint)""")
    qt_small7("""select cast("-32768" as smallint)""")

    qt_int1("""select cast("1" as int)""")
    qt_int2("""select cast("+1" as int)""")
    qt_int3("""select cast("0" as int)""")
    qt_int4("""select cast("-1" as int)""")
    qt_int5("""select cast(" \t\r+100 \t\n " as int)""")
    qt_int6("""select cast("2147483647" as int)""")
    qt_int7("""select cast("-2147483648" as int)""")

    qt_bigint1("""select cast("1" as bigint)""")
    qt_bigint2("""select cast("+1" as bigint)""")
    qt_bigint3("""select cast("0" as bigint)""")
    qt_bigint4("""select cast("-1" as bigint)""")
    qt_bigint5("""select cast(" \t\r+100 \t\n " as bigint)""")
    qt_bigint6("""select cast("9223372036854775807" as bigint)""")
    qt_bigint7("""select cast("-9223372036854775808" as bigint)""")

    qt_largeint1("""select cast("1" as largeint)""")
    qt_largeint2("""select cast("+1" as largeint)""")
    qt_largeint3("""select cast("0" as largeint)""")
    qt_largeint4("""select cast("-1" as largeint)""")
    qt_largeint5("""select cast(" \t\r+100 \t\n " as largeint)""")
    qt_largeint6("""select cast("170141183460469231731687303715884105727" as largeint)""")
    qt_largeint7("""select cast("-170141183460469231731687303715884105728" as largeint)""")

    test {
        sql """select cast("1.1" as int)"""
        exception """1.1 can't cast to INT in strict mode"""
    }
    test {
        sql """select cast("128" as tinyint)"""
        exception """128 can't cast to TINYINT in strict mode"""
    }
    test {
        sql """select cast("-129" as tinyint)"""
        exception """-129 can't cast to TINYINT in strict mode"""
    }
    test {
        sql """select cast("32768" as smallint)"""
        exception """32768 can't cast to SMALLINT in strict mode"""
    }
    test {
        sql """select cast("-32769" as smallint)"""
        exception """-32769 can't cast to SMALLINT in strict mode"""
    }
    test {
        sql """select cast("2147483648" as int)"""
        exception """2147483648 can't cast to INT in strict mode"""
    }
    test {
        sql """select cast("-2147483649" as int)"""
        exception """-2147483649 can't cast to INT in strict mode"""
    }
    test {
        sql """select cast("9223372036854775808" as bigint)"""
        exception """9223372036854775808 can't cast to BIGINT in strict mode"""
    }
    test {
        sql """select cast("-9223372036854775809" as bigint)"""
        exception """-9223372036854775809 can't cast to BIGINT in strict mode"""
    }
    test {
        sql """select cast("170141183460469231731687303715884105728" as largeint)"""
        exception """170141183460469231731687303715884105728 can't cast to LARGEINT in strict mode"""
    }
    test {
        sql """select cast("-170141183460469231731687303715884105729" as largeint)"""
        exception """-170141183460469231731687303715884105729 can't cast to LARGEINT in strict mode"""
    }

    qt_bool1("""select cast(true as tinyint)""")
    qt_bool2("""select cast(true as smallint)""")
    qt_bool3("""select cast(true as int)""")
    qt_bool4("""select cast(true as bigint)""")
    qt_bool5("""select cast(true as largeint)""")
    qt_bool6("""select cast(false as tinyint)""")
    qt_bool7("""select cast(false as smallint)""")
    qt_bool8("""select cast(false as int)""")
    qt_bool9("""select cast(false as bigint)""")
    qt_bool10("""select cast(false as largeint)""")

    qt_integer1("""select cast(127 as tinyint)""")
    qt_integer2("""select cast(-128 as tinyint)""")
    qt_integer3("""select cast(32767 as smallint)""")
    qt_integer4("""select cast(-32768 as smallint)""")
    qt_integer5("""select cast(2147483647 as int)""")
    qt_integer6("""select cast(-2147483648 as int)""")
    qt_integer7("""select cast(9223372036854775807 as bigint)""")
    qt_integer8("""select cast(-9223372036854775808 as bigint)""")
    qt_integer9("""select cast(170141183460469231731687303715884105727 as largeint)""")
    qt_integer10("""select cast(-170141183460469231731687303715884105728 as largeint)""")

    qt_date1("""select cast(cast("2025-05-31" as date) as int)""")
    qt_date2("""select cast(cast("2025-05-31" as date) as bigint)""")
    qt_date3("""select cast(cast("2025-05-31" as date) as largeint)""")
    qt_datetime1("""select cast(cast("2025-05-31T21:11:22.334455" as date) as bigint)""")
    qt_datetime2("""select cast(cast("2025-05-31T21:11:22.334455" as date) as largeint)""")

    qt_float1("""select cast(cast(1.8 as float) as tinyint)""")
    qt_float2("""select cast(cast(-1.9 as float) as tinyint)""")
    qt_float3("""select cast(cast(-0 as float) as tinyint)""")
    qt_float4("""select cast(cast(+0 as float) as tinyint)""")
    qt_float5("""select cast(cast(1.8 as float) as smallint)""")
    qt_float6("""select cast(cast(-1.9 as float) as smallint)""")
    qt_float7("""select cast(cast(-0 as float) as smallint)""")
    qt_float8("""select cast(cast(+0 as float) as smallint)""")
    qt_float9("""select cast(cast(1.8 as float) as int)""")
    qt_float10("""select cast(cast(-1.9 as float) as int)""")
    qt_float11("""select cast(cast(-0 as float) as int)""")
    qt_float12("""select cast(cast(+0 as float) as int)""")
    qt_float13("""select cast(cast(1.8 as float) as bigint)""")
    qt_float14("""select cast(cast(-1.9 as float) as bigint)""")
    qt_float15("""select cast(cast(-0 as float) as bigint)""")
    qt_float16("""select cast(cast(+0 as float) as bigint)""")
    qt_float17("""select cast(cast(1.8 as float) as largeint)""")
    qt_float18("""select cast(cast(-1.9 as float) as largeint)""")
    qt_float19("""select cast(cast(-0 as float) as largeint)""")
    qt_float20("""select cast(cast(+0 as float) as largeint)""")

    qt_double1("""select cast(cast(1.5 as double) as tinyint)""")
    qt_double2("""select cast(cast(-1.5 as double) as tinyint)""")
    qt_double3("""select cast(cast(-0 as double) as tinyint)""")
    qt_double4("""select cast(cast(+0 as double) as tinyint)""")
    qt_double5("""select cast(cast(1.5 as double) as smallint)""")
    qt_double6("""select cast(cast(-1.5 as double) as smallint)""")
    qt_double7("""select cast(cast(-0 as double) as smallint)""")
    qt_double8("""select cast(cast(+0 as double) as smallint)""")
    qt_double9("""select cast(cast(1.5 as double) as int)""")
    qt_double10("""select cast(cast(-1.5 as double) as int)""")
    qt_double11("""select cast(cast(-0 as double) as int)""")
    qt_double12("""select cast(cast(+0 as double) as int)""")
    qt_double13("""select cast(cast(1.5 as double) as bigint)""")
    qt_double14("""select cast(cast(-1.5 as double) as bigint)""")
    qt_double15("""select cast(cast(-0 as double) as bigint)""")
    qt_double16("""select cast(cast(+0 as double) as bigint)""")
    qt_double17("""select cast(cast(1.5 as double) as largeint)""")
    qt_double18("""select cast(cast(-1.5 as double) as largeint)""")
    qt_double19("""select cast(cast(-0 as double) as largeint)""")
    qt_double20("""select cast(cast(+0 as double) as largeint)""")

    test {
        sql """select cast(cast("NaN" as double) as largeint)"""
        exception "NaN can't cast to LARGEINT in strict mode"
    }
    test {
        sql """select cast(cast("inf" as double) as largeint)"""
        exception "Infinity can't cast to LARGEINT in strict mode"
    }
    test {
        sql """select cast(cast("-inf" as double) as largeint)"""
        exception "-Infinity can't cast to LARGEINT in strict mode"
    }

    qt_decimal1("""select cast(cast("1.23" as decimal) as int)""")
    test {
        sql """select cast(cast("12983472938234234.23" as decimal(30, 5)) as int)"""
        exception "12983472938234234.23000 can't cast to INT, overflow"
    }

    // *******************TEST OF NON STRICT MODE*************************

    sql """set enable_strict_cast=false"""

    qt_tiny1("""select cast("1" as tinyint)""")
    qt_tiny2("""select cast("+1" as tinyint)""")
    qt_tiny3("""select cast("0" as tinyint)""")
    qt_tiny4("""select cast("-1" as tinyint)""")
    qt_tiny5("""select cast(" \t\r+100 \t\n " as tinyint)""")
    qt_tiny6("""select cast("127" as tinyint)""")
    qt_tiny7("""select cast("-128" as tinyint)""")
    qt_tiny8("""select cast("128" as tinyint)""")
    qt_tiny9("""select cast("-129" as tinyint)""")

    qt_small1("""select cast("1" as smallint)""")
    qt_small2("""select cast("+1" as smallint)""")
    qt_small3("""select cast("0" as smallint)""")
    qt_small4("""select cast("-1" as smallint)""")
    qt_small5("""select cast(" \t\r+100 \t\n " as smallint)""")
    qt_small6("""select cast("32767" as smallint)""")
    qt_small7("""select cast("-32768" as smallint)""")
    qt_small8("""select cast("32768" as smallint)""")
    qt_small9("""select cast("-32769" as smallint)""")

    qt_int1("""select cast("1" as int)""")
    qt_int2("""select cast("+1" as int)""")
    qt_int3("""select cast("0" as int)""")
    qt_int4("""select cast("-1" as int)""")
    qt_int5("""select cast(" \t\r+100 \t\n " as int)""")
    qt_int6("""select cast("2147483647" as int)""")
    qt_int7("""select cast("-2147483648" as int)""")
    qt_int8("""select cast("2147483648" as int)""")
    qt_int9("""select cast("-2147483649" as int)""")
    qt_int10("""select cast("1.1" as int)""")
    qt_int11("""select cast("-1.1" as int)""")
    qt_int12("""select cast("aaa" as int)""")
    qt_int13("""select cast("1." as int)""")
    qt_int14("""select cast(".1" as int)""")

    qt_bigint1("""select cast("1" as bigint)""")
    qt_bigint2("""select cast("+1" as bigint)""")
    qt_bigint3("""select cast("0" as bigint)""")
    qt_bigint4("""select cast("-1" as bigint)""")
    qt_bigint5("""select cast(" \t\r+100 \t\n " as bigint)""")
    qt_bigint6("""select cast("9223372036854775807" as bigint)""")
    qt_bigint7("""select cast("-9223372036854775808" as bigint)""")
    qt_bigint8("""select cast("9223372036854775808" as bigint)""")
    qt_bigint9("""select cast("-9223372036854775809" as bigint)""")

    qt_largeint1("""select cast("1" as largeint)""")
    qt_largeint2("""select cast("+1" as largeint)""")
    qt_largeint3("""select cast("0" as largeint)""")
    qt_largeint4("""select cast("-1" as largeint)""")
    qt_largeint5("""select cast(" \t\r+100 \t\n " as largeint)""")
    qt_largeint6("""select cast("170141183460469231731687303715884105727" as largeint)""")
    qt_largeint7("""select cast("-170141183460469231731687303715884105728" as largeint)""")
    qt_largeint8("""select cast("170141183460469231731687303715884105728" as largeint)""")
    qt_largeint9("""select cast("-170141183460469231731687303715884105729" as largeint)""")

    qt_bool1("""select cast(true as tinyint)""")
    qt_bool2("""select cast(true as smallint)""")
    qt_bool3("""select cast(true as int)""")
    qt_bool4("""select cast(true as bigint)""")
    qt_bool5("""select cast(true as largeint)""")
    qt_bool6("""select cast(false as tinyint)""")
    qt_bool7("""select cast(false as smallint)""")
    qt_bool8("""select cast(false as int)""")
    qt_bool9("""select cast(false as bigint)""")
    qt_bool10("""select cast(false as largeint)""")

    qt_integer1("""select cast(127 as tinyint)""")
    qt_integer2("""select cast(-128 as tinyint)""")
    qt_integer3("""select cast(32767 as smallint)""")
    qt_integer4("""select cast(-32768 as smallint)""")
    qt_integer5("""select cast(2147483647 as int)""")
    qt_integer6("""select cast(-2147483648 as int)""")
    qt_integer7("""select cast(9223372036854775807 as bigint)""")
    qt_integer8("""select cast(-9223372036854775808 as bigint)""")
    qt_integer9("""select cast(170141183460469231731687303715884105727 as largeint)""")
    qt_integer10("""select cast(-170141183460469231731687303715884105728 as largeint)""")

    qt_date1("""select cast(cast("2025-05-31" as date) as int)""")
    qt_date2("""select cast(cast("2025-05-31" as date) as bigint)""")
    qt_date3("""select cast(cast("2025-05-31" as date) as largeint)""")
    qt_datetime1("""select cast(cast("2025-05-31T21:11:22.334455" as date) as bigint)""")
    qt_datetime2("""select cast(cast("2025-05-31T21:11:22.334455" as date) as largeint)""")

    qt_float1("""select cast(cast(1.8 as float) as tinyint)""")
    qt_float2("""select cast(cast(-1.9 as float) as tinyint)""")
    qt_float3("""select cast(cast(-0 as float) as tinyint)""")
    qt_float4("""select cast(cast(+0 as float) as tinyint)""")
    qt_float5("""select cast(cast(1.8 as float) as smallint)""")
    qt_float6("""select cast(cast(-1.9 as float) as smallint)""")
    qt_float7("""select cast(cast(-0 as float) as smallint)""")
    qt_float8("""select cast(cast(+0 as float) as smallint)""")
    qt_float9("""select cast(cast(1.8 as float) as int)""")
    qt_float10("""select cast(cast(-1.9 as float) as int)""")
    qt_float11("""select cast(cast(-0 as float) as int)""")
    qt_float12("""select cast(cast(+0 as float) as int)""")
    qt_float13("""select cast(cast(1.8 as float) as bigint)""")
    qt_float14("""select cast(cast(-1.9 as float) as bigint)""")
    qt_float15("""select cast(cast(-0 as float) as bigint)""")
    qt_float16("""select cast(cast(+0 as float) as bigint)""")
    qt_float17("""select cast(cast(1.8 as float) as largeint)""")
    qt_float18("""select cast(cast(-1.9 as float) as largeint)""")
    qt_float19("""select cast(cast(-0 as float) as largeint)""")
    qt_float20("""select cast(cast(+0 as float) as largeint)""")
    qt_float21("""select cast(cast("NaN" as float) as int)""")
    qt_float22("""select cast(cast("inf" as float) as int)""")
    qt_float23("""select cast(cast("-inf" as float) as int)""")


    qt_double1("""select cast(cast(1.5 as double) as tinyint)""")
    qt_double2("""select cast(cast(-1.5 as double) as tinyint)""")
    qt_double3("""select cast(cast(-0 as double) as tinyint)""")
    qt_double4("""select cast(cast(+0 as double) as tinyint)""")
    qt_double5("""select cast(cast(1.5 as double) as smallint)""")
    qt_double6("""select cast(cast(-1.5 as double) as smallint)""")
    qt_double7("""select cast(cast(-0 as double) as smallint)""")
    qt_double8("""select cast(cast(+0 as double) as smallint)""")
    qt_double9("""select cast(cast(1.5 as double) as int)""")
    qt_double10("""select cast(cast(-1.5 as double) as int)""")
    qt_double11("""select cast(cast(-0 as double) as int)""")
    qt_double12("""select cast(cast(+0 as double) as int)""")
    qt_double13("""select cast(cast(1.5 as double) as bigint)""")
    qt_double14("""select cast(cast(-1.5 as double) as bigint)""")
    qt_double15("""select cast(cast(-0 as double) as bigint)""")
    qt_double16("""select cast(cast(+0 as double) as bigint)""")
    qt_double17("""select cast(cast(1.5 as double) as largeint)""")
    qt_double18("""select cast(cast(-1.5 as double) as largeint)""")
    qt_double19("""select cast(cast(-0 as double) as largeint)""")
    qt_double20("""select cast(cast(+0 as double) as largeint)""")
    qt_double21("""select cast(cast("NaN" as double) as int)""")
    qt_double22("""select cast(cast("inf" as double) as int)""")
    qt_double23("""select cast(cast("-inf" as double) as int)""")

    qt_decimal1("""select cast(cast("1.23" as decimal) as int)""")
    qt_decimal2("""select cast(cast("12983472938234234.23" as decimal(30, 5)) as int)""")

}
