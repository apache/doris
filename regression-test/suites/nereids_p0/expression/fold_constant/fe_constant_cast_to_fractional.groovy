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

suite("fe_constant_cast_to_fractional") {
    sql """set enable_nereids_planner=true"""
    sql """set enable_fallback_to_original_planner=false"""
    sql """set debug_skip_fold_constant=false"""
    sql """set enable_strict_cast=true"""

    // *******************TEST OF STRICT MODE*************************

    qt_float1("""select cast("123.456" as float)""")
    qt_float2("""select cast("123456." as float)""")
    qt_float3("""select cast("123456" as float)""")
    qt_float4("""select cast(".123456" as float)""")
    qt_float5("""select cast("  \t\n123.456  \n\n" as float)""")
    qt_float6("""select cast("  \t\n+123.456  \n\n" as float)""")
    qt_float7("""select cast("  \t\n-123.456  \n\n" as float)""")
    qt_float8("""select cast("  +1.234e5" as float)""")
    qt_float9("""select cast("  +1.234e+5" as float)""")
    qt_float10("""select cast("  +1.23456e-1" as float)""")
    qt_float11("""select cast("  -1.23456e-1" as float)""")
    qt_float12("""select cast("  -1.234e+5" as float)""")
    qt_float13("""select cast("nan" as float)""")
    qt_float14("""select cast("inf" as float)""")
    qt_float15("""select cast("-inf" as float)""")
    qt_float16("""select cast("infinity" as float)""")
    qt_float17("""select cast("-infinity" as float)""")
    qt_float18("""select cast("1.7e409" as float)""")
    qt_float19("""select cast("-1.7e409" as float)""")
    test {
        sql """select cast("123.456a" as float)"""
        exception "123.456a can't cast to float in strict mode"
    }

    qt_double1("""select cast("123.456" as double)""")
    qt_double2("""select cast("123456." as double)""")
    qt_double3("""select cast("123456" as double)""")
    qt_double4("""select cast(".123456" as double)""")
    qt_double5("""select cast("  \t\n123.456  \n\n" as double)""")
    qt_double6("""select cast("  \t\n+123.456  \n\n" as double)""")
    qt_double7("""select cast("  \t\n-123.456  \n\n" as double)""")
    qt_double8("""select cast("  +1.234e5" as double)""")
    qt_double9("""select cast("  +1.234e+5" as double)""")
    qt_double10("""select cast("  +1.23456e-1" as double)""")
    qt_double11("""select cast("  -1.23456e-1" as double)""")
    qt_double12("""select cast("  -1.234e+5" as double)""")
    qt_double13("""select cast("nan" as double)""")
    qt_double14("""select cast("inf" as double)""")
    qt_double15("""select cast("-inf" as double)""")
    qt_double16("""select cast("infinity" as double)""")
    qt_double17("""select cast("-infinity" as double)""")
    qt_double18("""select cast("1.7e409" as double)""")
    qt_double19("""select cast("-1.7e409" as double)""")
    test {
        sql """select cast("123.456a" as double)"""
        exception "123.456a can't cast to double in strict mode"
    }

    // *******************TEST OF NON STRICT MODE*************************
    sql """set enable_strict_cast=false"""

    qt_float1("""select cast("123.456" as float)""")
    qt_float2("""select cast("123456." as float)""")
    qt_float3("""select cast("123456" as float)""")
    qt_float4("""select cast(".123456" as float)""")
    qt_float5("""select cast("  \t\n123.456  \n\n" as float)""")
    qt_float6("""select cast("  \t\n+123.456  \n\n" as float)""")
    qt_float7("""select cast("  \t\n-123.456  \n\n" as float)""")
    qt_float8("""select cast("  +1.234e5" as float)""")
    qt_float9("""select cast("  +1.234e+5" as float)""")
    qt_float10("""select cast("  +1.23456e-1" as float)""")
    qt_float11("""select cast("  -1.23456e-1" as float)""")
    qt_float12("""select cast("  -1.234e+5" as float)""")
    qt_float13("""select cast("nan" as float)""")
    qt_float14("""select cast("inf" as float)""")
    qt_float15("""select cast("-inf" as float)""")
    qt_float16("""select cast("infinity" as float)""")
    qt_float17("""select cast("-infinity" as float)""")
    qt_float18("""select cast("1.7e409" as float)""")
    qt_float19("""select cast("-1.7e409" as float)""")
    qt_float20("""select cast("123.456a" as float)""")

    qt_double1("""select cast("123.456" as double)""")
    qt_double2("""select cast("123456." as double)""")
    qt_double3("""select cast("123456" as double)""")
    qt_double4("""select cast(".123456" as double)""")
    qt_double5("""select cast("  \t\n123.456  \n\n" as double)""")
    qt_double6("""select cast("  \t\n+123.456  \n\n" as double)""")
    qt_double7("""select cast("  \t\n-123.456  \n\n" as double)""")
    qt_double8("""select cast("  +1.234e5" as double)""")
    qt_double9("""select cast("  +1.234e+5" as double)""")
    qt_double10("""select cast("  +1.23456e-1" as double)""")
    qt_double11("""select cast("  -1.23456e-1" as double)""")
    qt_double12("""select cast("  -1.234e+5" as double)""")
    qt_double13("""select cast("nan" as double)""")
    qt_double14("""select cast("inf" as double)""")
    qt_double15("""select cast("-inf" as double)""")
    qt_double16("""select cast("infinity" as double)""")
    qt_double17("""select cast("-infinity" as double)""")
    qt_double18("""select cast("1.7e409" as double)""")
    qt_double19("""select cast("-1.7e409" as double)""")
    qt_double20("""select cast("123.456a" as double)""")
}

