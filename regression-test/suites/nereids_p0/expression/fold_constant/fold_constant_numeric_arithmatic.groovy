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

suite("fold_constant_numeric_arithmatic") {
    def db = "fold_constant_string_arithmatic"
    sql "create database if not exists ${db}"

    sql "set enable_nereids_planner=true"
    sql "set enable_fallback_to_original_planner=false"
    sql "set enable_fold_constant_by_be=false"

    //Abs function cases
    testFoldConst("SELECT ABS(1)") 
    testFoldConst("SELECT ABS(0)") 
    testFoldConst("SELECT ABS(-1)") 
    testFoldConst("SELECT ABS(1.5)") 
    testFoldConst("SELECT ABS(-1.5)") 
    testFoldConst("SELECT ABS(1E308)") 
    testFoldConst("SELECT ABS(-1E308)")
    testFoldConst("SELECT ABS(NULL)") // NULL handling
    testFoldConst("SELECT ABS('')") // Empty string handling
    testFoldConst("SELECT ABS('abc')") // Invalid input
    testFoldConst("SELECT ABS(9223372036854775807)") // Max bigint
    testFoldConst("SELECT ABS(-9223372036854775808)") // Min bigint

//Acos function cases
    testFoldConst("SELECT ACOS(1) AS acos_case_1") //acos(1) = 0
    testFoldConst("SELECT ACOS(0) AS acos_case_2") //acos(0) = π/2
    testFoldConst("SELECT ACOS(-1) AS acos_case_3") //acos(-1) = π
    testFoldConst("SELECT ACOS(0.5)") // Common value
    testFoldConst("SELECT ACOS(-0.5)") // Negative common value
    testFoldConst("SELECT ACOS(NULL)") // NULL handling
    testFoldConst("SELECT ACOS(2)")
    testFoldConst("SELECT ACOS(-2)")
    testFoldConst("SELECT ACOS(1.5)")
    testFoldConst("SELECT ACOS(-1.5)")
    testFoldConst("SELECT ACOS(1E308)")
    testFoldConst("SELECT ACOS(-1E308)")

//Asin function cases
    testFoldConst("SELECT ASIN(1) AS asin_case_1") //asin(1) = π/2
    testFoldConst("SELECT ASIN(0) AS asin_case_2") //asin(0) = 0
    testFoldConst("SELECT ASIN(-1) AS asin_case_3") //asin(-1) = -π/2
    testFoldConst("SELECT ASIN(0.5)") // Common value
    testFoldConst("SELECT ASIN(-0.5)") // Negative common value
    testFoldConst("SELECT ASIN(NULL)") // NULL handling
    testFoldConst("SELECT ASIN(2)")
    testFoldConst("SELECT ASIN(-2)")
    testFoldConst("SELECT ASIN(1.5)")
    testFoldConst("SELECT ASIN(-1.5)")
    testFoldConst("SELECT ASIN(1E308)")
    testFoldConst("SELECT ASIN(-1E308)")

//Atan function cases
    testFoldConst("SELECT ATAN(1) AS atan_case_1") //atan(1) = π/4
    testFoldConst("SELECT ATAN(0) AS atan_case_2") //atan(0) = 0
    testFoldConst("SELECT ATAN(-1) AS atan_case_3") //atan(-1)
    testFoldConst("SELECT ATAN(1.5)") 
    testFoldConst("SELECT ATAN(-1.5)") 
    testFoldConst("SELECT ATAN(1E308)") 
    testFoldConst("SELECT ATAN(-1E308)")
    testFoldConst("SELECT ATAN(NULL)") // NULL handling
    testFoldConst("SELECT ATAN(PI())") // PI input
    testFoldConst("SELECT ATAN(-PI())") // Negative PI input
    testFoldConst("SELECT ATAN(1E-308)") // Very small positive number
    testFoldConst("SELECT ATAN(-1E-308)") // Very small negative number

//Atan2 function cases
    testFoldConst("SELECT ATAN2(1, 1) AS atan2_case_1") //atan2(1, 1) = π/4
    testFoldConst("SELECT ATAN2(0, 1) AS atan2_case_2") //atan2(0, 1) = 0
    testFoldConst("SELECT ATAN2(1, 0) AS atan2_case_3") //atan2(1, 0) = π/2
    testFoldConst("SELECT ATAN2(0, 0) AS atan2_case_exception") //undefined (returns NULL or error)
    testFoldConst("SELECT ATAN2(1.5, 1.5)") 
    testFoldConst("SELECT ATAN2(-1.5, 1.5)") 
    testFoldConst("SELECT ATAN2(1E308, 1E308)") 
    testFoldConst("SELECT ATAN2(-1E308, 1E308)")
    testFoldConst("SELECT ATAN2(NULL, 1)") // NULL y
    testFoldConst("SELECT ATAN2(1, NULL)") // NULL x
    testFoldConst("SELECT ATAN2(-1, -1)") // Both negative
    testFoldConst("SELECT ATAN2(1E-308, 1E-308)") // Very small numbers

//Bin function cases
    testFoldConst("SELECT BIN(5) AS bin_case_1") //bin(5) = 101
    testFoldConst("SELECT BIN(16) AS bin_case_2") //bin(16) = 10000
    testFoldConst("SELECT BIN(255) AS bin_case_3") //bin(255)
    testFoldConst("SELECT BIN(-1) AS bin_case_exception") //returns NULL or error in some databases
    testFoldConst("SELECT BIN(1E308)") 
    testFoldConst("SELECT BIN(-1E308)")
    testFoldConst("SELECT BIN(0)") // Zero case
    testFoldConst("SELECT BIN(NULL)") // NULL handling
    testFoldConst("SELECT BIN(9223372036854775807)") // Max bigint
    testFoldConst("SELECT BIN(-9223372036854775808)") // Min bigint
    testFoldConst("SELECT BIN(2147483647)") // Max int
    testFoldConst("SELECT BIN(-2147483648)") // Min int

//BitCount function cases
    testFoldConst("SELECT BIT_COUNT(5) AS bitcount_case_1") //bitcount(5) = 2 (101 has two 1s)
    testFoldConst("SELECT BIT_COUNT(16) AS bitcount_case_2") //bitcount(16) = 1
    testFoldConst("SELECT BIT_COUNT(255) AS bitcount_case_3") //bitcount(255) = 8
    testFoldConst("SELECT BIT_COUNT(-1) AS bitcount_case_exception")
    testFoldConst("SELECT BIT_COUNT(1E308)") 
    testFoldConst("SELECT BIT_COUNT(-1E308)")
    testFoldConst("SELECT BIT_COUNT(0)") // Zero case
    testFoldConst("SELECT BIT_COUNT(NULL)") // NULL handling
    testFoldConst("SELECT BIT_COUNT(9223372036854775807)") // Max bigint
    testFoldConst("SELECT BIT_COUNT(-9223372036854775808)") // Min bigint
    testFoldConst("SELECT BIT_COUNT(2147483647)") // Max int
    testFoldConst("SELECT BIT_COUNT(-2147483648)") // Min int

//Cbrt function cases
    testFoldConst("SELECT CBRT(8) AS cbrt_case_1") //cbrt(8) = 2
    testFoldConst("SELECT CBRT(-8) AS cbrt_case_2") //cbrt(-8) = -2
    testFoldConst("SELECT CBRT(1E308)")
    testFoldConst("SELECT CBRT(-1E308)")
    testFoldConst("SELECT CBRT(0)") // Zero case
    testFoldConst("SELECT CBRT(NULL)") // NULL handling
    testFoldConst("SELECT CBRT(1)") // Unit case
    testFoldConst("SELECT CBRT(-1)") // Negative unit case
    testFoldConst("SELECT CBRT(27)") // Perfect cube
    testFoldConst("SELECT CBRT(-27)") // Negative perfect cube

//Ceil function cases
    testFoldConst("SELECT CEIL(3.4) AS ceil_case_1")
    testFoldConst("SELECT CEIL(-3.4) AS ceil_case_2")
    testFoldConst("SELECT CEIL(5.0) AS ceil_case_3")
    testFoldConst("SELECT CEIL(1E308) AS ceil_case_overflow")
    testFoldConst("SELECT CEIL(1E308)") 
    testFoldConst("SELECT CEIL(-1E308)")
    testFoldConst("SELECT CEIL(NULL)") // NULL handling
    testFoldConst("SELECT CEIL(0)") // Zero case
    testFoldConst("SELECT CEIL(0.1)") // Small positive decimal
    testFoldConst("SELECT CEIL(-0.1)") // Small negative decimal
    testFoldConst("SELECT CEIL(9.99999999)") // Close to next integer
    testFoldConst("SELECT CEIL(-9.99999999)") // Negative close to next integer
    testFoldConst("SELECT CEIL(3, 2)") // Integer second argument
    testFoldConst("SELECT CEIL(5.5, 1)") // Decimal first, integer second
    testFoldConst("SELECT CEIL(-3.7, 0)") // Negative first, zero second
    testFoldConst("SELECT CEIL(10.123, 2)") // Decimal with precision
    testFoldConst("SELECT CEIL(-10.123, 1)") // Negative with precision

//Coalesce function cases
    testFoldConst("SELECT COALESCE(NULL, 5) AS coalesce_case_1")
    testFoldConst("SELECT COALESCE(NULL, NULL, 7) AS coalesce_case_2")
    testFoldConst("SELECT COALESCE(3, 5) AS coalesce_case_3")
    testFoldConst("SELECT COALESCE(NULL, NULL) AS coalesce_case_4")
    testFoldConst("SELECT COALESCE(1E308)") 
    testFoldConst("SELECT COALESCE(-1E308)")
    testFoldConst("SELECT COALESCE(NULL, NULL, NULL)") // All NULL
    testFoldConst("SELECT COALESCE('', NULL, 'test')") // Empty string
    testFoldConst("SELECT COALESCE(NULL, 0, NULL)") // Zero
    testFoldConst("SELECT COALESCE(1, NULL, 2)") // First non-NULL
    testFoldConst("SELECT COALESCE(NULL, NULL, NULL, 'last')") // Last value
    testFoldConst("SELECT COALESCE(NULL, -1, NULL, 1)") // Negative value

//Conv function cases
    testFoldConst("SELECT CONV(15, 10, 2) AS conv_case_1") //conv(15 from base 10 to base 2) = 1111
    testFoldConst("SELECT CONV(1111, 2, 10) AS conv_case_2") //conv(1111 from base 2 to base 10) = 15
    testFoldConst("SELECT CONV(255, 10, 16) AS conv_case_3") //conv(255 from base 10 to base 16) = FF
    testFoldConst("SELECT CONV(-10, 10, 2) AS conv_case_exception") //undefined or error
    testFoldConst("SELECT CONV(1E308, 10, 2)")
    testFoldConst("SELECT CONV(-1E308, 10, 16)")
    testFoldConst("SELECT CONV('FF', 16, 10)") // Hex to decimal
    testFoldConst("SELECT CONV('777', 8, 10)") // Octal to decimal
    testFoldConst("SELECT CONV(NULL, 10, 2)") // NULL handling
    testFoldConst("SELECT CONV('0', 10, 2)") // Zero case
    testFoldConst("SELECT CONV('Z', 36, 10)") // Max base-36
    testFoldConst("SELECT CONV('9223372036854775807', 10, 16)") // Max bigint

//Cos function cases
    testFoldConst("SELECT COS(PI()) AS cos_case_1") //cos(π) = -1
    testFoldConst("SELECT COS(0) AS cos_case_2") //cos(0) = 1
    testFoldConst("SELECT COS(PI() / 2) AS cos_case_3") //cos(π/2)
    testFoldConst("SELECT COS(1E308) AS cos_case_overflow")
    testFoldConst("SELECT COS(-1E308) AS cos_case_overflow")
    testFoldConst("SELECT COS(NULL)") // NULL handling
    testFoldConst("SELECT COS(PI()/4)") // 45 degrees
    testFoldConst("SELECT COS(PI()/6)") // 30 degrees
    testFoldConst("SELECT COS(2*PI())") // Full circle
    testFoldConst("SELECT COS(-PI())") // Negative PI
    testFoldConst("SELECT COS(1E-308)") // Very small number
    testFoldConst("SELECT COS(-1E-308)") // Very small negative number

//Cosh function cases
    testFoldConst("SELECT COSH(0) AS cosh_case_1") //cosh(0) = 1
//    COSH/POWER/DEGREES functions would meet inf problem when input is to large, be execute need to fix it
//    testFoldConst("SELECT COSH(1E308)")
//    testFoldConst("SELECT COSH(-1E308)")
    testFoldConst("SELECT COSH(NULL)") // NULL handling
    testFoldConst("SELECT COSH(1)") // Common value
    testFoldConst("SELECT COSH(-1)") // Negative common value
    testFoldConst("SELECT COSH(PI())") // PI input
    testFoldConst("SELECT COSH(-PI())") // Negative PI input
    testFoldConst("SELECT COSH(709.782712893384)") // Near overflow boundary
    testFoldConst("SELECT COSH(-709.782712893384)") // Near negative overflow boundary

//CountEqual function cases
    testFoldConst("SELECT COUNT(CASE WHEN 5 = 5 THEN 1 END) AS countequal_case_1") //1 (true)
    testFoldConst("SELECT COUNT(CASE WHEN 5 = 3 THEN 1 END) AS countequal_case_2") //0 (false)
    testFoldConst("SELECT COUNT(CASE WHEN 'a' = 1 THEN 1 END) AS countequal_case_exception") //undefined or error
    testFoldConst("SELECT COUNT(CASE WHEN 1E308 = 1E308 THEN 1 END) AS countequal_case_overflow")
    testFoldConst("SELECT COUNT(CASE WHEN -1E308 = -1E308 THEN 1 END) AS countequal_case_overflow")
    testFoldConst("SELECT COUNT(CASE WHEN NULL = NULL THEN 1 END)") // NULL comparison
    testFoldConst("SELECT COUNT(CASE WHEN '' = '' THEN 1 END)") // Empty string comparison
    testFoldConst("SELECT COUNT(CASE WHEN 0 = 0.0 THEN 1 END)") // Integer vs decimal
    testFoldConst("SELECT COUNT(CASE WHEN 'abc' = 'ABC' THEN 1 END)") // Case sensitivity
    testFoldConst("SELECT COUNT(CASE WHEN TRUE = FALSE THEN 1 END)") // Boolean comparison
    testFoldConst("SELECT COUNT(CASE WHEN 9223372036854775807 = 9223372036854775807 THEN 1 END)") // Max bigint
    testFoldConst("SELECT COUNT(CASE WHEN -9223372036854775808 = -9223372036854775808 THEN 1 END)") // Min bigint

//Degrees function cases
    testFoldConst("SELECT DEGREES(PI()) AS degrees_case_1") //degrees(π) = 180
    testFoldConst("SELECT DEGREES(PI()/2) AS degrees_case_2") //degrees(π/2) = 90
    testFoldConst("SELECT DEGREES(PI()/4) AS degrees_case_3") //degrees(π/4)
    testFoldConst("SELECT DEGREES(NULL)") // NULL handling
    testFoldConst("SELECT DEGREES(0)") // Zero case
    testFoldConst("SELECT DEGREES(-PI())") // Negative PI
    testFoldConst("SELECT DEGREES(2*PI())") // Full circle
    testFoldConst("SELECT DEGREES(PI()/6)") // 30 degrees
    testFoldConst("SELECT DEGREES(PI()/3)") // 60 degrees
    testFoldConst("SELECT DEGREES(3*PI()/2)") // 270 degrees
//    COSH/POWER/DEGREES functions would meet inf problem when input is to large, be execute need to fix it
//    testFoldConst("SELECT DEGREES(1E308)")
//    testFoldConst("SELECT DEGREES(-1E308)")

//Exp function cases
    testFoldConst("SELECT EXP(1) AS exp_case_1") //e^1
    testFoldConst("SELECT EXP(0) AS exp_case_2") //e^0
    testFoldConst("SELECT EXP(-1) AS exp_case_3") //e^-1
    testFoldConst("SELECT EXP(2.0) AS dexp_case_1") //dexp(2.0)
    testFoldConst("SELECT EXP(0.5) AS dexp_case_2") //dexp(0.5)
    testFoldConst("SELECT EXP(-2.0) AS dexp_case_3") //dexp(-2.0)
    testFoldConst("SELECT exp(0), exp(1), exp(-1), exp(2.5)")
    testFoldConst("SELECT EXP(NULL)") // NULL handling
    testFoldConst("SELECT EXP(PI())") // e^π
    testFoldConst("SELECT EXP(-PI())") // e^-π
    testFoldConst("SELECT EXP(1E-308)") // Very small number
    testFoldConst("SELECT EXP(-1E-308)") // Very small negative number
    testFoldConst("SELECT EXP(709.782712893384)") // Near overflow boundary
    testFoldConst("SELECT EXP(-709.782712893384)") // Near underflow boundary

//Floor function cases
    testFoldConst("SELECT FLOOR(3.7) AS floor_case_1")
    testFoldConst("SELECT FLOOR(-3.7) AS floor_case_2")
    testFoldConst("SELECT FLOOR(5.0) AS floor_case_3")
    testFoldConst("SELECT FLOOR(1E308) AS floor_case_overflow")
    testFoldConst("SELECT floor(1.23), floor(-1.23), floor(5), floor(-5.9)")
    testFoldConst("SELECT FLOOR(NULL)") // NULL handling
    testFoldConst("SELECT FLOOR(0)") // Zero case
    testFoldConst("SELECT FLOOR(0.1)") // Small positive decimal
    testFoldConst("SELECT FLOOR(-0.1)") // Small negative decimal
    testFoldConst("SELECT FLOOR(9.99999999)") // Close to next integer
    testFoldConst("SELECT FLOOR(-9.99999999)") // Negative close to next integer
    testFoldConst("SELECT FLOOR(1E-308)") // Very small number
    testFoldConst("SELECT FLOOR(-1E-308)") // Very small negative number
    testFoldConst("SELECT FLOOR(3, 2)") // Integer second argument
    testFoldConst("SELECT FLOOR(5.5, 1)") // Decimal first, integer second
    testFoldConst("SELECT FLOOR(-3.7, 0)") // Negative first, zero second
    testFoldConst("SELECT FLOOR(10.123, 2)") // Decimal with precision
    testFoldConst("SELECT FLOOR(-10.123, 1)") // Negative with precision

//Fmod function cases
    testFoldConst("SELECT MOD(10.5, 3.2) AS fmod_case_1") //fmod(10.5 % 3.2)
    testFoldConst("SELECT MOD(-10.5, 3.2) AS fmod_case_2") //fmod(-10.5 % 3.2)
    testFoldConst("SELECT MOD(10.5, -3.2) AS fmod_case_3") //fmod(10.5 % -3.2)
    testFoldConst("SELECT MOD(10.5, 0) AS fmod_case_exception") //undefined (returns NULL or error)
    testFoldConst("SELECT fmod(10.5, 3), fmod(-10.5, 3), fmod(10.5, -3)")
    testFoldConst("SELECT MOD(NULL, 3)") // NULL dividend
    testFoldConst("SELECT MOD(10, NULL)") // NULL divisor
    testFoldConst("SELECT MOD(0, 3)") // Zero dividend
    testFoldConst("SELECT MOD(10.5, 10.5)") // Equal numbers
    testFoldConst("SELECT MOD(1E308, 2)") // Very large dividend
    testFoldConst("SELECT MOD(10, 1E-308)") // Very small divisor
    testFoldConst("SELECT MOD(1E-308, 2)") // Very small dividend

//Fpow function cases
    testFoldConst("SELECT POWER(2.5, 3.2) AS fpow_case_1") //fpow(2.5^3.2)
    testFoldConst("SELECT POWER(10.0, 0.0) AS fpow_case_2") //fpow(10.0^0.0) = 1.0
    testFoldConst("SELECT POWER(5.5, -1.2) AS fpow_case_3") //fpow(5.5^-1.2)
    testFoldConst("SELECT fpow(2.0, 3.0), fpow(3.0, 2.0), fpow(2.5, 1.5)")
    testFoldConst("SELECT POWER(NULL, 2)") // NULL base
    testFoldConst("SELECT POWER(2, NULL)") // NULL exponent
    testFoldConst("SELECT POWER(0, 0)") // Zero base, zero exponent
//    COSH/POWER/DEGREES functions would meet inf problem when input is to large, be execute need to fix it
//    testFoldConst("SELECT POWER(0, -1)")
    testFoldConst("SELECT POWER(1E308, 0.5)") // Very large base
//    COSH/POWER/DEGREES functions would meet inf problem when input is to large, be execute need to fix it
//    testFoldConst("SELECT POWER(2, 1E308)")
    testFoldConst("SELECT POWER(1E-308, 2)") // Very small base
    testFoldConst("SELECT POWER(2, -1E308)") // Very small negative exponent

//Ln function cases
    testFoldConst("SELECT LN(1) AS ln_case_1") //ln(1) = 0
    testFoldConst("SELECT LN(EXP(1)) AS ln_case_2") //ln(e) = 1
    testFoldConst("SELECT LN(0.5) AS ln_case_3") //ln(0.5)
    testFoldConst("SELECT ln(1), ln(2.718281828459045), ln(10)")
    testFoldConst("SELECT LN(NULL)") // NULL handling
    testFoldConst("SELECT LN(0)")
    testFoldConst("SELECT LN(-1)")
    testFoldConst("SELECT LN(1E308)") // Very large number
    testFoldConst("SELECT LN(1E-308)") // Very small positive number
    testFoldConst("SELECT LN(2)") // ln(2)
    testFoldConst("SELECT LN(0.1)") // Small decimal
    testFoldConst("SELECT LN(100)") // Larger number

//Log function cases
    testFoldConst("SELECT log(100, 10), log(8, 2), log(1000, 10)")
    testFoldConst("SELECT LOG(NULL, 10)") // NULL number
    testFoldConst("SELECT LOG(100, NULL)") // NULL base
    testFoldConst("SELECT LOG(0, 10)")
    testFoldConst("SELECT LOG(100, 0)")
    testFoldConst("SELECT LOG(100, 1)") // Base 1
    testFoldConst("SELECT LOG(-1, 10)")
    testFoldConst("SELECT LOG(100, -1)")
    testFoldConst("SELECT LOG(1E308, 10)") // Very large number
    testFoldConst("SELECT LOG(100, 1E308)") // Very large base
    testFoldConst("SELECT LOG(1E-308, 10)") // Very small number
    testFoldConst("SELECT LOG(100, 1E-308)") // Very small base

//Log10 function cases
    testFoldConst("SELECT LOG10(100) AS log10_case_1") //log10(100) = 2
    testFoldConst("SELECT LOG10(1) AS log10_case_2") //log10(1) = 0
    testFoldConst("SELECT LOG10(1000) AS log10_case_3") //log10(1000) = 3
    testFoldConst("SELECT LOG10(100.0) AS dlog10_case_1") //dlog10(100.0) = 2
    testFoldConst("SELECT LOG10(1.0) AS dlog10_case_2") //dlog10(1.0) = 0
    testFoldConst("SELECT LOG10(1000.0) AS dlog10_case_3") //dlog10(1000.0)
    testFoldConst("SELECT log10(1), log10(10), log10(100), log10(1000)")
    testFoldConst("SELECT LOG10(NULL)") // NULL handling
    testFoldConst("SELECT LOG10(0)")
    testFoldConst("SELECT LOG10(-1)")
    testFoldConst("SELECT LOG10(1E308)") // Very large number
    testFoldConst("SELECT LOG10(1E-308)") // Very small positive number
    testFoldConst("SELECT LOG10(0.1)") // Decimal less than 1
    testFoldConst("SELECT LOG10(0.01)") // Small decimal

//Log2 function cases
    testFoldConst("SELECT LOG2(2) AS log2_case_1") //log2(2) = 1
    testFoldConst("SELECT LOG2(8) AS log2_case_2") //log2(8) = 3
    testFoldConst("SELECT LOG2(1) AS log2_case_3") //log2(1) = 0
    testFoldConst("SELECT log2(1), log2(2), log2(4), log2(8), log2(16)")
    testFoldConst("SELECT LOG2(NULL)") // NULL handling
    testFoldConst("SELECT LOG2(0)")
    testFoldConst("SELECT LOG2(-1)")
    testFoldConst("SELECT LOG2(1E308)") // Very large number
    testFoldConst("SELECT LOG2(1E-308)") // Very small positive number
    testFoldConst("SELECT LOG2(0.5)") // Fraction
    testFoldConst("SELECT LOG2(32)") // Power of 2
    testFoldConst("SELECT LOG2(1024)") // Larger power of 2

//Money_format function cases
    testFoldConst("SELECT money_format(1234.56), money_format(-1234.56), money_format(0.99), money_format(1234.5678)")
    testFoldConst("SELECT money_format(NULL)") // NULL handling
    testFoldConst("SELECT money_format(0)") // Zero case
    testFoldConst("SELECT money_format(1000000)") // Large number
    testFoldConst("SELECT money_format(0.01)") // Minimum cents
    testFoldConst("SELECT money_format(-0.01)") // Negative minimum cents
    testFoldConst("SELECT money_format(9999999.99)") // Large with cents
    testFoldConst("SELECT money_format(-9999999.99)") // Large negative with cents
    testFoldConst("SELECT money_format(1E308)") // Very large number
    testFoldConst("SELECT money_format(-1E308)") // Very large negative number
    testFoldConst("SELECT money_format(1E-308)") // Very small number
    testFoldConst("SELECT money_format(-1E-308)") // Very small negative number

//Negative function cases
    testFoldConst("SELECT negative(5), negative(-5), negative(0), negative(3.14)")
    testFoldConst("SELECT negative(NULL)") // NULL handling
    testFoldConst("SELECT negative(1E308)") // Very large number
    testFoldConst("SELECT negative(-1E308)") // Very large negative number
    testFoldConst("SELECT negative(1E-308)") // Very small number
    testFoldConst("SELECT negative(-1E-308)") // Very small negative number
    testFoldConst("SELECT negative(9223372036854775807)") // Max bigint
    testFoldConst("SELECT negative(-9223372036854775808)") // Min bigint
    testFoldConst("SELECT negative(2147483647)") // Max int
    testFoldConst("SELECT negative(-2147483648)") // Min int
    testFoldConst("SELECT negative(0.1)") // Small decimal

//Pi function cases
    testFoldConst("SELECT PI() AS pi_case_1") //π = 3.141592653589793
    testFoldConst("SELECT pi()")

//Pmod function cases
    testFoldConst("SELECT MOD(10, 3) AS pmod_case_1") //pmod(10 % 3) = 1
    testFoldConst("SELECT MOD(-10, 3) AS pmod_case_2") //pmod(-10 % 3) = 2 (makes result positive)
    testFoldConst("SELECT MOD(10, -3) AS pmod_case_3") //pmod(10 % -3) = -2 (ensures result is positive)
    testFoldConst("SELECT MOD(10, 0) AS pmod_case_exception") //undefined (returns NULL or error)
    testFoldConst("SELECT pmod(10, 3), pmod(-10, 3), pmod(10, -3), pmod(-10, -3)")

//Positive function cases
    testFoldConst("SELECT positive(5), positive(-5), positive(0), positive(-3.14)")

//Power function cases
    testFoldConst("SELECT POWER(2, 3) AS power_case_1") //2^3 = 8
    testFoldConst("SELECT POWER(10, 0) AS power_case_2") //10^0 = 1
    testFoldConst("SELECT POWER(5, -1) AS power_case_3") //5^-1 = 0.2
    testFoldConst("SELECT POWER(2.0, 3.0) AS dpow_case_1") //dpow(2.0^3.0) = 8.0
    testFoldConst("SELECT POWER(10.0, 0.0) AS dpow_case_2") //dpow(10.0^0.0) = 1.0
    testFoldConst("SELECT POWER(5.0, -1.0) AS dpow_case_3") //dpow(5.0^-1.0) = 0.2
    testFoldConst("SELECT pow(2, 3), power(2, 3), pow(3, 2), power(3, 2), pow(2.5, 1.5)")

//Radians function cases
    testFoldConst("SELECT RADIANS(180) AS radians_case_1") //radians(180) = π
    testFoldConst("SELECT RADIANS(90) AS radians_case_2") //radians(90) = π/2
    testFoldConst("SELECT RADIANS(45) AS radians_case_3") //radians(45)
    testFoldConst("SELECT radians(0), radians(180), radians(360), radians(45)")

//Round function cases
    testFoldConst("SELECT ROUND(3.4) AS round_case_1")
    testFoldConst("SELECT ROUND(3.5) AS round_case_2")
    testFoldConst("SELECT ROUND(-3.4) AS round_case_3")
    testFoldConst("SELECT ROUND(123.456, 2) AS round_case_4") //rounding to 2 decimal places
    testFoldConst("SELECT ROUND(1E308) AS round_case_overflow") //very large number
    testFoldConst("SELECT round(1.23), round(1.58), round(-1.58), round(1.234, 2), round(1.234, 1)")

//Round_bankers function cases
    testFoldConst("SELECT round_bankers(1.5), round_bankers(2.5), round_bankers(-1.5), round_bankers(-2.5)")

//Sign function cases
    testFoldConst("SELECT SIGN(5) AS sign_case_1") //sign(5) = 1
    testFoldConst("SELECT SIGN(-5) AS sign_case_2") //sign(-5) = -1
    testFoldConst("SELECT SIGN(0) AS sign_case_3") //sign(0) = 0
    testFoldConst("SELECT sign(-10), sign(0), sign(10), sign(-3.14), sign(3.14)")

//Sin function cases
    testFoldConst("SELECT SIN(PI() / 2) AS sin_case_1") //sin(π/2) = 1
    testFoldConst("SELECT SIN(0) AS sin_case_2") //sin(0) = 0
    testFoldConst("SELECT SIN(PI()) AS sin_case_3") //sin(π)
    testFoldConst("SELECT SIN(1E308) AS sin_case_overflow")
    testFoldConst("SELECT sin(0), sin(pi()/2), sin(pi()), sin(3*pi()/2)")

//Sqrt function cases
    testFoldConst("SELECT SQRT(16) AS sqrt_case_1") //sqrt(16) = 4
    testFoldConst("SELECT SQRT(0) AS sqrt_case_2") //sqrt(0) = 0
    testFoldConst("SELECT SQRT(2) AS sqrt_case_3") //sqrt(2)

//Tan function cases
    testFoldConst("SELECT TAN(PI() / 4) AS tan_case_1") //tan(π/4) = 1
    testFoldConst("SELECT TAN(0) AS tan_case_2") //tan(0) = 0
    testFoldConst("SELECT TAN(PI()) AS tan_case_3") //tan(π)
    testFoldConst("SELECT TAN(PI() / 2) AS tan_case_exception") //undefined (returns NULL or error)

//Tanh function cases
    testFoldConst("SELECT TANH(0) AS tanh_case_1") //tanh(0) = 0
    testFoldConst("SELECT TANH(1) AS tanh_case_2") //tanh(1)
    testFoldConst("SELECT TANH(-1) AS tanh_case_3") //tanh(-1)

//Truncate function cases
    testFoldConst("SELECT TRUNCATE(123.456, 2) AS truncate_case_1") //truncate(123.456, 2) = 123.45
    testFoldConst("SELECT TRUNCATE(-123.456, 1) AS truncate_case_2") //truncate(-123.456, 1) = -123.4
    testFoldConst("SELECT TRUNCATE(123.456, 0) AS truncate_case_3") //truncate(123.456, 0) = 123
    testFoldConst("SELECT TRUNCATE(123.456, -1) AS truncate_case_exception") //undefined or error

//Xor function cases
    testFoldConst("SELECT 5 ^ 3 AS xor_case_1") //5 XOR 3 = 6
    testFoldConst("SELECT 0 ^ 1 AS xor_case_2") //0 XOR 1 = 1
    testFoldConst("SELECT 255 ^ 128 AS xor_case_3") //255 XOR 128

}
