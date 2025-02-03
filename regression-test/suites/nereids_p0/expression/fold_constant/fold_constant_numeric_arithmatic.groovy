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

    testFoldConst("SELECT truncate(123.456, -1)")
//    testFoldConst("select pmod(-9223372036854775808,-1)")
    //Coalesce function cases
    testFoldConst("SELECT COALESCE(NULL, 5) AS coalesce_case_1")
    testFoldConst("SELECT COALESCE(NULL, NULL, 7) AS coalesce_case_2")
    testFoldConst("SELECT COALESCE(3, 5) AS coalesce_case_3")
    testFoldConst("SELECT COALESCE(NULL, NULL) AS coalesce_case_4")

//Round function cases
    testFoldConst("SELECT ROUND(3.4) AS round_case_1")
    testFoldConst("SELECT ROUND(3.5) AS round_case_2")
    testFoldConst("SELECT ROUND(-3.4) AS round_case_3")
    testFoldConst("SELECT ROUND(123.456, 2) AS round_case_4") //Rounding to 2 decimal places

//Ceil function cases
    testFoldConst("SELECT CEIL(3.4) AS ceil_case_1")
    testFoldConst("SELECT CEIL(-3.4) AS ceil_case_2")
    testFoldConst("SELECT CEIL(5.0) AS ceil_case_3")

//Floor function cases
    testFoldConst("SELECT FLOOR(3.7) AS floor_case_1")
    testFoldConst("SELECT FLOOR(-3.7) AS floor_case_2")
    testFoldConst("SELECT FLOOR(5.0) AS floor_case_3")

//Exp function cases
    testFoldConst("SELECT EXP(1) AS exp_case_1") //e^1
    testFoldConst("SELECT EXP(0) AS exp_case_2") //e^0
    testFoldConst("SELECT EXP(-1) AS exp_case_3") //e^-1

//Ln (natural logarithm) function cases
    testFoldConst("SELECT LN(1) AS ln_case_1")   //ln(1) = 0
    testFoldConst("SELECT LN(EXP(1)) AS ln_case_2") //ln(e) = 1
    testFoldConst("SELECT LN(0.5) AS ln_case_3") //ln(0.5)

//Log10 function cases
    testFoldConst("SELECT LOG10(100) AS log10_case_1") //log10(100) = 2
    testFoldConst("SELECT LOG10(1) AS log10_case_2") //log10(1) = 0
    testFoldConst("SELECT LOG10(1000) AS log10_case_3") //log10(1000) = 3

//Log2 function cases
    testFoldConst("SELECT LOG2(2) AS log2_case_1") //log2(2) = 1
    testFoldConst("SELECT LOG2(8) AS log2_case_2") //log2(8) = 3
    testFoldConst("SELECT LOG2(1) AS log2_case_3") //log2(1) = 0

//Sqrt function cases
    testFoldConst("SELECT SQRT(16) AS sqrt_case_1") //sqrt(16) = 4
    testFoldConst("SELECT SQRT(0) AS sqrt_case_2") //sqrt(0) = 0
    testFoldConst("SELECT SQRT(2) AS sqrt_case_3") //sqrt(2)

//Power function cases
    testFoldConst("SELECT POWER(2, 3) AS power_case_1") //2^3 = 8
    testFoldConst("SELECT POWER(10, 0) AS power_case_2") //10^0 = 1
    testFoldConst("SELECT POWER(5, -1) AS power_case_3") //5^-1 = 0.2

//Sin function cases
    testFoldConst("SELECT SIN(PI() / 2) AS sin_case_1") //sin(π/2) = 1
    testFoldConst("SELECT SIN(0) AS sin_case_2") //sin(0) = 0
    testFoldConst("SELECT SIN(PI()) AS sin_case_3") //sin(π)

//Cos function cases
    testFoldConst("SELECT COS(PI()) AS cos_case_1") //cos(π) = -1
    testFoldConst("SELECT COS(0) AS cos_case_2") //cos(0) = 1
    testFoldConst("SELECT COS(PI() / 2) AS cos_case_3") //cos(π/2)

//Tan function cases
    testFoldConst("SELECT TAN(PI() / 4) AS tan_case_1") //tan(π/4) = 1
    testFoldConst("SELECT TAN(0) AS tan_case_2") //tan(0) = 0
    testFoldConst("SELECT TAN(PI()) AS tan_case_3") //tan(π)

//Acos function cases
    testFoldConst("SELECT ACOS(1) AS acos_case_1") //acos(1) = 0
    testFoldConst("SELECT ACOS(0) AS acos_case_2") //acos(0) = π/2
    testFoldConst("SELECT ACOS(-1) AS acos_case_3") //acos(-1) = π

//Asin function cases
    testFoldConst("SELECT ASIN(1) AS asin_case_1") //asin(1) = π/2
    testFoldConst("SELECT ASIN(0) AS asin_case_2") //asin(0) = 0
    testFoldConst("SELECT ASIN(-1) AS asin_case_3") //asin(-1) = -π/2

//Atan function cases
    testFoldConst("SELECT ATAN(1) AS atan_case_1") //atan(1) = π/4
    testFoldConst("SELECT ATAN(0) AS atan_case_2") //atan(0) = 0
    testFoldConst("SELECT ATAN(-1) AS atan_case_3") //atan(-1)

//Atan2 function cases
    testFoldConst("SELECT ATAN2(1, 1) AS atan2_case_1") //atan2(1, 1) = π/4
    testFoldConst("SELECT ATAN2(0, 1) AS atan2_case_2") //atan2(0, 1) = 0
    testFoldConst("SELECT ATAN2(1, 0) AS atan2_case_3") //atan2(1, 0) = π/2

//Sign function cases
    testFoldConst("SELECT SIGN(5) AS sign_case_1") //sign(5) = 1
    testFoldConst("SELECT SIGN(-5) AS sign_case_2") //sign(-5) = -1
    testFoldConst("SELECT SIGN(0) AS sign_case_3") //sign(0) = 0

//Bin function cases (binary conversion)
    testFoldConst("SELECT BIN(5) AS bin_case_1") //bin(5) = 101
    testFoldConst("SELECT BIN(16) AS bin_case_2") //bin(16) = 10000
    testFoldConst("SELECT BIN(255) AS bin_case_3") //bin(255)

//BitCount function cases (count bits set to 1)
    testFoldConst("SELECT BIT_COUNT(5) AS bitcount_case_1") //bitcount(5) = 2 (101 has two 1s)
    testFoldConst("SELECT BIT_COUNT(16) AS bitcount_case_2") //bitcount(16) = 1
    testFoldConst("SELECT BIT_COUNT(255) AS bitcount_case_3") //bitcount(255)

//BitLength function cases
    testFoldConst("SELECT BIT_LENGTH('5') AS bitlength_case_1") //bitlength(101) = 3
    testFoldConst("SELECT BIT_LENGTH('16') AS bitlength_case_2") //bitlength(10000) = 5
    testFoldConst("SELECT BIT_LENGTH('11111111') AS bitlength_case_3") //bitlength(11111111)

//Cbrt (cube root) function cases
    testFoldConst("SELECT CBRT(8) AS cbrt_case_1") //cbrt(8) = 2
    testFoldConst("SELECT CBRT(-8) AS cbrt_case_2") //cbrt(-8) = -2
//    testFoldConst("SELECT CBRT(27) AS cbrt_case_3") //cbrt(27)

//Cosh function cases (hyperbolic cosine)
    testFoldConst("SELECT COSH(0) AS cosh_case_1") //cosh(0) = 1
//    testFoldConst("SELECT COSH(1) AS cosh_case_2") //cosh(1)
//    testFoldConst("SELECT COSH(-1) AS cosh_case_3") //cosh(-1)

//Tanh function cases (hyperbolic tangent)
    testFoldConst("SELECT TANH(0) AS tanh_case_1") //tanh(0) = 0
    testFoldConst("SELECT TANH(1) AS tanh_case_2") //tanh(1)
    testFoldConst("SELECT TANH(-1) AS tanh_case_3") //tanh(-1)

//Dexp function cases (double exp)
    testFoldConst("SELECT EXP(2.0) AS dexp_case_1") //dexp(2.0)
    testFoldConst("SELECT EXP(0.5) AS dexp_case_2") //dexp(0.5)
    testFoldConst("SELECT EXP(-2.0) AS dexp_case_3") //dexp(-2.0)

//Dlog10 function cases (double log base 10)
    testFoldConst("SELECT LOG10(100.0) AS dlog10_case_1") //dlog10(100.0) = 2
    testFoldConst("SELECT LOG10(1.0) AS dlog10_case_2") //dlog10(1.0) = 0
    testFoldConst("SELECT LOG10(1000.0) AS dlog10_case_3") //dlog10(1000.0)

//Dlog1 function cases (log base 1 is not commonly defined)

//Dpow function cases (double power)
    testFoldConst("SELECT POWER(2.0, 3.0) AS dpow_case_1") //dpow(2.0^3.0) = 8.0
    testFoldConst("SELECT POWER(10.0, 0.0) AS dpow_case_2") //dpow(10.0^0.0) = 1.0
    testFoldConst("SELECT POWER(5.0, -1.0) AS dpow_case_3")
//Coalesce function cases
    testFoldConst("SELECT COALESCE(NULL, 5) AS coalesce_case_1")
    testFoldConst("SELECT COALESCE(NULL, NULL, 7) AS coalesce_case_2")
    testFoldConst("SELECT COALESCE(3, 5) AS coalesce_case_3")
    testFoldConst("SELECT COALESCE(NULL, NULL) AS coalesce_case_4")

//Round function cases
    testFoldConst("SELECT ROUND(3.4) AS round_case_1")
    testFoldConst("SELECT ROUND(3.5) AS round_case_2")
    testFoldConst("SELECT ROUND(-3.4) AS round_case_3")
    testFoldConst("SELECT ROUND(123.456, 2) AS round_case_4") //rounding to 2 decimal places
//Exception: Round overflow (not common but tested with extremely large values)
    testFoldConst("SELECT ROUND(1E308) AS round_case_overflow") //very large number

//Ceil function cases
    testFoldConst("SELECT CEIL(3.4) AS ceil_case_1")
    testFoldConst("SELECT CEIL(-3.4) AS ceil_case_2")
    testFoldConst("SELECT CEIL(5.0) AS ceil_case_3")
//Exception: Ceil of extremely large number
    testFoldConst("SELECT CEIL(1E308) AS ceil_case_overflow")

//Floor function cases
    testFoldConst("SELECT FLOOR(3.7) AS floor_case_1")
    testFoldConst("SELECT FLOOR(-3.7) AS floor_case_2")
    testFoldConst("SELECT FLOOR(5.0) AS floor_case_3")
//Exception: Floor of extremely large number
    testFoldConst("SELECT FLOOR(1E308) AS floor_case_overflow")

//Exp function cases
    testFoldConst("SELECT EXP(1) AS exp_case_1") //e^1
    testFoldConst("SELECT EXP(0) AS exp_case_2") //e^0
    testFoldConst("SELECT EXP(-1) AS exp_case_3") //e^-1

//Ln (natural logarithm) function cases
    testFoldConst("SELECT LN(1) AS ln_case_1") //ln(1) = 0
    testFoldConst("SELECT LN(EXP(1)) AS ln_case_2") //ln(e) = 1
    testFoldConst("SELECT LN(0.5) AS ln_case_3") //ln(0.5)

//Log10 function cases
    testFoldConst("SELECT LOG10(100) AS log10_case_1") //log10(100) = 2
    testFoldConst("SELECT LOG10(1) AS log10_case_2") //log10(1) = 0
    testFoldConst("SELECT LOG10(1000) AS log10_case_3") //log10(1000) = 3

//Sqrt function cases
    testFoldConst("SELECT SQRT(16) AS sqrt_case_1") //sqrt(16) = 4
    testFoldConst("SELECT SQRT(0) AS sqrt_case_2") //sqrt(0) = 0
    testFoldConst("SELECT SQRT(2) AS sqrt_case_3") //sqrt(2)

//Power function cases
    testFoldConst("SELECT POWER(2, 3) AS power_case_1") //2^3 = 8
    testFoldConst("SELECT POWER(10, 0) AS power_case_2") //10^0 = 1
    testFoldConst("SELECT POWER(5, -1) AS power_case_3") //5^-1 = 0.2

//Sin function cases
    testFoldConst("SELECT SIN(PI() / 2) AS sin_case_1") //sin(π/2) = 1
    testFoldConst("SELECT SIN(0) AS sin_case_2") //sin(0) = 0
    testFoldConst("SELECT SIN(PI()) AS sin_case_3") //sin(π)
//Exception: Sin of extremely large number
    testFoldConst("SELECT SIN(1E308) AS sin_case_overflow")

//Cos function cases
    testFoldConst("SELECT COS(PI()) AS cos_case_1") //cos(π) = -1
    testFoldConst("SELECT COS(0) AS cos_case_2") //cos(0) = 1
    testFoldConst("SELECT COS(PI() / 2) AS cos_case_3") //cos(π/2)
//Exception: Cos of extremely large number
    testFoldConst("SELECT COS(1E308) AS cos_case_overflow")

//Tan function cases
    testFoldConst("SELECT TAN(PI() / 4) AS tan_case_1") //tan(π/4) = 1
    testFoldConst("SELECT TAN(0) AS tan_case_2") //tan(0) = 0
    testFoldConst("SELECT TAN(PI()) AS tan_case_3") //tan(π)
//Exception: Tan of extremely large number (undefined for multiples of π/2)
    testFoldConst("SELECT TAN(PI() / 2) AS tan_case_exception") //undefined (returns NULL or error)

//Acos function cases
    testFoldConst("SELECT ACOS(1) AS acos_case_1") //acos(1) = 0
    testFoldConst("SELECT ACOS(0) AS acos_case_2") //acos(0) = π/2
    testFoldConst("SELECT ACOS(-1) AS acos_case_3") //acos(-1) = π

//Asin function cases
    testFoldConst("SELECT ASIN(1) AS asin_case_1") //asin(1) = π/2
    testFoldConst("SELECT ASIN(0) AS asin_case_2") //asin(0) = 0
    testFoldConst("SELECT ASIN(-1) AS asin_case_3") //asin(-1) = -π/2

//Atan function cases
    testFoldConst("SELECT ATAN(1) AS atan_case_1") //atan(1) = π/4
    testFoldConst("SELECT ATAN(0) AS atan_case_2") //atan(0) = 0
    testFoldConst("SELECT ATAN(-1) AS atan_case_3") //atan(-1)
//No exceptions for Atan, defined for all real numbers

//Atan2 function cases
    testFoldConst("SELECT ATAN2(1, 1) AS atan2_case_1") //atan2(1, 1) = π/4
    testFoldConst("SELECT ATAN2(0, 1) AS atan2_case_2") //atan2(0, 1) = 0
    testFoldConst("SELECT ATAN2(1, 0) AS atan2_case_3") //atan2(1, 0) = π/2
//Exception: Atan2(0, 0) is undefined
    testFoldConst("SELECT ATAN2(0, 0) AS atan2_case_exception") //undefined (returns NULL or error)

//Sign function cases
    testFoldConst("SELECT SIGN(5) AS sign_case_1") //sign(5) = 1
    testFoldConst("SELECT SIGN(-5) AS sign_case_2") //sign(-5) = -1
    testFoldConst("SELECT SIGN(0) AS sign_case_3") //sign(0) = 0
//No exceptions for Sign

//Bin function cases
    testFoldConst("SELECT BIN(5) AS bin_case_1") //bin(5) = 101
    testFoldConst("SELECT BIN(16) AS bin_case_2") //bin(16) = 10000
    testFoldConst("SELECT BIN(255) AS bin_case_3") //bin(255)
//Exception: Bin of negative values (may not be defined in all DBs)
    testFoldConst("SELECT BIN(-1) AS bin_case_exception") //returns NULL or error in some databases

//BitCount function cases (count bits set to 1)
    testFoldConst("SELECT BIT_COUNT(5) AS bitcount_case_1") //bitcount(5) = 2 (101 has two 1s)
    testFoldConst("SELECT BIT_COUNT(16) AS bitcount_case_2")
//BitCount function cases (count bits set to 1)
    testFoldConst("SELECT BIT_COUNT(5) AS bitcount_case_1") //bitcount(5) = 2 (101 has two 1s)
    testFoldConst("SELECT BIT_COUNT(16) AS bitcount_case_2") //bitcount(16) = 1
    testFoldConst("SELECT BIT_COUNT(255) AS bitcount_case_3") //bitcount(255) = 8
//Exception: BitCount of negative values
    testFoldConst("SELECT BIT_COUNT(-1) AS bitcount_case_exception") //result depends on system's interpretation of negative values

//Cbrt (cube root) function cases
    testFoldConst("SELECT CBRT(8) AS cbrt_case_1") //cbrt(8) = 2
    testFoldConst("SELECT CBRT(-8) AS cbrt_case_2") //cbrt(-8) = -2

//Cosh function cases (hyperbolic cosine)
    testFoldConst("SELECT COSH(0) AS cosh_case_1") //cosh(0) = 1
//        testFoldConst("SELECT COSH(1) AS cosh_case_2") //cosh(1)
//        testFoldConst("SELECT COSH(-1) AS cosh_case_3") //cosh(-1)
//Exception: Overflow on large input
//        testFoldConst("SELECT COSH(1E308) AS cosh_case_overflow")

//Tanh function cases (hyperbolic tangent)
    testFoldConst("SELECT TANH(0) AS tanh_case_1") //tanh(0) = 0
    testFoldConst("SELECT TANH(1) AS tanh_case_2") //tanh(1)
    testFoldConst("SELECT TANH(-1) AS tanh_case_3") //tanh(-1)
//No exception cases for Tanh as it's defined for all real numbers

//Dexp function cases (double exp)
    testFoldConst("SELECT EXP(2.0) AS dexp_case_1") //dexp(2.0)
    testFoldConst("SELECT EXP(0.5) AS dexp_case_2") //dexp(0.5)
    testFoldConst("SELECT EXP(-2.0) AS dexp_case_3") //dexp(-2.0)

//Dlog10 function cases (double log base 10)
    testFoldConst("SELECT LOG10(100.0) AS dlog10_case_1") //dlog10(100.0) = 2
    testFoldConst("SELECT LOG10(1.0) AS dlog10_case_2") //dlog10(1.0) = 0
    testFoldConst("SELECT LOG10(1000.0) AS dlog10_case_3") //dlog10(1000.0)

//Dpow function cases (double power)
    testFoldConst("SELECT POWER(2.0, 3.0) AS dpow_case_1") //dpow(2.0^3.0) = 8.0
    testFoldConst("SELECT POWER(10.0, 0.0) AS dpow_case_2") //dpow(10.0^0.0) = 1.0
    testFoldConst("SELECT POWER(5.0, -1.0) AS dpow_case_3") //dpow(5.0^-1.0) = 0.2

//Dsqrt function cases (double sqrt)
    testFoldConst("SELECT SQRT(16.0) AS dsqrt_case_1") //sqrt(16.0) = 4
    testFoldConst("SELECT SQRT(0.0) AS dsqrt_case_2") //sqrt(0.0) = 0
    testFoldConst("SELECT SQRT(2.0) AS dsqrt_case_3") //sqrt(2.0)

//Fmod function cases (floating-point modulus)
    testFoldConst("SELECT MOD(10.5, 3.2) AS fmod_case_1") //fmod(10.5 % 3.2)
    testFoldConst("SELECT MOD(-10.5, 3.2) AS fmod_case_2") //fmod(-10.5 % 3.2)
    testFoldConst("SELECT MOD(10.5, -3.2) AS fmod_case_3") //fmod(10.5 % -3.2)
//Exception: Division by zero in modulus
    testFoldConst("SELECT MOD(10.5, 0) AS fmod_case_exception") //undefined (returns NULL or error)

//Fpow function cases (floating-point power)
    testFoldConst("SELECT POWER(2.5, 3.2) AS fpow_case_1") //fpow(2.5^3.2)
    testFoldConst("SELECT POWER(10.0, 0.0) AS fpow_case_2") //fpow(10.0^0.0) = 1.0
    testFoldConst("SELECT POWER(5.5, -1.2) AS fpow_case_3") //fpow(5.5^-1.2)

//Radians function cases (degrees to radians)
    testFoldConst("SELECT RADIANS(180) AS radians_case_1") //radians(180) = π
    testFoldConst("SELECT RADIANS(90) AS radians_case_2") //radians(90) = π/2
    testFoldConst("SELECT RADIANS(45) AS radians_case_3") //radians(45)
//No exception cases for Radians

//Degrees function cases (radians to degrees)
    testFoldConst("SELECT DEGREES(PI()) AS degrees_case_1") //degrees(π) = 180
    testFoldConst("SELECT DEGREES(PI()/2) AS degrees_case_2") //degrees(π/2) = 90
    testFoldConst("SELECT DEGREES(PI()/4) AS degrees_case_3") //degrees(π/4)
//No exception cases for Degrees

//Xor function cases (bitwise XOR)
    testFoldConst("SELECT 5 ^ 3 AS xor_case_1") //5 XOR 3 = 6
    testFoldConst("SELECT 0 ^ 1 AS xor_case_2") //0 XOR 1 = 1
    testFoldConst("SELECT 255 ^ 128 AS xor_case_3") //255 XOR 128
//Exception: XOR on non-integer types (if applicable)
test {
    sql "SELECT 'a' ^ 1 AS xor_case_exception"
    exception("string literal 'a' cannot be cast to double")
}

//Pi function cases
    testFoldConst("SELECT PI() AS pi_case_1") //π = 3.141592653589793
//No exception cases for Pi

//E function cases
    testFoldConst("SELECT EXP(1) AS e_case_1") //e = 2.718281828459045
//No exception cases for E

//Conv function cases (convert from one base to another)
    testFoldConst("SELECT CONV(15, 10, 2) AS conv_case_1") //conv(15 from base 10 to base 2) = 1111
    testFoldConst("SELECT CONV(1111, 2, 10) AS conv_case_2") //conv(1111 from base 2 to base 10) = 15
    testFoldConst("SELECT CONV(255, 10, 16) AS conv_case_3") //conv(255 from base 10 to base 16) = FF
//Exception: Conv with invalid bases or negative values
    testFoldConst("SELECT CONV(-10, 10, 2) AS conv_case_exception") //undefined or error

//Truncate function cases
    testFoldConst("SELECT TRUNCATE(123.456, 2) AS truncate_case_1") //truncate(123.456, 2) = 123.45
    testFoldConst("SELECT TRUNCATE(-123.456, 1) AS truncate_case_2") //truncate(-123.456, 1) = -123.4
    testFoldConst("SELECT TRUNCATE(123.456, 0) AS truncate_case_3") //truncate(123.456, 0) = 123
//Exception: Truncate with negative decimal places
    testFoldConst("SELECT TRUNCATE(123.456, -1) AS truncate_case_exception") //undefined or error

//CountEqual function cases
    testFoldConst("SELECT COUNT(CASE WHEN 5 = 5 THEN 1 END) AS countequal_case_1") //1 (true)
    testFoldConst("SELECT COUNT(CASE WHEN 5 = 3 THEN 1 END) AS countequal_case_2") //0 (false)
//Exception: Undefined operation
    testFoldConst("SELECT COUNT(CASE WHEN 'a' = 1 THEN 1 END) AS countequal_case_exception") //undefined or error

//Pmod function cases (positive
//Pmod function cases (positive modulus)
    testFoldConst("SELECT MOD(10, 3) AS pmod_case_1") //pmod(10 % 3) = 1
    testFoldConst("SELECT MOD(-10, 3) AS pmod_case_2") //pmod(-10 % 3) = 2 (makes result positive)
    testFoldConst("SELECT MOD(10, -3) AS pmod_case_3") //pmod(10 % -3) = -2 (ensures result is positive)
//Exception: Division by zero in modulus
    testFoldConst("SELECT MOD(10, 0) AS pmod_case_exception") //undefined (returns NULL or error)

//Summary of Edge Cases and Overflows
//Round, Ceil, Floor with extremely large values
    testFoldConst("SELECT ROUND(1E308) AS round_large_value")
    testFoldConst("SELECT CEIL(1E308) AS ceil_large_value")
    testFoldConst("SELECT FLOOR(1E308) AS floor_large_value")

//Trigonometric functions with large inputs or boundary conditions
    testFoldConst("SELECT SIN(1E308) AS sin_large_value") //Sin overflow
    testFoldConst("SELECT COS(1E308) AS cos_large_value") //Cos overflow
    testFoldConst("SELECT TAN(PI() / 2) AS tan_undefined") //Undefined for tan(π/2)

//Miscellaneous operations like bit manipulations and modulo on edge cases
    testFoldConst("SELECT BIN(-1) AS bin_negative_value") //Bin of negative number (may be undefined)
    testFoldConst("SELECT BIT_COUNT(-1) AS bitcount_negative_value") //BitCount of negative number
    testFoldConst("SELECT MOD(10.5, 0) AS fmod_divide_by_zero") //Modulo by zero (undefined)
    testFoldConst("SELECT TRUNCATE(123.456, -1) AS truncate_negative_decimals") //Truncate with negative decimals

//Additional cases for Xor, Conv, and other mathematical functions
    testFoldConst("SELECT CONV(-10, 10, 2) AS conv_invalid_base") //Conv with negative input (may be undefined)

    // fix floor/ceil/round function return type with DecimalV3 input
    testFoldConst("with cte as (select floor(300.343) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select round(300.343) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select ceil(300.343) order by 1 limit 1) select * from cte")

    testFoldConst("with cte as (select floor(300.343, 2) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select round(300.343, 2) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select ceil(300.343, 2) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select truncate(300.343, 2) order by 1 limit 1) select * from cte")

    testFoldConst("with cte as (select floor(300.343, 0) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select round(300.343, 0) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select ceil(300.343, 0) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select truncate(300.343, 0) order by 1 limit 1) select * from cte")

    testFoldConst("with cte as (select floor(300.343, -1) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select round(300.343, -1) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select ceil(300.343, -1) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select truncate(300.343, -1) order by 1 limit 1) select * from cte")

    testFoldConst("with cte as (select floor(300.343, -4) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select round(300.343, -4) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select ceil(300.343, -4) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select truncate(300.343, -4) order by 1 limit 1) select * from cte")

    testFoldConst("with cte as (select floor(3) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select round(3) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select ceil(3) order by 1 limit 1) select * from cte")

    testFoldConst("with cte as (select floor(3, 2) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select round(3, 2) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select ceil(3, 2) order by 1 limit 1) select * from cte")
    testFoldConst("with cte as (select truncate(3, 2) order by 1 limit 1) select * from cte")
}
