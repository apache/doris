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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/exprs/math-functions.cpp
// and modified by Doris

#include "exprs/math_functions.h"

#include <stdlib.h>

#include <cmath>
#include <iomanip>
#include <random>
#include <sstream>

#include "common/compiler_util.h"
#include "exprs/anyval_util.h"
#include "runtime/decimalv2_value.h"
#include "util/simd/vstring_function.h"
#include "util/string_parser.hpp"

namespace doris {

const char* MathFunctions::_s_alphanumeric_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

const double log_10[] = {
        1e000, 1e001, 1e002, 1e003, 1e004, 1e005, 1e006, 1e007, 1e008, 1e009, 1e010, 1e011, 1e012,
        1e013, 1e014, 1e015, 1e016, 1e017, 1e018, 1e019, 1e020, 1e021, 1e022, 1e023, 1e024, 1e025,
        1e026, 1e027, 1e028, 1e029, 1e030, 1e031, 1e032, 1e033, 1e034, 1e035, 1e036, 1e037, 1e038,
        1e039, 1e040, 1e041, 1e042, 1e043, 1e044, 1e045, 1e046, 1e047, 1e048, 1e049, 1e050, 1e051,
        1e052, 1e053, 1e054, 1e055, 1e056, 1e057, 1e058, 1e059, 1e060, 1e061, 1e062, 1e063, 1e064,
        1e065, 1e066, 1e067, 1e068, 1e069, 1e070, 1e071, 1e072, 1e073, 1e074, 1e075, 1e076, 1e077,
        1e078, 1e079, 1e080, 1e081, 1e082, 1e083, 1e084, 1e085, 1e086, 1e087, 1e088, 1e089, 1e090,
        1e091, 1e092, 1e093, 1e094, 1e095, 1e096, 1e097, 1e098, 1e099, 1e100, 1e101, 1e102, 1e103,
        1e104, 1e105, 1e106, 1e107, 1e108, 1e109, 1e110, 1e111, 1e112, 1e113, 1e114, 1e115, 1e116,
        1e117, 1e118, 1e119, 1e120, 1e121, 1e122, 1e123, 1e124, 1e125, 1e126, 1e127, 1e128, 1e129,
        1e130, 1e131, 1e132, 1e133, 1e134, 1e135, 1e136, 1e137, 1e138, 1e139, 1e140, 1e141, 1e142,
        1e143, 1e144, 1e145, 1e146, 1e147, 1e148, 1e149, 1e150, 1e151, 1e152, 1e153, 1e154, 1e155,
        1e156, 1e157, 1e158, 1e159, 1e160, 1e161, 1e162, 1e163, 1e164, 1e165, 1e166, 1e167, 1e168,
        1e169, 1e170, 1e171, 1e172, 1e173, 1e174, 1e175, 1e176, 1e177, 1e178, 1e179, 1e180, 1e181,
        1e182, 1e183, 1e184, 1e185, 1e186, 1e187, 1e188, 1e189, 1e190, 1e191, 1e192, 1e193, 1e194,
        1e195, 1e196, 1e197, 1e198, 1e199, 1e200, 1e201, 1e202, 1e203, 1e204, 1e205, 1e206, 1e207,
        1e208, 1e209, 1e210, 1e211, 1e212, 1e213, 1e214, 1e215, 1e216, 1e217, 1e218, 1e219, 1e220,
        1e221, 1e222, 1e223, 1e224, 1e225, 1e226, 1e227, 1e228, 1e229, 1e230, 1e231, 1e232, 1e233,
        1e234, 1e235, 1e236, 1e237, 1e238, 1e239, 1e240, 1e241, 1e242, 1e243, 1e244, 1e245, 1e246,
        1e247, 1e248, 1e249, 1e250, 1e251, 1e252, 1e253, 1e254, 1e255, 1e256, 1e257, 1e258, 1e259,
        1e260, 1e261, 1e262, 1e263, 1e264, 1e265, 1e266, 1e267, 1e268, 1e269, 1e270, 1e271, 1e272,
        1e273, 1e274, 1e275, 1e276, 1e277, 1e278, 1e279, 1e280, 1e281, 1e282, 1e283, 1e284, 1e285,
        1e286, 1e287, 1e288, 1e289, 1e290, 1e291, 1e292, 1e293, 1e294, 1e295, 1e296, 1e297, 1e298,
        1e299, 1e300, 1e301, 1e302, 1e303, 1e304, 1e305, 1e306, 1e307, 1e308};

#define ARRAY_ELEMENTS(A) ((uint64_t)(sizeof(A) / sizeof(A[0])))

double MathFunctions::my_double_round(double value, int64_t dec, bool dec_unsigned, bool truncate) {
    bool dec_negative = (dec < 0) && !dec_unsigned;
    uint64_t abs_dec = dec_negative ? -dec : dec;
    /*
       tmp2 is here to avoid return the value with 80 bit precision
       This will fix that the test round(0.1,1) = round(0.1,1) is true
       Tagging with volatile is no guarantee, it may still be optimized away...
       */
    volatile double tmp2 = 0.0;

    double tmp =
            (abs_dec < ARRAY_ELEMENTS(log_10) ? log_10[abs_dec] : std::pow(10.0, (double)abs_dec));

    // Pre-compute these, to avoid optimizing away e.g. 'floor(v/tmp) * tmp'.
    volatile double value_div_tmp = value / tmp;
    volatile double value_mul_tmp = value * tmp;

    if (dec_negative && std::isinf(tmp)) {
        tmp2 = 0.0;
    } else if (!dec_negative && std::isinf(value_mul_tmp)) {
        tmp2 = value;
    } else if (truncate) {
        if (value >= 0.0) {
            tmp2 = dec < 0 ? std::floor(value_div_tmp) * tmp : std::floor(value_mul_tmp) / tmp;
        } else {
            tmp2 = dec < 0 ? std::ceil(value_div_tmp) * tmp : std::ceil(value_mul_tmp) / tmp;
        }
    } else {
        tmp2 = dec < 0 ? std::round(value_div_tmp) * tmp : std::round(value_mul_tmp) / tmp;
    }

    return tmp2;
}

void MathFunctions::init() {}

DoubleVal MathFunctions::pi(FunctionContext* ctx) {
    return DoubleVal(M_PI);
}

DoubleVal MathFunctions::e(FunctionContext* ctx) {
    return DoubleVal(M_E);
}

// libc++ did not have std::abs for int128
__int128_t largeint_abs(__int128_t x) {
    return x > 0 ? x : -x;
}

DecimalV2Val MathFunctions::abs(FunctionContext* ctx, const doris_udf::DecimalV2Val& val) {
    if (val.is_null) {
        return DecimalV2Val::null();
    }
    if (UNLIKELY(val.val == MIN_INT128)) {
        return DecimalV2Val::null();
    } else {
        return DecimalV2Val(largeint_abs(val.val));
    }
}

LargeIntVal MathFunctions::abs(FunctionContext* ctx, const doris_udf::LargeIntVal& val) {
    if (val.is_null) {
        return LargeIntVal::null();
    }
    if (UNLIKELY(val.val == MIN_INT128)) {
        return LargeIntVal::null();
    } else {
        return LargeIntVal(largeint_abs(val.val));
    }
}

LargeIntVal MathFunctions::abs(FunctionContext* ctx, const doris_udf::BigIntVal& val) {
    if (val.is_null) {
        return LargeIntVal::null();
    }
    return LargeIntVal(largeint_abs(__int128(val.val)));
}

BigIntVal MathFunctions::abs(FunctionContext* ctx, const doris_udf::IntVal& val) {
    if (val.is_null) {
        return BigIntVal::null();
    }
    return BigIntVal(std::abs(int64_t(val.val)));
}

IntVal MathFunctions::abs(FunctionContext* ctx, const doris_udf::SmallIntVal& val) {
    if (val.is_null) {
        return IntVal::null();
    }
    return IntVal(std::abs(int32_t(val.val)));
}

SmallIntVal MathFunctions::abs(FunctionContext* ctx, const doris_udf::TinyIntVal& val) {
    if (val.is_null) {
        return SmallIntVal::null();
    }
    return SmallIntVal(std::abs(int16_t(val.val)));
}

#define LOG_MATH_FN(NAME, RET_TYPE, INPUT_TYPE, FN)                           \
    RET_TYPE MathFunctions::NAME(FunctionContext* ctx, const INPUT_TYPE& v) { \
        if (v.is_null || v.val <= 0) return RET_TYPE::null();                 \
        return RET_TYPE(FN(v.val));                                           \
    }

// Generates a UDF that always calls FN() on the input val and returns it.
#define ONE_ARG_MATH_FN(NAME, RET_TYPE, INPUT_TYPE, FN)                       \
    RET_TYPE MathFunctions::NAME(FunctionContext* ctx, const INPUT_TYPE& v) { \
        if (v.is_null) return RET_TYPE::null();                               \
        return RET_TYPE(FN(v.val));                                           \
    }

ONE_ARG_MATH_FN(abs, DoubleVal, DoubleVal, std::fabs);
ONE_ARG_MATH_FN(abs, FloatVal, FloatVal, std::fabs);
ONE_ARG_MATH_FN(sin, DoubleVal, DoubleVal, std::sin);
ONE_ARG_MATH_FN(asin, DoubleVal, DoubleVal, std::asin);
ONE_ARG_MATH_FN(cos, DoubleVal, DoubleVal, std::cos);
ONE_ARG_MATH_FN(acos, DoubleVal, DoubleVal, std::acos);
ONE_ARG_MATH_FN(tan, DoubleVal, DoubleVal, std::tan);
ONE_ARG_MATH_FN(atan, DoubleVal, DoubleVal, std::atan);
ONE_ARG_MATH_FN(sqrt, DoubleVal, DoubleVal, std::sqrt);
ONE_ARG_MATH_FN(ceil, BigIntVal, DoubleVal, std::ceil);
ONE_ARG_MATH_FN(floor, BigIntVal, DoubleVal, std::floor);
ONE_ARG_MATH_FN(exp, DoubleVal, DoubleVal, std::exp);
LOG_MATH_FN(ln, DoubleVal, DoubleVal, std::log);
LOG_MATH_FN(log10, DoubleVal, DoubleVal, std::log10);

TinyIntVal MathFunctions::sign(FunctionContext* ctx, const DoubleVal& v) {
    if (v.is_null) {
        return TinyIntVal::null();
    }
    return TinyIntVal((v.val > 0) ? 1 : ((v.val < 0) ? -1 : 0));
}

DoubleVal MathFunctions::radians(FunctionContext* ctx, const DoubleVal& v) {
    if (v.is_null) {
        return v;
    }
    return DoubleVal(v.val * M_PI / 180.0);
}

DoubleVal MathFunctions::degrees(FunctionContext* ctx, const DoubleVal& v) {
    if (v.is_null) {
        return v;
    }
    return DoubleVal(v.val * 180.0 / M_PI);
}

BigIntVal MathFunctions::round(FunctionContext* ctx, const DoubleVal& v) {
    if (v.is_null) {
        return BigIntVal::null();
    }
    return BigIntVal(static_cast<int64_t>(v.val + ((v.val < 0) ? -0.5 : 0.5)));
}

DoubleVal MathFunctions::round_up_to(FunctionContext* ctx, const DoubleVal& v,
                                     const IntVal& scale) {
    if (v.is_null || scale.is_null) {
        return DoubleVal::null();
    }
    return DoubleVal(my_double_round(v.val, scale.val, false, false));
}

DoubleVal MathFunctions::truncate(FunctionContext* ctx, const DoubleVal& v, const IntVal& scale) {
    if (v.is_null || scale.is_null) {
        return DoubleVal::null();
    }
    return DoubleVal(my_double_round(v.val, scale.val, false, true));
}

DoubleVal MathFunctions::log2(FunctionContext* ctx, const DoubleVal& v) {
    if (v.is_null || v.val <= 0.0) {
        return DoubleVal::null();
    }
    return DoubleVal(std::log(v.val) / std::log(2.0));
}

const double EPSILON = 1e-9;
DoubleVal MathFunctions::log(FunctionContext* ctx, const DoubleVal& base, const DoubleVal& v) {
    if (base.is_null || v.is_null) {
        return DoubleVal::null();
    }
    if (base.val <= 0 || std::fabs(base.val - 1.0) < EPSILON || v.val <= 0.0) {
        return DoubleVal::null();
    }

    return DoubleVal(std::log(v.val) / std::log(base.val));
}

DoubleVal MathFunctions::pow(FunctionContext* ctx, const DoubleVal& base, const DoubleVal& exp) {
    if (base.is_null || exp.is_null) {
        return DoubleVal::null();
    }
    return DoubleVal(std::pow(base.val, exp.val));
}

void MathFunctions::rand_prepare(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    std::mt19937* generator = reinterpret_cast<std::mt19937*>(ctx->allocate(sizeof(std::mt19937)));
    if (UNLIKELY(generator == nullptr)) {
        LOG(ERROR) << "allocate random seed generator failed.";
        return;
    }
    ctx->set_function_state(scope, generator);
    new (generator) std::mt19937();
    if (scope == FunctionContext::THREAD_LOCAL) {
        if (ctx->get_num_args() == 1) {
            uint32_t seed = 0;
            // This is a call to RandSeed, initialize the seed
            // TODO: should we support non-constant seed?
            if (!ctx->is_arg_constant(0)) {
                ctx->set_error("Seed argument to rand() must be constant");
                return;
            }
            BigIntVal* seed_arg = static_cast<BigIntVal*>(ctx->get_constant_arg(0));
            if (!seed_arg->is_null) {
                seed = seed_arg->val;
            }
            generator->seed(seed);
        } else {
            generator->seed(std::random_device()());
        }
    }
}

DoubleVal MathFunctions::rand(FunctionContext* ctx) {
    std::mt19937* generator =
            reinterpret_cast<std::mt19937*>(ctx->get_function_state(FunctionContext::THREAD_LOCAL));
    DCHECK(generator != nullptr);
    static const double min = 0.0;
    static const double max = 1.0;
    std::uniform_real_distribution<double> distribution(min, max);
    return DoubleVal(distribution(*generator));
}

DoubleVal MathFunctions::rand_seed(FunctionContext* ctx, const BigIntVal& seed) {
    if (seed.is_null) {
        return DoubleVal::null();
    }
    return rand(ctx);
}

void MathFunctions::rand_close(FunctionContext* ctx, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::THREAD_LOCAL) {
        uint8_t* generator =
                reinterpret_cast<uint8_t*>(ctx->get_function_state(FunctionContext::THREAD_LOCAL));
        ctx->free(generator);
        ctx->set_function_state(FunctionContext::THREAD_LOCAL, nullptr);
    }
}

StringVal MathFunctions::bin(FunctionContext* ctx, const BigIntVal& v) {
    if (v.is_null) {
        return StringVal::null();
    }
    // Cast to an unsigned integer because it is compiler dependent
    // whether the sign bit will be shifted like a regular bit.
    // (logical vs. arithmetic shift for signed numbers)
    uint64_t n = static_cast<uint64_t>(v.val);
    const size_t max_bits = sizeof(uint64_t) * 8;
    char result[max_bits];
    uint32_t index = max_bits;
    do {
        result[--index] = '0' + (n & 1);
    } while (n >>= 1);
    return AnyValUtil::from_buffer_temp(ctx, result + index, max_bits - index);
}

StringVal MathFunctions::hex_int(FunctionContext* ctx, const BigIntVal& v) {
    if (v.is_null) {
        return StringVal::null();
    }

    uint64_t num = v.val;
    if (num == 0) {
        return AnyValUtil::from_string_temp(ctx, "0");
    }
    char hex[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
    // uint64_t max value 0xFFFFFFFFFFFFFFFF , 16 'F'
    // need 1 more space for '\0'
    char ans[17];
    int i = 0;
    while (num) {
        ans[i++] = hex[num & 15];
        num = num >> 4;
    }
    ans[i] = '\0';
    // reverse
    for (int k = 0, j = i - 1; k <= j; k++, j--) {
        char tmp = ans[j];
        ans[j] = ans[k];
        ans[k] = tmp;
    }
    return AnyValUtil::from_string_temp(ctx, ans);
}

StringVal MathFunctions::hex_string(FunctionContext* ctx, const StringVal& s) {
    if (s.is_null) {
        return StringVal::null();
    }

    StringVal result = StringVal::create_temp_string_val(ctx, s.len * 2);
    simd::VStringFunctions::hex_encode(s.ptr, s.len, reinterpret_cast<char*>(result.ptr));
    return result;
}

StringVal MathFunctions::unhex(FunctionContext* ctx, const StringVal& s) {
    if (s.is_null) {
        return StringVal::null();
    }
    // For odd number of chars return empty string like Hive does.
    if (s.len & 1) {
        return StringVal();
    }

    int result_len = s.len / 2;
    char result[result_len];
    int res_index = 0;
    int s_index = 0;
    while (s_index < s.len) {
        char c = 0;
        for (int j = 0; j < 2; ++j, ++s_index) {
            switch (s.ptr[s_index]) {
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                c += (s.ptr[s_index] - '0') * ((j == 0) ? 16 : 1);
                break;
            case 'A':
            case 'B':
            case 'C':
            case 'D':
            case 'E':
            case 'F':
                // Map to decimal values [10, 15]
                c += (s.ptr[s_index] - 'A' + 10) * ((j == 0) ? 16 : 1);
                break;
            case 'a':
            case 'b':
            case 'c':
            case 'd':
            case 'e':
            case 'f':
                // Map to decimal [10, 15]
                c += (s.ptr[s_index] - 'a' + 10) * ((j == 0) ? 16 : 1);
                break;
            default:
                // Character not in hex alphabet, return empty string.
                return StringVal();
            }
        }
        result[res_index] = c;
        ++res_index;
    }
    return AnyValUtil::from_buffer_temp(ctx, result, result_len);
}

StringVal MathFunctions::conv_int(FunctionContext* ctx, const BigIntVal& num,
                                  const TinyIntVal& src_base, const TinyIntVal& dest_base) {
    if (num.is_null || src_base.is_null || dest_base.is_null) {
        return StringVal::null();
    }
    // As in MySQL and Hive, min base is 2 and max base is 36.
    // (36 is max base representable by alphanumeric chars)
    // If a negative target base is given, num should be interpreted in 2's complement.
    if (std::abs(src_base.val) < MIN_BASE || std::abs(src_base.val) > MAX_BASE ||
        std::abs(dest_base.val) < MIN_BASE || std::abs(dest_base.val) > MAX_BASE) {
        // Return nullptr like Hive does.
        return StringVal::null();
    }
    // Invalid input.
    if (src_base.val < 0 && num.val >= 0) {
        return StringVal::null();
    }
    int64_t decimal_num = num.val;
    if (src_base.val != 10) {
        // Convert src_num representing a number in src_base but encoded in decimal
        // into its actual decimal number.
        if (!decimal_in_base_to_decimal(num.val, src_base.val, &decimal_num)) {
            // Handle overflow, setting decimal_num appropriately.
            handle_parse_result(dest_base.val, &decimal_num, StringParser::PARSE_OVERFLOW);
        }
    }
    return decimal_to_base(ctx, decimal_num, dest_base.val);
}

StringVal MathFunctions::conv_string(FunctionContext* ctx, const StringVal& num_str,
                                     const TinyIntVal& src_base, const TinyIntVal& dest_base) {
    if (num_str.is_null || src_base.is_null || dest_base.is_null) {
        return StringVal::null();
    }
    // As in MySQL and Hive, min base is 2 and max base is 36.
    // (36 is max base representable by alphanumeric chars)
    // If a negative target base is given, num should be interpreted in 2's complement.
    if (std::abs(src_base.val) < MIN_BASE || std::abs(src_base.val) > MAX_BASE ||
        std::abs(dest_base.val) < MIN_BASE || std::abs(dest_base.val) > MAX_BASE) {
        // Return nullptr like Hive does.
        return StringVal::null();
    }
    // Convert digits in num_str in src_base to decimal.
    StringParser::ParseResult parse_res;
    int64_t decimal_num = StringParser::string_to_int<int64_t>(
            reinterpret_cast<char*>(num_str.ptr), num_str.len, src_base.val, &parse_res);
    if (src_base.val < 0 && decimal_num >= 0) {
        // Invalid input.
        return StringVal::null();
    }
    if (!handle_parse_result(dest_base.val, &decimal_num, parse_res)) {
        // Return 0 for invalid input strings like Hive does.
        return StringVal(reinterpret_cast<uint8_t*>(const_cast<char*>("0")), 1);
    }
    return decimal_to_base(ctx, decimal_num, dest_base.val);
}

StringVal MathFunctions::decimal_to_base(FunctionContext* ctx, int64_t src_num, int8_t dest_base) {
    // Max number of digits of any base (base 2 gives max digits), plus sign.
    const size_t max_digits = sizeof(uint64_t) * 8 + 1;
    char buf[max_digits];
    int32_t result_len = 0;
    int32_t buf_index = max_digits - 1;
    uint64_t temp_num;
    if (dest_base < 0) {
        // Dest base is negative, treat src_num as signed.
        temp_num = std::abs(src_num);
    } else {
        // Dest base is positive. We must interpret src_num in 2's complement.
        // Convert to an unsigned int to properly deal with 2's complement conversion.
        temp_num = static_cast<uint64_t>(src_num);
    }
    int abs_base = std::abs(dest_base);
    do {
        buf[buf_index] = _s_alphanumeric_chars[temp_num % abs_base];
        temp_num /= abs_base;
        --buf_index;
        ++result_len;
    } while (temp_num > 0);
    // Add optional sign.
    if (src_num < 0 && dest_base < 0) {
        buf[buf_index] = '-';
        ++result_len;
    }
    return AnyValUtil::from_buffer_temp(ctx, buf + max_digits - result_len, result_len);
}

bool MathFunctions::decimal_in_base_to_decimal(int64_t src_num, int8_t src_base, int64_t* result) {
    uint64_t temp_num = std::abs(src_num);
    int32_t place = 1;
    *result = 0;
    do {
        int32_t digit = temp_num % 10;
        // Reset result if digit is not representable in src_base.
        if (digit >= src_base) {
            *result = 0;
            place = 1;
        } else {
            *result += digit * place;
            place *= src_base;
            // Overflow.
            if (UNLIKELY(*result < digit)) {
                return false;
            }
        }
        temp_num /= 10;
    } while (temp_num > 0);
    *result = (src_num < 0) ? -(*result) : *result;
    return true;
}

bool MathFunctions::handle_parse_result(int8_t dest_base, int64_t* num,
                                        StringParser::ParseResult parse_res) {
    // On overflow set special value depending on dest_base.
    // This is consistent with Hive and MySQL's behavior.
    if (parse_res == StringParser::PARSE_OVERFLOW) {
        if (dest_base < 0) {
            *num = -1;
        } else {
            *num = std::numeric_limits<uint64_t>::max();
        }
    } else if (parse_res == StringParser::PARSE_FAILURE) {
        // Some other error condition.
        return false;
    }
    return true;
}

BigIntVal MathFunctions::pmod_bigint(FunctionContext* ctx, const BigIntVal& a, const BigIntVal& b) {
    if (a.is_null || b.is_null) {
        return BigIntVal::null();
    }
    return BigIntVal(((a.val % b.val) + b.val) % b.val);
}

DoubleVal MathFunctions::pmod_double(FunctionContext* ctx, const DoubleVal& a, const DoubleVal& b) {
    if (a.is_null || b.is_null) {
        return DoubleVal::null();
    }
    return DoubleVal(fmod(fmod(a.val, b.val) + b.val, b.val));
}

FloatVal MathFunctions::fmod_float(FunctionContext* ctx, const FloatVal& a, const FloatVal& b) {
    if (a.is_null || b.is_null || b.val == 0) {
        return FloatVal::null();
    }
    return FloatVal(fmodf(a.val, b.val));
}

DoubleVal MathFunctions::fmod_double(FunctionContext* ctx, const DoubleVal& a, const DoubleVal& b) {
    if (a.is_null || b.is_null || b.val == 0) {
        return DoubleVal::null();
    }
    return DoubleVal(fmod(a.val, b.val));
}

BigIntVal MathFunctions::positive_bigint(FunctionContext* ctx, const BigIntVal& val) {
    return val;
}

DoubleVal MathFunctions::positive_double(FunctionContext* ctx, const DoubleVal& val) {
    return val;
}

DecimalV2Val MathFunctions::positive_decimal(FunctionContext* ctx, const DecimalV2Val& val) {
    return val;
}

BigIntVal MathFunctions::negative_bigint(FunctionContext* ctx, const BigIntVal& val) {
    if (val.is_null) {
        return val;
    }
    return BigIntVal(-val.val);
}

DoubleVal MathFunctions::negative_double(FunctionContext* ctx, const DoubleVal& val) {
    if (val.is_null) {
        return val;
    }
    return DoubleVal(-val.val);
}

DecimalV2Val MathFunctions::negative_decimal(FunctionContext* ctx, const DecimalV2Val& val) {
    if (val.is_null) {
        return val;
    }
    const DecimalV2Value& dv1 = DecimalV2Value::from_decimal_val(val);
    DecimalV2Val result;
    (-dv1).to_decimal_val(&result);
    return result;
}

#define LEAST_FN(TYPE)                                                                \
    TYPE MathFunctions::least(FunctionContext* ctx, int num_args, const TYPE* args) { \
        if (args[0].is_null) return TYPE::null();                                     \
        int result_idx = 0;                                                           \
        for (int i = 1; i < num_args; ++i) {                                          \
            if (args[i].is_null) return TYPE::null();                                 \
            if (args[i].val < args[result_idx].val) result_idx = i;                   \
        }                                                                             \
        return TYPE(args[result_idx].val);                                            \
    }

#define LEAST_FNS()        \
    LEAST_FN(TinyIntVal);  \
    LEAST_FN(SmallIntVal); \
    LEAST_FN(IntVal);      \
    LEAST_FN(BigIntVal);   \
    LEAST_FN(LargeIntVal); \
    LEAST_FN(FloatVal);    \
    LEAST_FN(DoubleVal);

LEAST_FNS();

#define LEAST_NONNUMERIC_FN(TYPE_NAME, TYPE, DORIS_TYPE)                              \
    TYPE MathFunctions::least(FunctionContext* ctx, int num_args, const TYPE* args) { \
        if (args[0].is_null) return TYPE::null();                                     \
        DORIS_TYPE result_val = DORIS_TYPE::from_##TYPE_NAME(args[0]);                \
        for (int i = 1; i < num_args; ++i) {                                          \
            if (args[i].is_null) return TYPE::null();                                 \
            DORIS_TYPE val = DORIS_TYPE::from_##TYPE_NAME(args[i]);                   \
            if (val < result_val) result_val = val;                                   \
        }                                                                             \
        TYPE result;                                                                  \
        result_val.to_##TYPE_NAME(&result);                                           \
        return result;                                                                \
    }

#define LEAST_NONNUMERIC_FNS()                                     \
    LEAST_NONNUMERIC_FN(string_val, StringVal, StringValue);       \
    LEAST_NONNUMERIC_FN(datetime_val, DateTimeVal, DateTimeValue); \
    LEAST_NONNUMERIC_FN(decimal_val, DecimalV2Val, DecimalV2Value);

LEAST_NONNUMERIC_FNS();

#define GREATEST_FN(TYPE)                                                                \
    TYPE MathFunctions::greatest(FunctionContext* ctx, int num_args, const TYPE* args) { \
        if (args[0].is_null) return TYPE::null();                                        \
        int result_idx = 0;                                                              \
        for (int i = 1; i < num_args; ++i) {                                             \
            if (args[i].is_null) return TYPE::null();                                    \
            if (args[i].val > args[result_idx].val) result_idx = i;                      \
        }                                                                                \
        return TYPE(args[result_idx].val);                                               \
    }

#define GREATEST_FNS()        \
    GREATEST_FN(TinyIntVal);  \
    GREATEST_FN(SmallIntVal); \
    GREATEST_FN(IntVal);      \
    GREATEST_FN(BigIntVal);   \
    GREATEST_FN(LargeIntVal); \
    GREATEST_FN(FloatVal);    \
    GREATEST_FN(DoubleVal);

GREATEST_FNS();

#define GREATEST_NONNUMERIC_FN(TYPE_NAME, TYPE, DORIS_TYPE)                              \
    TYPE MathFunctions::greatest(FunctionContext* ctx, int num_args, const TYPE* args) { \
        if (args[0].is_null) return TYPE::null();                                        \
        DORIS_TYPE result_val = DORIS_TYPE::from_##TYPE_NAME(args[0]);                   \
        for (int i = 1; i < num_args; ++i) {                                             \
            if (args[i].is_null) return TYPE::null();                                    \
            DORIS_TYPE val = DORIS_TYPE::from_##TYPE_NAME(args[i]);                      \
            if (val > result_val) result_val = val;                                      \
        }                                                                                \
        TYPE result;                                                                     \
        result_val.to_##TYPE_NAME(&result);                                              \
        return result;                                                                   \
    }

#define GREATEST_NONNUMERIC_FNS()                                     \
    GREATEST_NONNUMERIC_FN(string_val, StringVal, StringValue);       \
    GREATEST_NONNUMERIC_FN(datetime_val, DateTimeVal, DateTimeValue); \
    GREATEST_NONNUMERIC_FN(decimal_val, DecimalV2Val, DecimalV2Value);

GREATEST_NONNUMERIC_FNS();
} // namespace doris
