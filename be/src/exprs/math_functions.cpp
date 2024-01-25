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
#include <string.h>

#include "common/compiler_util.h" // IWYU pragma: keep
// IWYU pragma: no_include <bits/std_abs.h>
#include <cmath>
#include <limits>
#include <string_view>

#include "udf/udf.h"
#include "util/string_parser.hpp"

namespace doris {

constexpr std::string_view alphanumeric_chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

constexpr double log_10[] = {
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

StringRef MathFunctions::decimal_to_base(FunctionContext* ctx, int64_t src_num, int8_t dest_base) {
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
        buf[buf_index] = alphanumeric_chars[temp_num % abs_base];
        temp_num /= abs_base;
        --buf_index;
        ++result_len;
    } while (temp_num > 0);
    // Add optional sign.
    if (src_num < 0 && dest_base < 0) {
        buf[buf_index] = '-';
        ++result_len;
    }
    StringRef result = ctx->create_temp_string_val(result_len);
    memcpy(const_cast<char*>(result.data), buf + max_digits - result_len, result_len);
    return result;
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

} // namespace doris
