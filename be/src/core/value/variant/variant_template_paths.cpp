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

#include "core/value/variant/variant_template_paths.h"

#include <fnmatch.h>

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <utility>

#include "core/data_type/primitive_type.h"
#include "core/value/variant/variant_parquet_encoding.h"

namespace doris {

VariantTemplatePaths::VariantTemplatePaths(std::vector<VariantTemplatePath> paths)
        : _paths(std::move(paths)),
          _reads_decimal_text(std::any_of(
                  _paths.begin(), _paths.end(),
                  [](const VariantTemplatePath& field) { return is_decimalv3(field.type); })) {}

VariantNumberRead VariantTemplatePaths::number_read(const std::string& path) const {
    for (const VariantTemplatePath& field : _paths) {
        // Both sides as C strings, as the shredder compares them (generate_sub_column_info): a key
        // holding a NUL is matched up to it there, so it has to be here.
        const bool matched =
                field.is_glob ? fnmatch(field.pattern.c_str(), path.c_str(), FNM_PATHNAME) == 0
                              : std::strcmp(field.pattern.c_str(), path.c_str()) == 0;
        if (!matched) {
            continue;
        }
        // The first matching field decides, whatever its type: a field of any other type ends the
        // search with NATIVE, as it ends the shredder's.
        if (is_decimalv3(field.type)) {
            return VariantNumberRead::DECIMAL_TEXT;
        }
        return field.type == TYPE_LARGEINT ? VariantNumberRead::LARGEINT_TEXT
                                           : VariantNumberRead::NATIVE;
    }
    return VariantNumberRead::NATIVE;
}

namespace {

constexpr int kMaxDigits = 38;

// 2^127, the magnitude of the smallest LARGEINT; the largest LARGEINT is one less.
constexpr unsigned __int128 kLargeIntMinMagnitude = static_cast<unsigned __int128>(1) << 127;

bool is_digit(char c) {
    return c >= '0' && c <= '9';
}

// A Variant decimal4 / decimal8 holds at most 9 / 18 digits, and a reader may reject a scale above
// that bound as firmly as a longer unscaled value (Spark's VariantUtil.checkDecimal does), so the
// width follows the scale as well as the magnitude: 0.0000000001 (unscaled 1, scale 10) is a
// decimal8, not the decimal4 its magnitude alone would pick, and 1e-19 a decimal16.
uint8_t variant_decimal_width(unsigned __int128 magnitude, long scale) {
    if (scale <= 9 && magnitude <= VARIANT_DECIMAL4_MAX) {
        return 4;
    }
    if (scale <= 18 && magnitude <= VARIANT_DECIMAL8_MAX) {
        return 8;
    }
    return 16;
}

} // namespace

JsonNumberToDecimal parse_json_number_as_decimal(std::string_view token,
                                                 VariantDecimalText& out) noexcept {
    if (token.empty()) {
        return JsonNumberToDecimal::MALFORMED;
    }
    size_t pos = 0;
    const bool negative = token[pos] == '-';
    if (negative) {
        ++pos;
    }
    // JSON: int = "0" / digit1-9 *DIGIT, so a leading zero may only be the whole integer part.
    // simdjson reports "01", "-01" or "00.5" as NUMBER_ERROR and the collector hands such tokens
    // here when the path is declared DECIMAL, so this is the only check they meet: without it
    // {"amount":01} would store 1 on a declared path while the same document fails everywhere else.
    if (pos + 1 < token.size() && token[pos] == '0' && is_digit(token[pos + 1])) {
        return JsonNumberToDecimal::MALFORMED;
    }
    unsigned __int128 unscaled = 0;
    int significant = 0;
    int fraction_digits = 0;
    bool in_fraction = false;
    bool any_digit = false;
    bool too_many_digits = false;
    for (; pos < token.size(); ++pos) {
        const char c = token[pos];
        if (is_digit(c)) {
            any_digit = true;
            if (in_fraction) {
                ++fraction_digits;
            }
            if (unscaled == 0 && c == '0') {
                continue;
            }
            if (++significant > kMaxDigits) {
                too_many_digits = true;
                continue;
            }
            unscaled = unscaled * 10 + static_cast<unsigned>(c - '0');
        } else if (c == '.' && !in_fraction && any_digit) {
            in_fraction = true;
            any_digit = false;
        } else {
            break;
        }
    }
    if (!any_digit) {
        return JsonNumberToDecimal::MALFORMED;
    }
    long exponent = 0;
    if (pos < token.size() && (token[pos] == 'e' || token[pos] == 'E')) {
        ++pos;
        bool exponent_negative = false;
        if (pos < token.size() && (token[pos] == '+' || token[pos] == '-')) {
            exponent_negative = token[pos] == '-';
            ++pos;
        }
        if (pos >= token.size() || !is_digit(token[pos])) {
            return JsonNumberToDecimal::MALFORMED;
        }
        // Past this bound the exponent outweighs every fraction digit the token holds and every
        // digit a decimal holds, so the verdict below no longer depends on its exact value: the
        // exponent saturates there instead of growing. A fixed cap would not do, since a long
        // fraction can cancel any fixed exponent: 0.<999 zeros>1e10000 is 10^9000, not 1.
        const long exponent_bound = static_cast<long>(token.size()) + kMaxDigits;
        for (; pos < token.size() && is_digit(token[pos]); ++pos) {
            if (exponent <= exponent_bound) {
                exponent = exponent * 10 + (token[pos] - '0');
            }
        }
        if (exponent_negative) {
            exponent = -exponent;
        }
    }
    if (pos != token.size()) {
        return JsonNumberToDecimal::MALFORMED;
    }
    long scale = static_cast<long>(fraction_digits) - exponent;
    // A value of `significant` digits at this scale is below 10^(significant - scale). Below
    // 10^-39 it is less than a tenth of the last place of the finest decimal there is, so it rounds
    // to 0 at every scale a decimal has, as an exact zero does. Reading it as 0 here keeps it away
    // from the cast behind OUT_OF_RANGE, which reads no exponent beyond 32 bits and would answer
    // NULL for 1e-3000000000 where every smaller exponent answers 0. The exponent saturates far
    // past this bound, so a saturated negative exponent always lands here.
    const bool rounds_to_zero = scale - static_cast<long>(significant) > kMaxDigits;
    if (unscaled == 0 || rounds_to_zero) {
        out.unscaled = 0;
        out.scale = static_cast<uint8_t>(scale < 0 ? 0 : (scale > kMaxDigits ? kMaxDigits : scale));
        out.width = variant_decimal_width(0, out.scale);
        return JsonNumberToDecimal::OK;
    }
    if (too_many_digits) {
        return JsonNumberToDecimal::OUT_OF_RANGE;
    }
    while (scale < 0) {
        if (++significant > kMaxDigits) {
            return JsonNumberToDecimal::OUT_OF_RANGE;
        }
        unscaled *= 10;
        ++scale;
    }
    if (scale > kMaxDigits) {
        return JsonNumberToDecimal::OUT_OF_RANGE;
    }
    out.unscaled = negative ? -static_cast<__int128>(unscaled) : static_cast<__int128>(unscaled);
    out.scale = static_cast<uint8_t>(scale);
    out.width = variant_decimal_width(unscaled, scale);
    return JsonNumberToDecimal::OK;
}

JsonNumberToLargeInt parse_json_integer_as_largeint(std::string_view token,
                                                    __int128& out) noexcept {
    size_t pos = 0;
    const bool negative = !token.empty() && token[0] == '-';
    if (negative) {
        ++pos;
    }
    if (pos >= token.size() || !is_digit(token[pos])) {
        return JsonNumberToLargeInt::NOT_AN_INTEGER;
    }
    // JSON: int = "0" / digit1-9 *DIGIT, so a zero may only be the whole integer. simdjson reports
    // "01" or "-01" as NUMBER_ERROR and the collector hands such tokens here when the path is
    // declared LARGEINT, so this is the only check they meet (see parse_json_number_as_decimal).
    if (token[pos] == '0' && pos + 1 < token.size()) {
        return JsonNumberToLargeInt::NOT_AN_INTEGER;
    }
    // The range is not symmetric: -2^127 is a LARGEINT and 2^127 is not. The magnitude is
    // accumulated unsigned against the bound of its sign, because 2^127 has no positive __int128.
    const unsigned __int128 limit = negative ? kLargeIntMinMagnitude : kLargeIntMinMagnitude - 1;
    unsigned __int128 magnitude = 0;
    bool out_of_range = false;
    for (; pos < token.size(); ++pos) {
        const char c = token[pos];
        if (!is_digit(c)) {
            // a fraction, an exponent or trailing bytes
            return JsonNumberToLargeInt::NOT_AN_INTEGER;
        }
        if (out_of_range) {
            // keep scanning: a malformed tail still has to answer NOT_AN_INTEGER
            continue;
        }
        const auto digit = static_cast<unsigned __int128>(c - '0');
        if (magnitude > (limit - digit) / 10) {
            out_of_range = true;
            continue;
        }
        magnitude = magnitude * 10 + digit;
    }
    if (out_of_range) {
        return JsonNumberToLargeInt::OUT_OF_RANGE;
    }
    // 0 - magnitude wraps modulo 2^128, which is the two's complement of the magnitude: it yields
    // -2^127 for 2^127, where negating a signed value would overflow.
    out = negative ? static_cast<__int128>(0 - magnitude) : static_cast<__int128>(magnitude);
    return JsonNumberToLargeInt::OK;
}

} // namespace doris
