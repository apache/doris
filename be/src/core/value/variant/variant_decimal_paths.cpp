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

#include "core/value/variant/variant_decimal_paths.h"

#include <fnmatch.h>

#include <cstddef>
#include <utility>

#include "core/value/variant/variant_parquet_encoding.h"

namespace doris {

VariantDecimalPathSet::VariantDecimalPathSet(std::vector<VariantTemplatePath> paths)
        : _paths(std::move(paths)) {}

bool VariantDecimalPathSet::matches(const std::string& path) const {
    if (path.empty()) {
        return false;
    }
    for (const VariantTemplatePath& field : _paths) {
        const bool matched = field.is_glob
                                     ? fnmatch(field.pattern.c_str(), path.c_str(), FNM_PATHNAME) == 0
                                     : field.pattern == path;
        if (matched) {
            return field.is_decimal;
        }
    }
    return false;
}

namespace {

constexpr int kMaxDigits = 38;

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
        for (; pos < token.size() && is_digit(token[pos]); ++pos) {
            if (exponent < 1000) {
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
    if (too_many_digits) {
        return JsonNumberToDecimal::OUT_OF_RANGE;
    }
    long scale = static_cast<long>(fraction_digits) - exponent;
    if (unscaled == 0) {
        out.unscaled = 0;
        out.scale = static_cast<uint8_t>(scale < 0 ? 0 : (scale > kMaxDigits ? kMaxDigits : scale));
        out.width = variant_decimal_width(0, out.scale);
        return JsonNumberToDecimal::OK;
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

} // namespace doris
