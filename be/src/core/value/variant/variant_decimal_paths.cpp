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

namespace doris {

VariantDecimalPathSet::VariantDecimalPathSet(std::vector<VariantDecimalPath> paths)
        : _paths(std::move(paths)) {
    for (const VariantDecimalPath& path : _paths) {
        if (path.is_glob) {
            _globs.push_back(path.pattern);
        } else {
            _exact.insert(path.pattern);
        }
    }
}

bool VariantDecimalPathSet::matches(std::string_view path) const {
    if (path.empty()) {
        return false;
    }
    const std::string candidate(path);
    if (_exact.find(candidate) != _exact.end()) {
        return true;
    }
    for (const std::string& glob : _globs) {
        if (fnmatch(glob.c_str(), candidate.c_str(), FNM_PATHNAME) == 0) {
            return true;
        }
    }
    return false;
}

namespace {

constexpr int kMaxDigits = 38;

bool is_digit(char c) {
    return c >= '0' && c <= '9';
}

} // namespace

JsonNumberToDecimal parse_json_number_as_decimal(std::string_view token,
                                                 VariantDecimalText* out) noexcept {
    if (out == nullptr || token.empty()) {
        return JsonNumberToDecimal::MALFORMED;
    }
    size_t pos = 0;
    const bool negative = token[pos] == '-';
    if (negative) {
        ++pos;
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
        out->unscaled = 0;
        out->scale = static_cast<uint8_t>(scale < 0 ? 0 : (scale > kMaxDigits ? kMaxDigits : scale));
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
    out->unscaled = negative ? -static_cast<__int128>(unscaled) : static_cast<__int128>(unscaled);
    out->scale = static_cast<uint8_t>(scale);
    return JsonNumberToDecimal::OK;
}

} // namespace doris
