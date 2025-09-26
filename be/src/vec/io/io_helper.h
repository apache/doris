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

#pragma once

#include <fmt/compile.h>
#include <gen_cpp/data.pb.h>
#include <snappy/snappy.h>

#include <iostream>
#include <type_traits>

#include "common/exception.h"
#include "util/binary_cast.hpp"
#include "util/string_parser.hpp"
#include "vec/common/arena.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/io/var_int.h"
#include "vec/runtime/ipv4_value.h"
#include "vec/runtime/ipv6_value.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
inline std::string int128_to_string(int128_t value) {
    return fmt::format(FMT_COMPILE("{}"), value);
}

inline std::string int128_to_string(uint128_t value) {
    return fmt::format(FMT_COMPILE("{}"), value);
}

inline std::string int128_to_string(UInt128 value) {
    return value.to_hex_string();
}

template <typename T>
void write_text(Decimal<T> value, UInt32 scale, std::ostream& ostr) {
    if (value < Decimal<T>(0)) {
        value *= Decimal<T>(-1);
        if (value > Decimal<T>(0)) {
            ostr << '-';
        }
    }

    T whole_part = value;

    if (scale) {
        whole_part = value / decimal_scale_multiplier<T>(scale);
    }
    if constexpr (std::is_same_v<T, __int128_t>) {
        ostr << int128_to_string(whole_part);
    } else {
        ostr << whole_part;
    }
    if (scale) {
        ostr << '.';
        String str_fractional(scale, '0');
        Int32 pos = scale - 1;
        if (value < Decimal<T>(0) && pos >= 0) {
            // Reach here iff this value is a min value of a signed numeric type. It means min<int>()
            // which is -2147483648 multiply -1 is still -2147483648.
            str_fractional[pos] += (value / 10 * 10) - value;
            pos--;
            value /= 10;
            value *= Decimal<T>(-1);
        }
        for (; pos >= 0; --pos, value /= 10) {
            str_fractional[pos] += value % 10;
        }
        ostr.write(str_fractional.data(), scale);
    }
}

template <typename T>
bool try_read_float_text(T& x, const StringRef& in) {
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>,
                  "Argument for readFloatTextImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.',
                  "Layout of char is not like ASCII"); //-V590

    StringParser::ParseResult result;
    x = StringParser::string_to_float<T>(in.data, in.size, &result);

    return result == StringParser::PARSE_SUCCESS;
}

template <typename T, bool enable_strict_mode = false>
bool try_read_int_text(T& x, const StringRef& buf) {
    StringParser::ParseResult result;
    x = StringParser::string_to_int<T, enable_strict_mode>(buf.data, buf.size, &result);

    return result == StringParser::PARSE_SUCCESS;
}

template <typename T>
bool read_date_text_impl(T& x, StringRef& buf) {
    static_assert(std::is_same_v<Int64, T>);
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    auto ans = dv.from_date_str(buf.data, buf.size);
    dv.cast_to_date();

    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

template <typename T>
bool read_date_text_impl(T& x, StringRef& buf, const cctz::time_zone& local_time_zone) {
    static_assert(std::is_same_v<Int64, T>);
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    auto ans = dv.from_date_str(buf.data, buf.size, local_time_zone);
    dv.cast_to_date();
    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

template <typename T>
bool read_ipv4_text_impl(T& x, StringRef& buf) {
    static_assert(std::is_same_v<IPv4, T>);
    bool res = IPv4Value::from_string(x, buf.data, buf.size);
    return res;
}

template <typename T>
bool read_ipv6_text_impl(T& x, StringRef& buf) {
    static_assert(std::is_same_v<IPv6, T>);
    bool res = IPv6Value::from_string(x, buf.data, buf.size);
    return res;
}

template <typename T>
bool read_datetime_text_impl(T& x, StringRef& buf) {
    static_assert(std::is_same_v<Int64, T>);
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    auto ans = dv.from_date_str(buf.data, buf.size);
    dv.to_datetime();
    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

template <typename T>
bool read_datetime_text_impl(T& x, StringRef& buf, const cctz::time_zone& local_time_zone) {
    static_assert(std::is_same_v<Int64, T>);
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    auto ans = dv.from_date_str(buf.data, buf.size, local_time_zone);
    dv.to_datetime();
    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

template <typename T>
bool read_date_v2_text_impl(T& x, StringRef& buf) {
    static_assert(std::is_same_v<UInt32, T>);
    auto dv = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(x);
    auto ans = dv.from_date_str(buf.data, (int)buf.size, config::allow_zero_date);

    // only to match the is_all_read() check to prevent return null

    x = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(dv);
    return ans;
}

template <typename T>
bool read_date_v2_text_impl(T& x, StringRef& buf, const cctz::time_zone& local_time_zone) {
    static_assert(std::is_same_v<UInt32, T>);
    auto dv = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(x);
    auto ans = dv.from_date_str(buf.data, buf.size, local_time_zone, config::allow_zero_date);

    // only to match the is_all_read() check to prevent return null

    x = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(dv);
    return ans;
}

template <typename T>
bool read_datetime_v2_text_impl(T& x, StringRef& buf, UInt32 scale = -1) {
    static_assert(std::is_same_v<UInt64, T>);
    auto dv = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(x);
    auto ans = dv.from_date_str(buf.data, (int)buf.size, scale, config::allow_zero_date);

    // only to match the is_all_read() check to prevent return null

    x = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(dv);
    return ans;
}

template <typename T>
bool read_datetime_v2_text_impl(T& x, StringRef& buf, const cctz::time_zone& local_time_zone,
                                UInt32 scale = -1) {
    static_assert(std::is_same_v<UInt64, T>);
    auto dv = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(x);
    auto ans =
            dv.from_date_str(buf.data, buf.size, local_time_zone, scale, config::allow_zero_date);
    x = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(dv);
    return ans;
}

template <PrimitiveType P, typename T>
StringParser::ParseResult read_decimal_text_impl(T& x, const StringRef& buf, UInt32 precision,
                                                 UInt32 scale) {
    static_assert(IsDecimalNumber<T>);
    if constexpr (!std::is_same_v<Decimal128V2, T>) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        x.value = StringParser::string_to_decimal<P>(buf.data, (int)buf.size, precision, scale,
                                                     &result);
        return result;
    } else {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;
        x.value = StringParser::string_to_decimal<TYPE_DECIMALV2>(
                buf.data, (int)buf.size, DecimalV2Value::PRECISION, DecimalV2Value::SCALE, &result);
        return result;
    }
}

template <typename T>
const char* try_read_first_int_text(T& x, const char* pos, const char* end) {
    const int64_t len = end - pos;
    int64_t i = 0;
    while (i < len) {
        if (pos[i] >= '0' && pos[i] <= '9') {
            i++;
        } else {
            break;
        }
    }
    const char* int_end = pos + i;
    StringRef in((char*)pos, int_end - pos);
    const size_t count = in.size;
    try_read_int_text(x, in);
    return pos + count;
}

template <PrimitiveType P, typename T>
StringParser::ParseResult try_read_decimal_text(T& x, const StringRef& in, UInt32 precision,
                                                UInt32 scale) {
    return read_decimal_text_impl<P, T>(x, in, precision, scale);
}

template <typename T>
bool try_read_ipv4_text(T& x, StringRef& in) {
    return read_ipv4_text_impl<T>(x, in);
}

template <typename T>
bool try_read_ipv6_text(T& x, StringRef& in) {
    return read_ipv6_text_impl<T>(x, in);
}

template <typename T>
bool try_read_datetime_text(T& x, StringRef& in, const cctz::time_zone& local_time_zone) {
    return read_datetime_text_impl<T>(x, in, local_time_zone);
}

template <typename T>
bool try_read_date_text(T& x, StringRef& in, const cctz::time_zone& local_time_zone) {
    return read_date_text_impl<T>(x, in, local_time_zone);
}

template <typename T>
bool try_read_date_v2_text(T& x, StringRef& in, const cctz::time_zone& local_time_zone) {
    return read_date_v2_text_impl<T>(x, in, local_time_zone);
}

template <typename T>
bool try_read_datetime_v2_text(T& x, StringRef& in, const cctz::time_zone& local_time_zone,
                               UInt32 scale) {
    return read_datetime_v2_text_impl<T>(x, in, local_time_zone, scale);
}

bool inline try_read_bool_text(UInt8& x, const StringRef& buf) {
    StringParser::ParseResult result;
    x = StringParser::string_to_bool(buf.data, buf.size, &result);
    return result == StringParser::PARSE_SUCCESS;
}

#include "common/compile_check_end.h"

} // namespace doris::vectorized
