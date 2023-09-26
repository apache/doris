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

#include <gen_cpp/data.pb.h>
#include <snappy/snappy.h>

#include <iostream>

#include "common/exception.h"
#include "util/binary_cast.hpp"
#include "util/string_parser.hpp"
#include "vec/common/arena.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/common/uint128.h"
#include "vec/core/types.h"
#include "vec/io/reader_buffer.h"
#include "vec/io/var_int.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {

// Define in the namespace and avoid defining global macros,
// because it maybe conflicts with other libs
static constexpr size_t DEFAULT_MAX_STRING_SIZE = 1073741824; // 1GB
static constexpr size_t DEFAULT_MAX_JSON_SIZE = 1073741824;   // 1GB
static constexpr auto WRITE_HELPERS_MAX_INT_WIDTH = 40U;

inline std::string int128_to_string(__int128_t value) {
    fmt::memory_buffer buffer;
    fmt::format_to(buffer, "{}", value);
    return std::string(buffer.data(), buffer.size());
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
/// Methods for output in binary format.

/// Write POD-type in native format. It's recommended to use only with packed (dense) data types.
template <typename Type>
void write_pod_binary(const Type& x, BufferWritable& buf) {
    buf.write(reinterpret_cast<const char*>(&x), sizeof(x));
}

template <typename Type>
void write_int_binary(const Type& x, BufferWritable& buf) {
    write_pod_binary(x, buf);
}

template <typename Type>
void write_float_binary(const Type& x, BufferWritable& buf) {
    write_pod_binary(x, buf);
}

inline void write_string_binary(const std::string& s, BufferWritable& buf) {
    write_var_uint(s.size(), buf);
    buf.write(s.data(), s.size());
}

inline void write_string_binary(const StringRef& s, BufferWritable& buf) {
    write_var_uint(s.size, buf);
    buf.write(s.data, s.size);
}

inline void write_string_binary(const char* s, BufferWritable& buf) {
    write_string_binary(StringRef {std::string(s)}, buf);
}

inline void write_json_binary(JsonbField s, BufferWritable& buf) {
    write_string_binary(StringRef {s.get_value(), s.get_size()}, buf);
}

template <typename Type>
void write_vector_binary(const std::vector<Type>& v, BufferWritable& buf) {
    write_var_uint(v.size(), buf);

    for (typename std::vector<Type>::const_iterator it = v.begin(); it != v.end(); ++it) {
        write_binary(*it, buf);
    }
}

inline void write_binary(const String& x, BufferWritable& buf) {
    write_string_binary(x, buf);
}

inline void write_binary(const StringRef& x, BufferWritable& buf) {
    write_string_binary(x, buf);
}

template <typename Type>
void write_binary(const Type& x, BufferWritable& buf) {
    write_pod_binary(x, buf);
}

/// Read POD-type in native format
template <typename Type>
void read_pod_binary(Type& x, BufferReadable& buf) {
    buf.read(reinterpret_cast<char*>(&x), sizeof(x));
}

template <typename Type>
void read_int_binary(Type& x, BufferReadable& buf) {
    read_pod_binary(x, buf);
}

template <typename Type>
void read_float_binary(Type& x, BufferReadable& buf) {
    read_pod_binary(x, buf);
}

inline void read_string_binary(std::string& s, BufferReadable& buf,
                               size_t MAX_STRING_SIZE = DEFAULT_MAX_STRING_SIZE) {
    UInt64 size = 0;
    read_var_uint(size, buf);

    if (size > MAX_STRING_SIZE) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Too large string size.");
    }

    s.resize(size);
    buf.read(s.data(), size);
}

inline void read_string_binary(StringRef& s, BufferReadable& buf,
                               size_t MAX_STRING_SIZE = DEFAULT_MAX_STRING_SIZE) {
    UInt64 size = 0;
    read_var_uint(size, buf);

    if (size > MAX_STRING_SIZE) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Too large string size.");
    }

    s = buf.read(size);
}

inline StringRef read_string_binary_into(Arena& arena, BufferReadable& buf) {
    UInt64 size = 0;
    read_var_uint(size, buf);

    char* data = arena.alloc(size);
    buf.read(data, size);

    return StringRef(data, size);
}

inline void read_json_binary(JsonbField val, BufferReadable& buf,
                             size_t MAX_JSON_SIZE = DEFAULT_MAX_JSON_SIZE) {
    StringRef jrf = StringRef {val.get_value(), val.get_size()};
    read_string_binary(jrf, buf);
}

template <typename Type>
void read_vector_binary(std::vector<Type>& v, BufferReadable& buf,
                        size_t MAX_VECTOR_SIZE = DEFAULT_MAX_STRING_SIZE) {
    UInt64 size = 0;
    read_var_uint(size, buf);

    if (size > MAX_VECTOR_SIZE) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Too large vector size.");
    }

    v.resize(size);
    for (size_t i = 0; i < size; ++i) {
        read_binary(v[i], buf);
    }
}

inline void read_binary(String& x, BufferReadable& buf) {
    read_string_binary(x, buf);
}

inline void read_binary(StringRef& x, BufferReadable& buf) {
    read_string_binary(x, buf);
}

template <typename Type>
void read_binary(Type& x, BufferReadable& buf) {
    read_pod_binary(x, buf);
}

template <typename T>
bool read_float_text_fast_impl(T& x, ReadBuffer& in) {
    static_assert(std::is_same_v<T, double> || std::is_same_v<T, float>,
                  "Argument for readFloatTextImpl must be float or double");
    static_assert('a' > '.' && 'A' > '.' && '\n' < '.' && '\t' < '.' && '\'' < '.' && '"' < '.',
                  "Layout of char is not like ASCII"); //-V590

    StringParser::ParseResult result;
    x = StringParser::string_to_float<T>(in.position(), in.count(), &result);

    if (UNLIKELY(result != StringParser::PARSE_SUCCESS || std::isnan(x) || std::isinf(x))) {
        return false;
    }

    // only to match the is_all_read() check to prevent return null
    in.position() = in.end();
    return true;
}

template <typename T>
bool read_int_text_impl(T& x, ReadBuffer& buf) {
    StringParser::ParseResult result;
    x = StringParser::string_to_int<T>(buf.position(), buf.count(), &result);

    if (UNLIKELY(result != StringParser::PARSE_SUCCESS)) {
        return false;
    }

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    return true;
}

template <typename T>
bool read_date_text_impl(T& x, ReadBuffer& buf) {
    static_assert(std::is_same_v<Int64, T>);
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    auto ans = dv.from_date_str(buf.position(), buf.count());
    dv.cast_to_date();

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

template <typename T>
bool read_date_text_impl(T& x, ReadBuffer& buf, const cctz::time_zone& local_time_zone) {
    static_assert(std::is_same_v<Int64, T>);
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    auto ans = dv.from_date_str(buf.position(), buf.count(), local_time_zone);
    dv.cast_to_date();

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

template <typename T>
bool read_datetime_text_impl(T& x, ReadBuffer& buf) {
    static_assert(std::is_same_v<Int64, T>);
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    auto ans = dv.from_date_str(buf.position(), buf.count());
    dv.to_datetime();

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

template <typename T>
bool read_datetime_text_impl(T& x, ReadBuffer& buf, const cctz::time_zone& local_time_zone) {
    static_assert(std::is_same_v<Int64, T>);
    auto dv = binary_cast<Int64, VecDateTimeValue>(x);
    auto ans = dv.from_date_str(buf.position(), buf.count(), local_time_zone);
    dv.to_datetime();

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    x = binary_cast<VecDateTimeValue, Int64>(dv);
    return ans;
}

template <typename T>
bool read_date_v2_text_impl(T& x, ReadBuffer& buf) {
    static_assert(std::is_same_v<UInt32, T>);
    auto dv = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(x);
    auto ans = dv.from_date_str(buf.position(), buf.count());

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    x = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(dv);
    return ans;
}

template <typename T>
bool read_date_v2_text_impl(T& x, ReadBuffer& buf, const cctz::time_zone& local_time_zone) {
    static_assert(std::is_same_v<UInt32, T>);
    auto dv = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(x);
    auto ans = dv.from_date_str(buf.position(), buf.count(), local_time_zone);

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    x = binary_cast<DateV2Value<DateV2ValueType>, UInt32>(dv);
    return ans;
}

template <typename T>
bool read_datetime_v2_text_impl(T& x, ReadBuffer& buf, UInt32 scale = -1) {
    static_assert(std::is_same_v<UInt64, T>);
    auto dv = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(x);
    auto ans = dv.from_date_str(buf.position(), buf.count(), scale);

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    x = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(dv);
    return ans;
}

template <typename T>
bool read_datetime_v2_text_impl(T& x, ReadBuffer& buf, const cctz::time_zone& local_time_zone,
                                UInt32 scale = -1) {
    static_assert(std::is_same_v<UInt64, T>);
    auto dv = binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(x);
    auto ans = dv.from_date_str(buf.position(), buf.count(), local_time_zone, scale);

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    x = binary_cast<DateV2Value<DateTimeV2ValueType>, UInt64>(dv);
    return ans;
}

template <PrimitiveType P, typename T>
StringParser::ParseResult read_decimal_text_impl(T& x, ReadBuffer& buf, UInt32 precision,
                                                 UInt32 scale) {
    static_assert(IsDecimalNumber<T>);
    if constexpr (!std::is_same_v<Decimal128, T>) {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;

        x.value = StringParser::string_to_decimal<P>((const char*)buf.position(), buf.count(),
                                                     precision, scale, &result);
        // only to match the is_all_read() check to prevent return null
        buf.position() = buf.end();
        return result;
    } else {
        StringParser::ParseResult result = StringParser::PARSE_SUCCESS;

        x.value = StringParser::string_to_decimal<TYPE_DECIMALV2>(buf.position(), buf.count(),
                                                                  DecimalV2Value::PRECISION,
                                                                  DecimalV2Value::SCALE, &result);

        // only to match the is_all_read() check to prevent return null
        buf.position() = buf.end();

        return result;
    }
}

template <typename T>
bool try_read_bool_text(T& x, ReadBuffer& buf) {
    if (read_int_text_impl<T>(x, buf)) {
        return x == 0 || x == 1;
    }

    StringParser::ParseResult result;
    x = StringParser::string_to_bool(buf.position(), buf.count(), &result);
    if (UNLIKELY(result != StringParser::PARSE_SUCCESS)) {
        return false;
    }

    // only to match the is_all_read() check to prevent return null
    buf.position() = buf.end();
    return true;
}

template <typename T>
bool try_read_int_text(T& x, ReadBuffer& buf) {
    return read_int_text_impl<T>(x, buf);
}

template <typename T>
const char* try_read_first_int_text(T& x, const char* pos, const char* end) {
    const int len = end - pos;
    int i = 0;
    while (i < len) {
        if (pos[i] >= '0' && pos[i] <= '9') {
            i++;
        } else {
            break;
        }
    }
    const char* int_end = pos + i;
    ReadBuffer in((char*)pos, int_end - pos);
    const size_t count = in.count();
    try_read_int_text(x, in);
    return pos + count;
}

template <typename T>
bool try_read_float_text(T& x, ReadBuffer& in) {
    return read_float_text_fast_impl<T>(x, in);
}

template <PrimitiveType P, typename T>
StringParser::ParseResult try_read_decimal_text(T& x, ReadBuffer& in, UInt32 precision,
                                                UInt32 scale) {
    return read_decimal_text_impl<P, T>(x, in, precision, scale);
}

template <typename T>
bool try_read_datetime_text(T& x, ReadBuffer& in, const cctz::time_zone& local_time_zone) {
    return read_datetime_text_impl<T>(x, in, local_time_zone);
}

template <typename T>
bool try_read_date_text(T& x, ReadBuffer& in, const cctz::time_zone& local_time_zone) {
    return read_date_text_impl<T>(x, in, local_time_zone);
}

template <typename T>
bool try_read_date_v2_text(T& x, ReadBuffer& in, const cctz::time_zone& local_time_zone) {
    return read_date_v2_text_impl<T>(x, in, local_time_zone);
}

template <typename T>
bool try_read_datetime_v2_text(T& x, ReadBuffer& in, const cctz::time_zone& local_time_zone,
                               UInt32 scale) {
    return read_datetime_v2_text_impl<T>(x, in, local_time_zone, scale);
}
} // namespace doris::vectorized
