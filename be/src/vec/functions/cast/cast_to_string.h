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

#include "cast_base.h"
#include "util/mysql_global.h"
#include "util/to_string.h"
#include "vec/core/types.h"
#include "vec/runtime/time_value.h"
namespace doris::vectorized {

struct CastToString {
    template <class SRC>
    static inline std::string from_number(const SRC& from);

    // Caller is responsible for ensuring that `buffer` has enough space.
    template <class SRC>
    static inline int from_number(const SRC& from, char* buffer);

    template <typename T>
        requires(std::is_same_v<T, Float32> || std::is_same_v<T, Float64>)
    static inline int from_number(const T& from, char* buffer);

    template <class SRC>
    static inline void push_number(const SRC& from, ColumnString::Chars& chars);

    template <class SRC>
    static inline std::string from_decimal(const SRC& from, UInt32 scale);

    static inline std::string from_date_or_datetime(const VecDateTimeValue& from);

    static inline void push_date_or_datetime(const VecDateTimeValue& from,
                                             ColumnString::Chars& chars);

    static inline std::string from_datev2(const DateV2Value<DateV2ValueType>& from);
    static inline void push_datev2(const DateV2Value<DateV2ValueType>& from,
                                   ColumnString::Chars& chars);

    static inline std::string from_datetimev2(const DateV2Value<DateTimeV2ValueType>& from,
                                              UInt32 scale);
    static inline void push_datetimev2(const DateV2Value<DateTimeV2ValueType>& from, UInt32 scale,
                                       ColumnString::Chars& chars);

    template <class SRC>
    static inline std::string from_ip(const SRC& from);

    static inline std::string from_time(const TimeValue::TimeType& from, UInt32 scale);

private:
    // refer to: https://en.cppreference.com/w/cpp/types/numeric_limits/max_digits10.html
    template <typename T>
        requires(std::is_same_v<T, float> || std::is_same_v<T, double>)
    static inline int _fast_to_buffer(T value, char* buffer) {
        char* end = nullptr;
        // output NaN and Infinity to be compatible with most of the implementations
        if (std::isnan(value)) {
            static constexpr char nan_str[] = "NaN";
            static constexpr int nan_str_len = sizeof(nan_str) - 1;
            memcpy(buffer, nan_str, nan_str_len);
            end = buffer + nan_str_len;
        } else if (std::isinf(value)) {
            static constexpr char inf_str[] = "Infinity";
            static constexpr int inf_str_len = sizeof(inf_str) - 1;
            static constexpr char neg_inf_str[] = "-Infinity";
            static constexpr int neg_inf_str_len = sizeof(neg_inf_str) - 1;
            if (value > 0) {
                memcpy(buffer, inf_str, inf_str_len);
                end = buffer + inf_str_len;
            } else {
                memcpy(buffer, neg_inf_str, neg_inf_str_len);
                end = buffer + neg_inf_str_len;
            }
        } else {
            if constexpr (std::is_same_v<T, float>) {
                end = fmt::format_to(buffer, FMT_COMPILE("{:.{}g}"), value,
                                     std::numeric_limits<float>::digits10 + 1);
            } else {
                end = fmt::format_to(buffer, FMT_COMPILE("{:.{}g}"), value,
                                     std::numeric_limits<double>::digits10 + 1);
            }
        }
        *end = '\0';
        return end - buffer;
    }
};

// BOOLEAN
template <>
inline std::string CastToString::from_number(const UInt8& num) {
    auto f = fmt::format_int(num);
    return std::string(f.data(), f.data() + f.size());
}

template <>
inline void CastToString::push_number(const UInt8& num, ColumnString::Chars& chars) {
    auto f = fmt::format_int(num);
    chars.insert(f.data(), f.data() + f.size());
}

// TINYINT
template <>
inline std::string CastToString::from_number(const Int8& num) {
    auto f = fmt::format_int(num);
    return std::string(f.data(), f.data() + f.size());
}

template <>
inline void CastToString::push_number(const Int8& num, ColumnString::Chars& chars) {
    auto f = fmt::format_int(num);
    chars.insert(f.data(), f.data() + f.size());
}

// SMALLINT
template <>
inline std::string CastToString::from_number(const Int16& num) {
    auto f = fmt::format_int(num);
    return std::string(f.data(), f.data() + f.size());
}

template <>
inline void CastToString::push_number(const Int16& num, ColumnString::Chars& chars) {
    auto f = fmt::format_int(num);
    chars.insert(f.data(), f.data() + f.size());
}

// INT
template <>
inline std::string CastToString::from_number(const Int32& num) {
    auto f = fmt::format_int(num);
    return std::string(f.data(), f.data() + f.size());
}

template <>
inline void CastToString::push_number(const Int32& num, ColumnString::Chars& chars) {
    auto f = fmt::format_int(num);
    chars.insert(f.data(), f.data() + f.size());
}

// BIGINT
template <>
inline std::string CastToString::from_number(const Int64& num) {
    auto f = fmt::format_int(num);
    return std::string(f.data(), f.data() + f.size());
}

template <>
inline void CastToString::push_number(const Int64& num, ColumnString::Chars& chars) {
    auto f = fmt::format_int(num);
    chars.insert(f.data(), f.data() + f.size());
}

// LARGEINT
template <>
inline std::string CastToString::from_number(const Int128& num) {
    fmt::memory_buffer buffer;
    fmt::format_to(buffer, "{}", num);
    return std::string(buffer.data(), buffer.size());
}

template <>
inline void CastToString::push_number(const Int128& num, ColumnString::Chars& chars) {
    fmt::memory_buffer buffer;
    fmt::format_to(buffer, "{}", num);
    chars.insert(buffer.data(), buffer.data() + buffer.size());
}

// FLOAT

template <>
inline std::string CastToString::from_number(const Float32& num) {
    char buf[MAX_FLOAT_STR_LENGTH + 2];
    int len = _fast_to_buffer(num, buf);
    return std::string(buf, buf + len);
}

template <typename T>
    requires(std::is_same_v<T, Float32> || std::is_same_v<T, Float64>)
inline int CastToString::from_number(const T& from, char* buffer) {
    return _fast_to_buffer(from, buffer);
}

template <>
inline void CastToString::push_number(const Float32& num, ColumnString::Chars& chars) {
    char buf[MAX_FLOAT_STR_LENGTH + 2];
    int len = _fast_to_buffer(num, buf);
    chars.insert(buf, buf + len);
}

// DOUBLE
template <>
inline std::string CastToString::from_number(const Float64& num) {
    char buf[MAX_DOUBLE_STR_LENGTH + 2];
    int len = _fast_to_buffer(num, buf);
    return std::string(buf, len);
}

template <>
inline void CastToString::push_number(const Float64& num, ColumnString::Chars& chars) {
    char buf[MAX_DOUBLE_STR_LENGTH + 2];
    int len = _fast_to_buffer(num, buf);
    chars.insert(buf, buf + len);
}

// DECIMAL32
template <>
inline std::string CastToString::from_decimal(const Decimal32& from, UInt32 scale) {
    return from.to_string(scale);
}

// DECIMAL64
template <>
inline std::string CastToString::from_decimal(const Decimal64& from, UInt32 scale) {
    return from.to_string(scale);
}

// DECIMAL128
template <>
inline std::string CastToString::from_decimal(const Decimal128V3& from, UInt32 scale) {
    return from.to_string(scale);
}

// DECIMAL256
template <>
inline std::string CastToString::from_decimal(const Decimal256& from, UInt32 scale) {
    return from.to_string(scale);
}

// DECIMALV2
template <>
inline std::string CastToString::from_decimal(const Decimal128V2& from, UInt32 scale) {
    auto value = (DecimalV2Value)from;
    auto str = value.to_string(scale);
    return str;
}

// DATEV1 DATETIMEV1
inline std::string CastToString::from_date_or_datetime(const VecDateTimeValue& from) {
    char buf[64];
    char* pos = from.to_string(buf);
    // DateTime to_string the end is /0
    return std::string(buf, pos - 1);
}

inline void CastToString::push_date_or_datetime(const VecDateTimeValue& from,
                                                ColumnString::Chars& chars) {
    char buf[64];
    char* pos = from.to_string(buf);
    // DateTime to_string the end is /0
    chars.insert(buf, pos - 1);
}

// DATEV2
inline std::string CastToString::from_datev2(const DateV2Value<DateV2ValueType>& from) {
    char buf[64];
    char* pos = from.to_string(buf);
    // DateTime to_string the end is /0
    return std::string(buf, pos - 1);
}

inline void CastToString::push_datev2(const DateV2Value<DateV2ValueType>& from,
                                      ColumnString::Chars& chars) {
    char buf[64];
    char* pos = from.to_string(buf);
    // DateTime to_string the end is /0
    chars.insert(buf, pos - 1);
}

// DATETIMEV2
inline std::string CastToString::from_datetimev2(const DateV2Value<DateTimeV2ValueType>& from,
                                                 UInt32 scale) {
    char buf[64];
    char* pos = from.to_string(buf, scale);
    // DateTime to_string the end is /0
    return std::string(buf, pos - 1);
}

inline void CastToString::push_datetimev2(const DateV2Value<DateTimeV2ValueType>& from,
                                          UInt32 scale, ColumnString::Chars& chars) {
    char buf[64];
    char* pos = from.to_string(buf, scale);
    // DateTime to_string the end is /0
    chars.insert(buf, pos - 1);
}

// IPv4
template <>
inline std::string CastToString::from_ip(const IPv4& from) {
    auto value = IPv4Value(from);
    return value.to_string();
}

//IPv6

template <>
inline std::string CastToString::from_ip(const IPv6& from) {
    auto value = IPv6Value(from);
    return value.to_string();
}

// Time
inline std::string CastToString::from_time(const TimeValue::TimeType& from, UInt32 scale) {
    return timev2_to_buffer_from_double(from, scale);
}

class CastToStringFunction {
public:
    static Status execute_impl(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count,
                               const NullMap::value_type* null_map = nullptr) {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IDataType& type = *col_with_type_and_name.type;
        const IColumn& col_from = *col_with_type_and_name.column;

        auto col_to = ColumnString::create();
        type.to_string_batch(col_from, *col_to);

        block.replace_by_position(result, std::move(col_to));
        return Status::OK();
    }
};

namespace CastWrapper {

inline WrapperType create_string_wrapper(const DataTypePtr& from_type) {
    return [](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
              uint32_t result, size_t input_rows_count,
              const NullMap::value_type* null_map = nullptr) {
        return CastToStringFunction::execute_impl(context, block, arguments, result,
                                                  input_rows_count, null_map);
    };
}

}; // namespace CastWrapper
} // namespace doris::vectorized