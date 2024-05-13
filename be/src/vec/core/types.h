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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Types.h
// and modified by Doris

#pragma once

#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <vector>

#include "common/consts.h"
#include "util/binary_cast.hpp"
#include "vec/common/int_exp.h"
#include "vec/core/wide_integer.h"
#include "vec/core/wide_integer_to_string.h"

namespace doris {

class BitmapValue;
class HyperLogLog;
class QuantileState;

struct decimal12_t;
struct uint24_t;
struct StringRef;

namespace vectorized {

/// Data types for representing elementary values from a database in RAM.

struct Null {};

// The identifier should be less than int16, because castexpr using the identifier
// instead of type name as type parameter. It will using int16 as column type.
enum class TypeIndex {
    Nothing = 0,
    UInt8 = 1,
    UInt16 = 2,
    UInt32 = 3,
    UInt64 = 4,
    UInt128 = 5,
    Int8 = 6,
    Int16 = 7,
    Int32 = 8,
    Int64 = 9,
    Int128 = 10,
    Float32 = 11,
    Float64 = 12,
    Date = 13,
    DateTime = 14,
    String = 15,
    FixedString = 16,
    Enum8 = 17,
    Enum16 = 18,
    Decimal32 = 19,
    Decimal64 = 20,
    Decimal128V2 = 21,
    UUID = 22,
    Array = 23,
    Tuple = 24,
    Set = 25,
    Interval = 26,
    Nullable = 27,
    Function = 28,
    AggregateFunction = 29,
    LowCardinality = 30,
    BitMap = 31,
    HLL = 32,
    DateV2 = 33,
    DateTimeV2 = 34,
    TimeV2 = 35,
    FixedLengthObject = 36,
    JSONB = 37,
    Decimal128V3 = 38,
    Map = 39,
    Struct = 40,
    VARIANT = 41,
    QuantileState = 42,
    Time = 43,
    AggState = 44,
    Decimal256 = 45,
    IPv4 = 46,
    IPv6 = 47,
    Int256
};

struct Consted {
    TypeIndex tp;
};

using UInt8 = uint8_t;
using UInt16 = uint16_t;
using UInt32 = uint32_t;
using UInt64 = uint64_t;

using Int8 = int8_t;
using Int16 = int16_t;
using Int32 = int32_t;
using Int64 = int64_t;

using Float32 = float;
using Float64 = double;

using String = std::string;

/** Note that for types not used in DB, IsNumber is false.
  */
template <typename T>
constexpr bool IsNumber = false;

template <>
inline constexpr bool IsNumber<UInt8> = true;
template <>
inline constexpr bool IsNumber<UInt16> = true;
template <>
inline constexpr bool IsNumber<UInt32> = true;
template <>
inline constexpr bool IsNumber<UInt64> = true;
template <>
inline constexpr bool IsNumber<Int8> = true;
template <>
inline constexpr bool IsNumber<Int16> = true;
template <>
inline constexpr bool IsNumber<Int32> = true;
template <>
inline constexpr bool IsNumber<Int64> = true;
template <>
inline constexpr bool IsNumber<Float32> = true;
template <>
inline constexpr bool IsNumber<Float64> = true;

template <typename T>
struct TypeName;

// only used at predicate_column
template <>
struct TypeName<bool> {
    static const char* get() { return "bool"; }
};
template <>
struct TypeName<decimal12_t> {
    static const char* get() { return "decimal12_t"; }
};
template <>
struct TypeName<uint24_t> {
    static const char* get() { return "uint24_t"; }
};
template <>
struct TypeName<StringRef> {
    static const char* get() { return "StringRef"; }
};

template <>
struct TypeName<UInt8> {
    static const char* get() { return "UInt8"; }
};
template <>
struct TypeName<UInt16> {
    static const char* get() { return "UInt16"; }
};
template <>
struct TypeName<UInt32> {
    static const char* get() { return "UInt32"; }
};
template <>
struct TypeName<UInt64> {
    static const char* get() { return "UInt64"; }
};
template <>
struct TypeName<Int8> {
    static const char* get() { return "Int8"; }
};
template <>
struct TypeName<Int16> {
    static const char* get() { return "Int16"; }
};
template <>
struct TypeName<Int32> {
    static const char* get() { return "Int32"; }
};
template <>
struct TypeName<Int64> {
    static const char* get() { return "Int64"; }
};
template <>
struct TypeName<Float32> {
    static const char* get() { return "Float32"; }
};
template <>
struct TypeName<Float64> {
    static const char* get() { return "Float64"; }
};
template <>
struct TypeName<String> {
    static const char* get() { return "String"; }
};
template <>
struct TypeName<BitmapValue> {
    static const char* get() { return "BitMap"; }
};
template <>
struct TypeName<HyperLogLog> {
    static const char* get() { return "HLL"; }
};

template <>
struct TypeName<QuantileState> {
    static const char* get() { return "QuantileState"; }
};

template <>
struct TypeName<DecimalV2Value> {
    static const char* get() { return "decimalv2"; }
};

template <>
struct TypeName<VecDateTimeValue> {
    static const char* get() { return "Datetime"; }
};

template <>
struct TypeName<DateV2Value<DateV2ValueType>> {
    static const char* get() { return "DateV2"; }
};

template <>
struct TypeName<DateV2Value<DateTimeV2ValueType>> {
    static const char* get() { return "DatetimeV2"; }
};

template <typename T>
struct TypeId;
template <>
struct TypeId<UInt8> {
    static constexpr const TypeIndex value = TypeIndex::UInt8;
};
template <>
struct TypeId<UInt16> {
    static constexpr const TypeIndex value = TypeIndex::UInt16;
};
template <>
struct TypeId<UInt32> {
    static constexpr const TypeIndex value = TypeIndex::UInt32;
};
template <>
struct TypeId<UInt64> {
    static constexpr const TypeIndex value = TypeIndex::UInt64;
};
template <>
struct TypeId<Int8> {
    static constexpr const TypeIndex value = TypeIndex::Int8;
};
template <>
struct TypeId<Int16> {
    static constexpr const TypeIndex value = TypeIndex::Int16;
};
template <>
struct TypeId<Int32> {
    static constexpr const TypeIndex value = TypeIndex::Int32;
};
template <>
struct TypeId<Int64> {
    static constexpr const TypeIndex value = TypeIndex::Int64;
};
template <>
struct TypeId<Float32> {
    static constexpr const TypeIndex value = TypeIndex::Float32;
};
template <>
struct TypeId<Float64> {
    static constexpr const TypeIndex value = TypeIndex::Float64;
};
template <>
struct TypeId<String> {
    static constexpr const TypeIndex value = TypeIndex::String;
};

/// Not a data type in database, defined just for convenience.
using Strings = std::vector<String>;

using IPv4 = uint32_t;
using IPv6 = uint128_t;

template <>
inline constexpr bool IsNumber<IPv6> = true;
template <>
struct TypeName<IPv6> {
    static const char* get() { return "IPv6"; }
};
template <>
struct TypeId<IPv6> {
    static constexpr const TypeIndex value = TypeIndex::IPv6;
};

using Int128 = __int128;

template <>
inline constexpr bool IsNumber<Int128> = true;
template <>
struct TypeName<Int128> {
    static const char* get() { return "Int128"; }
};
template <>
inline constexpr bool IsNumber<wide::Int256> = true;
template <>
struct TypeName<wide::Int256> {
    static const char* get() { return "Int256"; }
};
template <>
struct TypeId<Int128> {
    static constexpr const TypeIndex value = TypeIndex::Int128;
};

template <>
struct TypeId<wide::Int256> {
    static constexpr const TypeIndex value = TypeIndex::Int256;
};

using Date = Int64;
using DateTime = Int64;
using DateV2 = UInt32;
using DateTimeV2 = UInt64;

template <typename T>
inline constexpr T decimal_scale_multiplier(UInt32 scale);
template <>
inline constexpr Int32 decimal_scale_multiplier<Int32>(UInt32 scale) {
    return common::exp10_i32(scale);
}
template <>
inline constexpr Int64 decimal_scale_multiplier<Int64>(UInt32 scale) {
    return common::exp10_i64(scale);
}
template <>
inline constexpr Int128 decimal_scale_multiplier<Int128>(UInt32 scale) {
    return common::exp10_i128(scale);
}
// gcc report error if add constexpr in declaration
template <>
inline wide::Int256 decimal_scale_multiplier<wide::Int256>(UInt32 scale) {
    return common::exp10_i256(scale);
}
template <typename T>
std::string decimal_to_string(const T& value, UInt32 scale) {
    if (value == std::numeric_limits<T>::min()) {
        if constexpr (std::is_same_v<T, wide::Int256>) {
            std::string res {wide::to_string(value)};
            res.insert(res.size() - scale, ".");
            return res;
        } else {
            fmt::memory_buffer buffer;
            fmt::format_to(buffer, "{}", value);
            std::string res {buffer.data(), buffer.size()};
            res.insert(res.size() - scale, ".");
            return res;
        }
    }

    static constexpr auto precision =
            std::is_same_v<T, Int32>
                    ? BeConsts::MAX_DECIMAL32_PRECISION
                    : (std::is_same_v<T, Int64> ? BeConsts::MAX_DECIMAL64_PRECISION
                                                : (std::is_same_v<T, __int128>
                                                           ? BeConsts::MAX_DECIMAL128_PRECISION
                                                           : BeConsts::MAX_DECIMAL256_PRECISION));
    bool is_nagetive = value < 0;
    int max_result_length = precision + (scale > 0) // Add a space for decimal place
                            + (scale == precision)  // Add a space for leading 0
                            + (is_nagetive);        // Add a space for negative sign
    std::string str = std::string(max_result_length, '0');

    T abs_value = value;
    int pos = 0;

    if (is_nagetive) {
        abs_value = -value;
        str[pos++] = '-';
    }

    T whole_part = abs_value;
    T frac_part;
    if (scale) {
        whole_part = abs_value / decimal_scale_multiplier<T>(scale);
        frac_part = abs_value % decimal_scale_multiplier<T>(scale);
    }
    if constexpr (std::is_same_v<T, wide::Int256>) {
        std::string num_str {wide::to_string(whole_part)};
        auto end = fmt::format_to(str.data() + pos, "{}", num_str);
        pos = end - str.data();
    } else {
        auto end = fmt::format_to(str.data() + pos, "{}", whole_part);
        pos = end - str.data();
    }

    if (scale) {
        str[pos++] = '.';
        for (auto end_pos = pos + scale - 1; end_pos >= pos && frac_part > 0;
             --end_pos, frac_part /= 10) {
            str[end_pos] += (int)(frac_part % 10);
        }
    }

    str.resize(pos + scale);
    return str;
}

template <typename T>
std::string decimal_to_string(const T& orig_value, UInt32 trunc_precision, UInt32 scale) {
    T multiplier = decimal_scale_multiplier<T>(trunc_precision);
    T value = orig_value % multiplier;
    return decimal_to_string(value, scale);
}

template <typename T>
size_t decimal_to_string(const T& value, char* dst, UInt32 scale, const T& scale_multiplier) {
    if (UNLIKELY(value == std::numeric_limits<T>::min())) {
        if constexpr (std::is_same_v<T, wide::Int256>) {
            // handle scale?
            std::string num_str {wide::to_string(value)};
            auto* end = fmt::format_to(dst, "{}", num_str);
            return end - dst;
        } else {
            auto end = fmt::format_to(dst, "{}", value);
            return end - dst;
        }
    }

    bool is_negative = value < 0;
    T abs_value = value;
    int pos = 0;

    if (is_negative) {
        abs_value = -value;
        dst[pos++] = '-';
    }

    T whole_part = abs_value;
    T frac_part;
    if (LIKELY(scale)) {
        whole_part = abs_value / scale_multiplier;
        frac_part = abs_value % scale_multiplier;
    }
    if constexpr (std::is_same_v<T, wide::Int256>) {
        std::string num_str {wide::to_string(whole_part)};
        auto* end = fmt::format_to(dst + pos, "{}", num_str);
        pos = end - dst;
    } else {
        auto end = fmt::format_to(dst + pos, "{}", whole_part);
        pos = end - dst;
    }

    if (LIKELY(scale)) {
        int low_scale = 0;
        int high_scale = scale;
        while (low_scale < high_scale) {
            int mid_scale = (high_scale + low_scale) >> 1;
            const auto mid_scale_factor = decimal_scale_multiplier<T>(mid_scale);
            if (mid_scale_factor <= frac_part) {
                low_scale = mid_scale + 1;
            } else {
                high_scale = mid_scale;
            }
        }
        dst[pos++] = '.';
        if (low_scale < scale) {
            memset(&dst[pos], '0', scale - low_scale);
            pos += scale - low_scale;
        }
        if (frac_part) {
            if constexpr (std::is_same_v<T, wide::Int256>) {
                std::string num_str {wide::to_string(frac_part)};
                auto* end = fmt::format_to(&dst[pos], "{}", num_str);
                pos = end - dst;
            } else {
                auto end = fmt::format_to(&dst[pos], "{}", frac_part);
                pos = end - dst;
            }
        }
    }

    return pos;
}

template <typename T>
static constexpr int max_decimal_string_length() {
    constexpr auto precision =
            std::is_same_v<T, Int32>
                    ? BeConsts::MAX_DECIMAL32_PRECISION
                    : (std::is_same_v<T, Int64> ? BeConsts::MAX_DECIMAL64_PRECISION
                                                : (std::is_same_v<T, __int128>
                                                           ? BeConsts::MAX_DECIMAL128_PRECISION
                                                           : BeConsts::MAX_DECIMAL256_PRECISION));
    return precision + 1 // Add a space for decimal place
           + 1           // Add a space for leading 0
           + 1;          // Add a space for negative sign
}

template <typename T>
concept DecimalNativeTypeConcept = std::is_same_v<T, Int32> || std::is_same_v<T, Int64> ||
                                   std::is_same_v<T, Int128> || std::is_same_v<T, wide::Int256>;

/// Own FieldType for Decimal.
/// It is only a "storage" for decimal. To perform operations, you also have to provide a scale (number of digits after point).
template <DecimalNativeTypeConcept T>
struct Decimal {
    using NativeType = T;

    static constexpr bool IsInt256 = std::is_same_v<T, wide::Int256>;

    Decimal() = default;
    Decimal(Decimal<T>&&) = default;
    Decimal(const Decimal<T>&) = default;

    explicit(IsInt256) Decimal(Int32 value) noexcept : value(value) {}
    explicit(IsInt256) Decimal(Int64 value) noexcept : value(value) {}
    explicit(IsInt256) Decimal(Int128 value) noexcept : value(value) {}
    explicit(IsInt256) Decimal(IPv6 value) noexcept : value(value) {}
    explicit(IsInt256) Decimal(wide::Int256 value) noexcept : value(value) {}
    explicit(IsInt256) Decimal(UInt64 value) noexcept : value(value) {}
    explicit(IsInt256) Decimal(UInt32 value) noexcept : value(value) {}
    explicit(IsInt256) Decimal(Float32 value) noexcept : value(type_round(value)) {}
    explicit(IsInt256) Decimal(Float64 value) noexcept : value(type_round(value)) {}

    /// If T is integral, the given value will be rounded to integer.
    template <std::floating_point U>
    static constexpr T type_round(U value) noexcept {
        if constexpr (wide::IntegralConcept<T>()) {
            return T(round(value));
        }
        return T(value);
    }

    static Decimal double_to_decimal(double value_) {
        DecimalV2Value decimal_value;
        decimal_value.assign_from_double(value_);
        return Decimal(binary_cast<DecimalV2Value, T>(decimal_value));
    }

    static Decimal from_int_frac(T integer, T fraction, int scale) {
        return Decimal(integer * int_exp10(scale) + fraction);
    }

    template <typename U>
    Decimal(const Decimal<U>& x) {
        if constexpr (IsInt256) {
            value = x.value;
        } else {
            value = x;
        }
    }

    constexpr Decimal<T>& operator=(Decimal<T>&&) = default;
    constexpr Decimal<T>& operator=(const Decimal<T>&) = default;

    operator T() const { return value; }

    operator wide::Int256() const
        requires(!IsInt256)
    {
        wide::Int256 result;
        wide::Int256::_impl::wide_integer_from_builtin(result, value);
        return result;
    }

    operator Int128() const
        requires(IsInt256)
    {
        return (Int128)value.items[0] + ((Int128)(value.items[1]) << 64);
    }

    const Decimal<T>& operator++() {
        value++;
        return *this;
    }
    const Decimal<T>& operator--() {
        value--;
        return *this;
    }

    const Decimal<T>& operator+=(const T& x) {
        value += x;
        return *this;
    }
    const Decimal<T>& operator-=(const T& x) {
        value -= x;
        return *this;
    }
    const Decimal<T>& operator*=(const T& x) {
        value *= x;
        return *this;
    }
    const Decimal<T>& operator/=(const T& x) {
        value /= x;
        return *this;
    }
    const Decimal<T>& operator%=(const T& x) {
        value %= x;
        return *this;
    }

    auto operator<=>(const Decimal<T>& x) const
        requires(!Decimal<T>::IsInt256)
    {
        return value <=> x.value;
    }

    auto operator==(const Decimal<T>& x) const { return value == x.value; }

    static constexpr int max_string_length() { return max_decimal_string_length<T>(); }

    std::string to_string(UInt32 scale) const { return decimal_to_string(value, scale); }

    // truncate to specified precision and scale,
    // used by runtime filter only for now.
    std::string to_string(UInt32 precision, UInt32 scale) const {
        return decimal_to_string(value, precision, scale);
    }

    /**
     * Got the string representation of a decimal.
     * @param dst Store the result, should be pre-allocated.
     * @param scale Decimal's scale.
     * @param scale_multiplier Decimal's scale multiplier.
     * @return The length of string.
     */
    __attribute__((always_inline)) size_t to_string(char* dst, UInt32 scale,
                                                    const T& scale_multiplier) const {
        return decimal_to_string(value, dst, scale, scale_multiplier);
    }

    T value;
};

template <typename T>
inline Decimal<T> operator-(const Decimal<T>& x) {
    return Decimal<T>(-x.value);
}

template <typename T>
inline Decimal<T> operator+(const Decimal<T>& x, const Decimal<T>& y) {
    return Decimal<T>(x.value + y.value);
}
template <typename T>
inline Decimal<T> operator-(const Decimal<T>& x, const Decimal<T>& y) {
    return Decimal<T>(x.value - y.value);
}
template <typename T>
inline Decimal<T> operator*(const Decimal<T>& x, const Decimal<T>& y) {
    return Decimal<T>(x.value * y.value);
}
template <typename T>
inline Decimal<T> operator/(const Decimal<T>& x, const Decimal<T>& y) {
    return Decimal<T>(x.value / y.value);
}
template <typename T>
inline Decimal<T> operator%(const Decimal<T>& x, const Decimal<T>& y) {
    return Decimal<T>(x.value % y.value);
}

struct Decimal128V3 : public Decimal<Int128> {
    Decimal128V3() = default;

#define DECLARE_NUMERIC_CTOR(TYPE) \
    Decimal128V3(const TYPE& value_) : Decimal<Int128>(value_) {}

    DECLARE_NUMERIC_CTOR(wide::Int256)
    DECLARE_NUMERIC_CTOR(Int128)
    DECLARE_NUMERIC_CTOR(IPv6)
    DECLARE_NUMERIC_CTOR(Int32)
    DECLARE_NUMERIC_CTOR(Int64)
    DECLARE_NUMERIC_CTOR(UInt32)
    DECLARE_NUMERIC_CTOR(UInt64)
    DECLARE_NUMERIC_CTOR(Float32)
    DECLARE_NUMERIC_CTOR(Float64)
#undef DECLARE_NUMERIC_CTOR

    template <typename U>
    Decimal128V3(const Decimal<U>& x) {
        value = x;
    }
};

using Decimal32 = Decimal<Int32>;
using Decimal64 = Decimal<Int64>;
using Decimal128V2 = Decimal<Int128>;
using Decimal256 = Decimal<wide::Int256>;

inline bool operator<(const Decimal256& x, const Decimal256& y) {
    return x.value < y.value;
}
inline bool operator>(const Decimal256& x, const Decimal256& y) {
    return x.value > y.value;
}
inline bool operator<=(const Decimal256& x, const Decimal256& y) {
    return x.value <= y.value;
}
inline bool operator>=(const Decimal256& x, const Decimal256& y) {
    return x.value >= y.value;
}

template <>
struct TypeName<Decimal32> {
    static const char* get() { return "Decimal32"; }
};
template <>
struct TypeName<Decimal64> {
    static const char* get() { return "Decimal64"; }
};
template <>
struct TypeName<Decimal128V2> {
    static const char* get() { return "Decimal128V2"; }
};
template <>
struct TypeName<Decimal128V3> {
    static const char* get() { return "Decimal128V3"; }
};

template <>
struct TypeName<Decimal256> {
    static const char* get() { return "Decimal256"; }
};

template <>
struct TypeId<Decimal32> {
    static constexpr const TypeIndex value = TypeIndex::Decimal32;
};
template <>
struct TypeId<Decimal64> {
    static constexpr const TypeIndex value = TypeIndex::Decimal64;
};
template <>
struct TypeId<Decimal128V2> {
    static constexpr const TypeIndex value = TypeIndex::Decimal128V2;
};
template <>
struct TypeId<Decimal128V3> {
    static constexpr const TypeIndex value = TypeIndex::Decimal128V3;
};
template <>
struct TypeId<Decimal256> {
    static constexpr const TypeIndex value = TypeIndex::Decimal256;
};

template <typename T>
constexpr bool IsDecimalNumber = false;
template <>
inline constexpr bool IsDecimalNumber<Decimal32> = true;
template <>
inline constexpr bool IsDecimalNumber<Decimal64> = true;
template <>
inline constexpr bool IsDecimalNumber<Decimal128V2> = true;
template <>
inline constexpr bool IsDecimalNumber<Decimal128V3> = true;
template <>
inline constexpr bool IsDecimalNumber<Decimal256> = true;

template <typename T>
constexpr bool IsDecimal128V2 = false;
template <>
inline constexpr bool IsDecimal128V2<Decimal128V2> = true;

template <typename T>
constexpr bool IsDecimal128V3 = false;
template <>
inline constexpr bool IsDecimal128V3<Decimal128V3> = true;

template <typename T>
constexpr bool IsDecimal256 = false;
template <>
inline constexpr bool IsDecimal256<Decimal256> = true;

template <typename T>
constexpr bool IsDecimalV2 = IsDecimal128V2<T> && !IsDecimal128V3<T>;

template <typename T, typename U>
using DisposeDecimal = std::conditional_t<IsDecimalV2<T>, Decimal128V2,
                                          std::conditional_t<IsDecimalNumber<T>, Decimal128V3, U>>;

template <typename T, typename U>
using DisposeDecimal256 = std::conditional_t<IsDecimalV2<T>, Decimal128V2,
                                             std::conditional_t<IsDecimalNumber<T>, Decimal256, U>>;

template <typename T>
constexpr bool IsFloatNumber = false;
template <>
inline constexpr bool IsFloatNumber<Float32> = true;
template <>
inline constexpr bool IsFloatNumber<Float64> = true;

template <typename T>
struct NativeType {
    using Type = T;
};
template <>
struct NativeType<Decimal32> {
    using Type = Int32;
};
template <>
struct NativeType<Decimal64> {
    using Type = Int64;
};
template <>
struct NativeType<Decimal128V2> {
    using Type = Int128;
};
template <>
struct NativeType<Decimal128V3> {
    using Type = Int128;
};
template <>
struct NativeType<Decimal256> {
    using Type = wide::Int256;
};

inline const char* getTypeName(TypeIndex idx) {
    switch (idx) {
    case TypeIndex::Nothing:
        return "Nothing";
    case TypeIndex::UInt8:
        return TypeName<UInt8>::get();
    case TypeIndex::UInt16:
        return TypeName<UInt16>::get();
    case TypeIndex::UInt32:
        return TypeName<UInt32>::get();
    case TypeIndex::UInt64:
        return TypeName<UInt64>::get();
    case TypeIndex::UInt128:
        return "UInt128";
    case TypeIndex::Int8:
        return TypeName<Int8>::get();
    case TypeIndex::Int16:
        return TypeName<Int16>::get();
    case TypeIndex::Int32:
        return TypeName<Int32>::get();
    case TypeIndex::Int64:
        return TypeName<Int64>::get();
    case TypeIndex::Int128:
        return TypeName<Int128>::get();
    case TypeIndex::Int256:
        return TypeName<wide::Int256>::get();
    case TypeIndex::Float32:
        return TypeName<Float32>::get();
    case TypeIndex::Float64:
        return TypeName<Float64>::get();
    case TypeIndex::Date:
        return "Date";
    case TypeIndex::DateTime:
        return "DateTime";
    case TypeIndex::DateV2:
        return "DateV2";
    case TypeIndex::DateTimeV2:
        return "DateTimeV2";
    case TypeIndex::TimeV2:
        return "TimeV2";
    case TypeIndex::IPv4:
        return "IPv4";
    case TypeIndex::IPv6:
        return "IPv6";
    case TypeIndex::String:
        return TypeName<String>::get();
    case TypeIndex::FixedString:
        return "FixedString";
    case TypeIndex::Enum8:
        return "Enum8";
    case TypeIndex::Enum16:
        return "Enum16";
    case TypeIndex::Decimal32:
        return TypeName<Decimal32>::get();
    case TypeIndex::Decimal64:
        return TypeName<Decimal64>::get();
    case TypeIndex::Decimal128V2:
        return TypeName<Decimal128V2>::get();
    case TypeIndex::Decimal128V3:
        return TypeName<Decimal128V3>::get();
    case TypeIndex::Decimal256:
        return TypeName<Decimal256>::get();
    case TypeIndex::UUID:
        return "UUID";
    case TypeIndex::Array:
        return "Array";
    case TypeIndex::Tuple:
        return "Tuple";
    case TypeIndex::Map:
        return "Map";
    case TypeIndex::Set:
        return "Set";
    case TypeIndex::Interval:
        return "Interval";
    case TypeIndex::Nullable:
        return "Nullable";
    case TypeIndex::Function:
        return "Function";
    case TypeIndex::AggregateFunction:
        return "AggregateFunction";
    case TypeIndex::LowCardinality:
        return "LowCardinality";
    case TypeIndex::VARIANT:
        return "Variant";
    case TypeIndex::BitMap:
        return TypeName<BitmapValue>::get();
    case TypeIndex::HLL:
        return TypeName<HyperLogLog>::get();
    case TypeIndex::FixedLengthObject:
        return "FixedLengthObject";
    case TypeIndex::JSONB:
        return "JSONB";
    case TypeIndex::Struct:
        return "Struct";
    case TypeIndex::QuantileState:
        return TypeName<QuantileState>::get();
    case TypeIndex::AggState:
        return "AggState";
    case TypeIndex::Time:
        return "Time";
    }

    LOG(FATAL) << "__builtin_unreachable";
    __builtin_unreachable();
}
} // namespace vectorized
} // namespace doris

/// Specialization of `std::hash` for the Decimal<T> types.
template <typename T>
struct std::hash<doris::vectorized::Decimal<T>> {
    size_t operator()(const doris::vectorized::Decimal<T>& x) const { return hash<T>()(x.value); }
};

template <>
struct std::hash<doris::vectorized::Decimal128V2> {
    size_t operator()(const doris::vectorized::Decimal128V2& x) const {
        return std::hash<doris::vectorized::Int64>()(x.value >> 64) ^
               std::hash<doris::vectorized::Int64>()(
                       x.value & std::numeric_limits<doris::vectorized::UInt64>::max());
    }
};

template <>
struct std::hash<doris::vectorized::Decimal128V3> {
    size_t operator()(const doris::vectorized::Decimal128V3& x) const {
        return std::hash<doris::vectorized::Int64>()(x.value >> 64) ^
               std::hash<doris::vectorized::Int64>()(
                       x.value & std::numeric_limits<doris::vectorized::UInt64>::max());
    }
};

template <>
struct std::hash<doris::vectorized::Decimal256> {
    size_t operator()(const doris::vectorized::Decimal256& x) const {
        return std::hash<uint64_t>()(x.value >> 192) ^ std::hash<uint64_t>()(x.value >> 128) ^
               std::hash<uint64_t>()(x.value >> 64) ^
               std::hash<uint64_t>()(x.value & std::numeric_limits<uint64_t>::max());
    }
};

constexpr bool typeindex_is_int(doris::vectorized::TypeIndex index) {
    using TypeIndex = doris::vectorized::TypeIndex;
    switch (index) {
    case TypeIndex::UInt8:
    case TypeIndex::UInt16:
    case TypeIndex::UInt32:
    case TypeIndex::UInt64:
    case TypeIndex::UInt128:
    case TypeIndex::Int8:
    case TypeIndex::Int16:
    case TypeIndex::Int32:
    case TypeIndex::Int64:
    case TypeIndex::Int128: {
        return true;
    }
    default: {
        return false;
    }
    }
}
