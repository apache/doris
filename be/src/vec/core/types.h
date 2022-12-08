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
#include <vector>

#include "util/binary_cast.hpp"

namespace doris {

class BitmapValue;
class HyperLogLog;
struct decimal12_t;
struct uint24_t;

namespace vectorized {

/// Data types for representing elementary values from a database in RAM.

struct Null {};

enum class TypeIndex {
    Nothing = 0,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Float32,
    Float64,
    Date,
    DateTime,
    String,
    FixedString,
    Enum8,
    Enum16,
    Decimal32,
    Decimal64,
    Decimal128,
    UUID,
    Array,
    Tuple,
    Set,
    Interval,
    Nullable,
    Function,
    AggregateFunction,
    LowCardinality,
    BitMap,
    HLL,
    DateV2,
    DateTimeV2,
    TimeV2,
    FixedLengthObject,
    JSONB,
    Decimal128I,
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
struct TypeName<StringValue> {
    static const char* get() { return "StringValue"; }
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

/// Not a data type in database, defined just for convenience.
using Strings = std::vector<String>;

using Int128 = __int128;
template <>
inline constexpr bool IsNumber<Int128> = true;
template <>
struct TypeName<Int128> {
    static const char* get() { return "Int128"; }
};
template <>
struct TypeId<Int128> {
    static constexpr const TypeIndex value = TypeIndex::Int128;
};

using Date = Int64;
using DateTime = Int64;
using DateV2 = UInt32;
using DateTimeV2 = UInt64;

struct Int128I {};

/// Own FieldType for Decimal.
/// It is only a "storage" for decimal. To perform operations, you also have to provide a scale (number of digits after point).
template <typename T>
struct Decimal {
    using NativeType = T;

    Decimal() = default;
    Decimal(Decimal<T>&&) = default;
    Decimal(const Decimal<T>&) = default;

#define DECLARE_NUMERIC_CTOR(TYPE) \
    Decimal(const TYPE& value_) : value(value_) {}

    DECLARE_NUMERIC_CTOR(Int128)
    DECLARE_NUMERIC_CTOR(Int32)
    DECLARE_NUMERIC_CTOR(Int64)
    DECLARE_NUMERIC_CTOR(UInt32)
    DECLARE_NUMERIC_CTOR(UInt64)
    DECLARE_NUMERIC_CTOR(Float32)
    DECLARE_NUMERIC_CTOR(Float64)
#undef DECLARE_NUMERIC_CTOR

    static Decimal double_to_decimal(double value_) {
        DecimalV2Value decimal_value;
        decimal_value.assign_from_double(value_);
        return Decimal(binary_cast<DecimalV2Value, T>(decimal_value));
    }

    template <typename U>
    Decimal(const Decimal<U>& x) {
        value = x;
    }

    constexpr Decimal<T>& operator=(Decimal<T>&&) = default;
    constexpr Decimal<T>& operator=(const Decimal<T>&) = default;

    operator T() const { return value; }

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

    T value;
};

template <>
struct Decimal<Int128I> : public Decimal<Int128> {
    Decimal() = default;

#define DECLARE_NUMERIC_CTOR(TYPE) \
    Decimal(const TYPE& value_) : Decimal<Int128>(value_) {}

    DECLARE_NUMERIC_CTOR(Int128)
    DECLARE_NUMERIC_CTOR(Int32)
    DECLARE_NUMERIC_CTOR(Int64)
    DECLARE_NUMERIC_CTOR(UInt32)
    DECLARE_NUMERIC_CTOR(UInt64)
    DECLARE_NUMERIC_CTOR(Float32)
    DECLARE_NUMERIC_CTOR(Float64)
#undef DECLARE_NUMERIC_CTOR

    template <typename U>
    Decimal(const Decimal<U>& x) {
        value = x;
    }
};

using Decimal32 = Decimal<Int32>;
using Decimal64 = Decimal<Int64>;
using Decimal128 = Decimal<Int128>;
using Decimal128I = Decimal<Int128I>;

template <>
struct TypeName<Decimal32> {
    static const char* get() { return "Decimal32"; }
};
template <>
struct TypeName<Decimal64> {
    static const char* get() { return "Decimal64"; }
};
template <>
struct TypeName<Decimal128> {
    static const char* get() { return "Decimal128"; }
};
template <>
struct TypeName<Decimal128I> {
    static const char* get() { return "Decimal128I"; }
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
struct TypeId<Decimal128> {
    static constexpr const TypeIndex value = TypeIndex::Decimal128;
};
template <>
struct TypeId<Decimal128I> {
    static constexpr const TypeIndex value = TypeIndex::Decimal128I;
};

template <typename T>
constexpr bool IsDecimalNumber = false;
template <>
inline constexpr bool IsDecimalNumber<Decimal32> = true;
template <>
inline constexpr bool IsDecimalNumber<Decimal64> = true;
template <>
inline constexpr bool IsDecimalNumber<Decimal128> = true;
template <>
inline constexpr bool IsDecimalNumber<Decimal128I> = true;

template <typename T>
constexpr bool IsDecimal128 = false;
template <>
inline constexpr bool IsDecimal128<Decimal128> = true;

template <typename T>
constexpr bool IsDecimal128I = false;
template <>
inline constexpr bool IsDecimal128I<Decimal128I> = true;

template <typename T>
constexpr bool IsDecimalV2 = IsDecimal128<T> && !IsDecimal128I<T>;

template <typename T, typename U>
using DisposeDecimal = std::conditional_t<IsDecimalV2<T>, Decimal128,
                                          std::conditional_t<IsDecimalNumber<T>, Decimal128I, U>>;

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
struct NativeType<Decimal128> {
    using Type = Int128;
};
template <>
struct NativeType<Decimal128I> {
    using Type = Int128;
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
    case TypeIndex::Decimal128:
        return TypeName<Decimal128>::get();
    case TypeIndex::Decimal128I:
        return TypeName<Decimal128I>::get();
    case TypeIndex::UUID:
        return "UUID";
    case TypeIndex::Array:
        return "Array";
    case TypeIndex::Tuple:
        return "Tuple";
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
    case TypeIndex::BitMap:
        return TypeName<BitmapValue>::get();
    case TypeIndex::HLL:
        return TypeName<HyperLogLog>::get();
    case TypeIndex::FixedLengthObject:
        return "FixedLengthObject";
    case TypeIndex::JSONB:
        return "JSONB";
    }

    __builtin_unreachable();
}
} // namespace vectorized
} // namespace doris

/// Specialization of `std::hash` for the Decimal<T> types.
namespace std {
template <typename T>
struct hash<doris::vectorized::Decimal<T>> {
    size_t operator()(const doris::vectorized::Decimal<T>& x) const { return hash<T>()(x.value); }
};

template <>
struct hash<doris::vectorized::Decimal128> {
    size_t operator()(const doris::vectorized::Decimal128& x) const {
        return std::hash<doris::vectorized::Int64>()(x.value >> 64) ^
               std::hash<doris::vectorized::Int64>()(
                       x.value & std::numeric_limits<doris::vectorized::UInt64>::max());
    }
};

constexpr bool is_integer(doris::vectorized::TypeIndex index) {
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
} // namespace std
