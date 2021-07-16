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

#include <algorithm>
#include <cassert>
#include <functional>
#include <type_traits>
#include <vector>

#include "vec/common/exception.h"
#include "vec/common/int_exp.h"
#include "vec/common/strong_typedef.h"
#include "vec/common/uint128.h"
#include "vec/core/types.h"

namespace doris::vectorized {

template <typename T>
struct NearestFieldTypeImpl;

template <typename T>
using NearestFieldType = typename NearestFieldTypeImpl<T>::Type;

class Field;
using FieldVector = std::vector<Field>;

/// Array and Tuple use the same storage type -- FieldVector, but we declare
/// distinct types for them, so that the caller can choose whether it wants to
/// construct a Field of Array or a Tuple type. An alternative approach would be
/// to construct both of these types from FieldVector, and have the caller
/// specify the desired Field type explicitly.
#define DEFINE_FIELD_VECTOR(X)          \
    struct X : public FieldVector {     \
        using FieldVector::FieldVector; \
    }

DEFINE_FIELD_VECTOR(Array);
DEFINE_FIELD_VECTOR(Tuple);

#undef DEFINE_FIELD_VECTOR

struct AggregateFunctionStateData {
    String name; /// Name with arguments.
    String data;

    bool operator<(const AggregateFunctionStateData&) const {
        LOG(FATAL) << "Operator < is not implemented for AggregateFunctionStateData.";
    }

    bool operator<=(const AggregateFunctionStateData&) const {
        LOG(FATAL) << "Operator <= is not implemented for AggregateFunctionStateData.";
    }

    bool operator>(const AggregateFunctionStateData&) const {
        LOG(FATAL) << "Operator <= is not implemented for AggregateFunctionStateData.";
    }

    bool operator>=(const AggregateFunctionStateData&) const {
        LOG(FATAL) << "Operator >= is not implemented for AggregateFunctionStateData.";
    }

    bool operator==(const AggregateFunctionStateData& rhs) const {
        if (name != rhs.name) {
            LOG(FATAL) << fmt::format(
                    "Comparing aggregate functions with different types: {} and {}", name,
                    rhs.name);
        }

        return data == rhs.data;
    }
};

template <typename T>
bool decimal_equal(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T>
bool decimal_less(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T>
bool decimal_less_or_equal(T x, T y, UInt32 x_scale, UInt32 y_scale);

template <typename T>
class DecimalField {
public:
    DecimalField(T value, UInt32 scale_) : dec(value), scale(scale_) {}

    operator T() const { return dec; }
    T get_value() const { return dec; }
    T get_scale_multiplier() const;
    UInt32 get_scale() const { return scale; }

    template <typename U>
    bool operator<(const DecimalField<U>& r) const {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimal_less<MaxType>(dec, r.get_value(), scale, r.get_scale());
    }

    template <typename U>
    bool operator<=(const DecimalField<U>& r) const {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimal_less_or_equal<MaxType>(dec, r.get_value(), scale, r.get_scale());
    }

    template <typename U>
    bool operator==(const DecimalField<U>& r) const {
        using MaxType = std::conditional_t<(sizeof(T) > sizeof(U)), T, U>;
        return decimal_equal<MaxType>(dec, r.get_value(), scale, r.get_scale());
    }

    template <typename U>
    bool operator>(const DecimalField<U>& r) const {
        return r < *this;
    }
    template <typename U>
    bool operator>=(const DecimalField<U>& r) const {
        return r <= *this;
    }
    template <typename U>
    bool operator!=(const DecimalField<U>& r) const {
        return !(*this == r);
    }

    const DecimalField<T>& operator+=(const DecimalField<T>& r) {
        if (scale != r.get_scale()) {
            LOG(FATAL) << "Add different decimal fields";
        }
        dec += r.get_value();
        return *this;
    }

    const DecimalField<T>& operator-=(const DecimalField<T>& r) {
        if (scale != r.get_scale()) {
            LOG(FATAL) << "Sub different decimal fields";
        }
        dec -= r.get_value();
        return *this;
    }

private:
    T dec;
    UInt32 scale;
};

/** 32 is enough. Round number is used for alignment and for better arithmetic inside std::vector.
  * NOTE: Actually, sizeof(std::string) is 32 when using libc++, so Field is 40 bytes.
  */
#define DBMS_MIN_FIELD_SIZE 32

/** Discriminated union of several types.
  * Made for replacement of `boost::variant`
  *  is not generalized,
  *  but somewhat more efficient, and simpler.
  *
  * Used to represent a single value of one of several types in memory.
  * Warning! Prefer to use chunks of columns instead of single values. See Column.h
  */
class Field {
public:
    struct Types {
        /// Type tag.
        enum Which {
            Null = 0,
            UInt64 = 1,
            Int64 = 2,
            Float64 = 3,
            UInt128 = 4,
            Int128 = 5,

            /// Non-POD types.

            String = 16,
            Array = 17,
            Tuple = 18,
            Decimal32 = 19,
            Decimal64 = 20,
            Decimal128 = 21,
            AggregateFunctionState = 22,
        };

        static const int MIN_NON_POD = 16;

        static const char* to_string(Which which) {
            switch (which) {
            case Null:
                return "Null";
            case UInt64:
                return "UInt64";
            case UInt128:
                return "UInt128";
            case Int64:
                return "Int64";
            case Int128:
                return "Int128";
            case Float64:
                return "Float64";
            case String:
                return "String";
            case Array:
                return "Array";
            case Tuple:
                return "Tuple";
            case Decimal32:
                return "Decimal32";
            case Decimal64:
                return "Decimal64";
            case Decimal128:
                return "Decimal128";
            case AggregateFunctionState:
                return "AggregateFunctionState";
            }

            LOG(FATAL) << "Bad type of Field";
            return nullptr;
        }
    };

    /// Returns an identifier for the type or vice versa.
    template <typename T>
    struct TypeToEnum;
    template <Types::Which which>
    struct EnumToType;

    static bool is_decimal(Types::Which which) {
        return which >= Types::Decimal32 && which <= Types::Decimal128;
    }

    Field() : which(Types::Null) {}

    /** Despite the presence of a template constructor, this constructor is still needed,
      *  since, in its absence, the compiler will still generate the default constructor.
      */
    Field(const Field& rhs) { create(rhs); }

    Field(Field&& rhs) { create(std::move(rhs)); }

    template <typename T>
    Field(T&& rhs, std::enable_if_t<!std::is_same_v<std::decay_t<T>, Field>, void*> = nullptr);

    /// Create a string inplace.
    Field(const char* data, size_t size) { create(data, size); }

    Field(const unsigned char* data, size_t size) { create(data, size); }

    /// NOTE In case when field already has string type, more direct assign is possible.
    void assign_string(const char* data, size_t size) {
        destroy();
        create(data, size);
    }

    void assign_string(const unsigned char* data, size_t size) {
        destroy();
        create(data, size);
    }

    Field& operator=(const Field& rhs) {
        if (this != &rhs) {
            if (which != rhs.which) {
                destroy();
                create(rhs);
            } else
                assign(rhs); /// This assigns string or vector without deallocation of existing buffer.
        }
        return *this;
    }

    Field& operator=(Field&& rhs) {
        if (this != &rhs) {
            if (which != rhs.which) {
                destroy();
                create(std::move(rhs));
            } else
                assign(std::move(rhs));
        }
        return *this;
    }

    template <typename T>
    std::enable_if_t<!std::is_same_v<std::decay_t<T>, Field>, Field&> operator=(T&& rhs);

    ~Field() { destroy(); }

    Types::Which get_type() const { return which; }
    const char* get_type_name() const { return Types::to_string(which); }

    bool is_null() const { return which == Types::Null; }

    template <typename T>
    T& get() {
        using TWithoutRef = std::remove_reference_t<T>;
        TWithoutRef* MAY_ALIAS ptr = reinterpret_cast<TWithoutRef*>(&storage);
        return *ptr;
    }

    template <typename T>
    const T& get() const {
        using TWithoutRef = std::remove_reference_t<T>;
        const TWithoutRef* MAY_ALIAS ptr = reinterpret_cast<const TWithoutRef*>(&storage);
        return *ptr;
    }

    template <typename T>
    bool try_get(T& result) {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested) return false;
        result = get<T>();
        return true;
    }

    template <typename T>
    bool try_get(T& result) const {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        if (which != requested) return false;
        result = get<T>();
        return true;
    }

    template <typename T>
    T& safe_get() {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        CHECK_EQ(which, requested) << fmt::format("Bad get: has {}, requested {}", get_type_name(),
                                                  Types::to_string(requested));
        return get<T>();
    }

    template <typename T>
    const T& safe_get() const {
        const Types::Which requested = TypeToEnum<std::decay_t<T>>::value;
        CHECK_EQ(which, requested) << fmt::format("Bad get: has {}, requested {}", get_type_name(),
                                                  Types::to_string(requested));
        return get<T>();
    }

    bool operator<(const Field& rhs) const {
        if (which < rhs.which) return true;
        if (which > rhs.which) return false;

        switch (which) {
        case Types::Null:
            return false;
        case Types::UInt64:
            return get<UInt64>() < rhs.get<UInt64>();
        case Types::UInt128:
            return get<UInt128>() < rhs.get<UInt128>();
        case Types::Int64:
            return get<Int64>() < rhs.get<Int64>();
        case Types::Int128:
            return get<Int128>() < rhs.get<Int128>();
        case Types::Float64:
            return get<Float64>() < rhs.get<Float64>();
        case Types::String:
            return get<String>() < rhs.get<String>();
        case Types::Array:
            return get<Array>() < rhs.get<Array>();
        case Types::Tuple:
            return get<Tuple>() < rhs.get<Tuple>();
        case Types::Decimal32:
            return get<DecimalField<Decimal32>>() < rhs.get<DecimalField<Decimal32>>();
        case Types::Decimal64:
            return get<DecimalField<Decimal64>>() < rhs.get<DecimalField<Decimal64>>();
        case Types::Decimal128:
            return get<DecimalField<Decimal128>>() < rhs.get<DecimalField<Decimal128>>();
        case Types::AggregateFunctionState:
            return get<AggregateFunctionStateData>() < rhs.get<AggregateFunctionStateData>();
        }

        LOG(FATAL) << "Bad type of Field";
        return {};
    }

    bool operator>(const Field& rhs) const { return rhs < *this; }

    bool operator<=(const Field& rhs) const {
        if (which < rhs.which) return true;
        if (which > rhs.which) return false;

        switch (which) {
        case Types::Null:
            return true;
        case Types::UInt64:
            return get<UInt64>() <= rhs.get<UInt64>();
        case Types::UInt128:
            return get<UInt128>() <= rhs.get<UInt128>();
        case Types::Int64:
            return get<Int64>() <= rhs.get<Int64>();
        case Types::Int128:
            return get<Int128>() <= rhs.get<Int128>();
        case Types::Float64:
            return get<Float64>() <= rhs.get<Float64>();
        case Types::String:
            return get<String>() <= rhs.get<String>();
        case Types::Array:
            return get<Array>() <= rhs.get<Array>();
        case Types::Tuple:
            return get<Tuple>() <= rhs.get<Tuple>();
        case Types::Decimal32:
            return get<DecimalField<Decimal32>>() <= rhs.get<DecimalField<Decimal32>>();
        case Types::Decimal64:
            return get<DecimalField<Decimal64>>() <= rhs.get<DecimalField<Decimal64>>();
        case Types::Decimal128:
            return get<DecimalField<Decimal128>>() <= rhs.get<DecimalField<Decimal128>>();
        case Types::AggregateFunctionState:
            return get<AggregateFunctionStateData>() <= rhs.get<AggregateFunctionStateData>();
        }
        LOG(FATAL) << "Bad type of Field";
        return {};
    }

    bool operator>=(const Field& rhs) const { return rhs <= *this; }

    bool operator==(const Field& rhs) const {
        if (which != rhs.which) return false;

        switch (which) {
        case Types::Null:
            return true;
        case Types::UInt64:
        case Types::Int64:
        case Types::Float64:
            return get<UInt64>() == rhs.get<UInt64>();
        case Types::String:
            return get<String>() == rhs.get<String>();
        case Types::Array:
            return get<Array>() == rhs.get<Array>();
        case Types::Tuple:
            return get<Tuple>() == rhs.get<Tuple>();
        case Types::UInt128:
            return get<UInt128>() == rhs.get<UInt128>();
        case Types::Int128:
            return get<Int128>() == rhs.get<Int128>();
        case Types::Decimal32:
            return get<DecimalField<Decimal32>>() == rhs.get<DecimalField<Decimal32>>();
        case Types::Decimal64:
            return get<DecimalField<Decimal64>>() == rhs.get<DecimalField<Decimal64>>();
        case Types::Decimal128:
            return get<DecimalField<Decimal128>>() == rhs.get<DecimalField<Decimal128>>();
        case Types::AggregateFunctionState:
            return get<AggregateFunctionStateData>() == rhs.get<AggregateFunctionStateData>();
        }

        CHECK(false) << "Bad type of Field";
    }

    bool operator!=(const Field& rhs) const { return !(*this == rhs); }

private:
    std::aligned_union_t<DBMS_MIN_FIELD_SIZE - sizeof(Types::Which), Null, UInt64, UInt128, Int64,
                         Int128, Float64, String, Array, Tuple, DecimalField<Decimal32>,
                         DecimalField<Decimal64>, DecimalField<Decimal128>,
                         AggregateFunctionStateData>
            storage;

    Types::Which which;

    /// Assuming there was no allocated state or it was deallocated (see destroy).
    template <typename T>
    void create_concrete(T&& x) {
        using UnqualifiedType = std::decay_t<T>;

        // In both Field and PODArray, small types may be stored as wider types,
        // e.g. char is stored as UInt64. Field can return this extended value
        // with get<StorageType>(). To avoid uninitialized results from get(),
        // we must initialize the entire wide stored type, and not just the
        // nominal type.
        using StorageType = NearestFieldType<UnqualifiedType>;
        new (&storage) StorageType(std::forward<T>(x));
        which = TypeToEnum<UnqualifiedType>::value;
    }

    /// Assuming same types.
    template <typename T>
    void assign_concrete(T&& x) {
        using JustT = std::decay_t<T>;
        assert(which == TypeToEnum<JustT>::value);
        JustT* MAY_ALIAS ptr = reinterpret_cast<JustT*>(&storage);
        *ptr = std::forward<T>(x);
    }

    template <typename F,
              typename Field> /// Field template parameter may be const or non-const Field.
    static void dispatch(F&& f, Field& field) {
        switch (field.which) {
        case Types::Null:
            f(field.template get<Null>());
            return;

// gcc 7.3.0
#if !__clang__
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif
        case Types::UInt64:
            f(field.template get<UInt64>());
            return;
        case Types::UInt128:
            f(field.template get<UInt128>());
            return;
        case Types::Int64:
            f(field.template get<Int64>());
            return;
        case Types::Int128:
            f(field.template get<Int128>());
            return;
        case Types::Float64:
            f(field.template get<Float64>());
            return;
#if !__clang__
#pragma GCC diagnostic pop
#endif
        case Types::String:
            f(field.template get<String>());
            return;
        case Types::Array:
            f(field.template get<Array>());
            return;
        case Types::Tuple:
            f(field.template get<Tuple>());
            return;
        case Types::Decimal32:
            f(field.template get<DecimalField<Decimal32>>());
            return;
        case Types::Decimal64:
            f(field.template get<DecimalField<Decimal64>>());
            return;
        case Types::Decimal128:
            f(field.template get<DecimalField<Decimal128>>());
            return;
        case Types::AggregateFunctionState:
            f(field.template get<AggregateFunctionStateData>());
            return;
        }
    }

    void create(const Field& x) {
        dispatch([this](auto& value) { create_concrete(value); }, x);
    }

    void create(Field&& x) {
        dispatch([this](auto& value) { create_concrete(std::move(value)); }, x);
    }

    void assign(const Field& x) {
        dispatch([this](auto& value) { assign_concrete(value); }, x);
    }

    void assign(Field&& x) {
        dispatch([this](auto& value) { assign_concrete(std::move(value)); }, x);
    }

    void create(const char* data, size_t size) {
        new (&storage) String(data, size);
        which = Types::String;
    }

    void create(const unsigned char* data, size_t size) {
        create(reinterpret_cast<const char*>(data), size);
    }

    ALWAYS_INLINE void destroy() {
        if (which < Types::MIN_NON_POD) return;

        switch (which) {
        case Types::String:
            destroy<String>();
            break;
        case Types::Array:
            destroy<Array>();
            break;
        case Types::Tuple:
            destroy<Tuple>();
            break;
        case Types::AggregateFunctionState:
            destroy<AggregateFunctionStateData>();
            break;
        default:
            break;
        }

        which = Types::
                Null; /// for exception safety in subsequent calls to destroy and create, when create fails.
    }

    template <typename T>
    void destroy() {
        T* MAY_ALIAS ptr = reinterpret_cast<T*>(&storage);
        ptr->~T();
    }
};

#undef DBMS_MIN_FIELD_SIZE

template <>
struct Field::TypeToEnum<Null> {
    static const Types::Which value = Types::Null;
};
template <>
struct Field::TypeToEnum<UInt64> {
    static const Types::Which value = Types::UInt64;
};
template <>
struct Field::TypeToEnum<UInt128> {
    static const Types::Which value = Types::UInt128;
};
template <>
struct Field::TypeToEnum<Int64> {
    static const Types::Which value = Types::Int64;
};
template <>
struct Field::TypeToEnum<Int128> {
    static const Types::Which value = Types::Int128;
};
template <>
struct Field::TypeToEnum<Float64> {
    static const Types::Which value = Types::Float64;
};
template <>
struct Field::TypeToEnum<String> {
    static const Types::Which value = Types::String;
};
template <>
struct Field::TypeToEnum<Array> {
    static const Types::Which value = Types::Array;
};
template <>
struct Field::TypeToEnum<Tuple> {
    static const Types::Which value = Types::Tuple;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal32>> {
    static const Types::Which value = Types::Decimal32;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal64>> {
    static const Types::Which value = Types::Decimal64;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal128>> {
    static const Types::Which value = Types::Decimal128;
};
template <>
struct Field::TypeToEnum<AggregateFunctionStateData> {
    static const Types::Which value = Types::AggregateFunctionState;
};

template <>
struct Field::EnumToType<Field::Types::Null> {
    using Type = Null;
};
template <>
struct Field::EnumToType<Field::Types::UInt64> {
    using Type = UInt64;
};
template <>
struct Field::EnumToType<Field::Types::UInt128> {
    using Type = UInt128;
};
template <>
struct Field::EnumToType<Field::Types::Int64> {
    using Type = Int64;
};
template <>
struct Field::EnumToType<Field::Types::Int128> {
    using Type = Int128;
};
template <>
struct Field::EnumToType<Field::Types::Float64> {
    using Type = Float64;
};
template <>
struct Field::EnumToType<Field::Types::String> {
    using Type = String;
};
template <>
struct Field::EnumToType<Field::Types::Array> {
    using Type = Array;
};
template <>
struct Field::EnumToType<Field::Types::Tuple> {
    using Type = Tuple;
};
template <>
struct Field::EnumToType<Field::Types::Decimal32> {
    using Type = DecimalField<Decimal32>;
};
template <>
struct Field::EnumToType<Field::Types::Decimal64> {
    using Type = DecimalField<Decimal64>;
};
template <>
struct Field::EnumToType<Field::Types::Decimal128> {
    using Type = DecimalField<Decimal128>;
};
template <>
struct Field::EnumToType<Field::Types::AggregateFunctionState> {
    using Type = DecimalField<AggregateFunctionStateData>;
};

template <typename T>
T get(const Field& field) {
    return field.template get<T>();
}

template <typename T>
T get(Field& field) {
    return field.template get<T>();
}

template <typename T>
T safe_get(const Field& field) {
    return field.template safe_get<T>();
}

template <typename T>
T safe_get(Field& field) {
    return field.template safe_get<T>();
}

template <>
struct TypeName<Array> {
    static std::string get() { return "Array"; }
};
template <>
struct TypeName<Tuple> {
    static std::string get() { return "Tuple"; }
};
template <>
struct TypeName<AggregateFunctionStateData> {
    static std::string get() { return "AggregateFunctionState"; }
};

/// char may be signed or unsigned, and behave identically to signed char or unsigned char,
///  but they are always three different types.
/// signedness of char is different in Linux on x86 and Linux on ARM.
template <>
struct NearestFieldTypeImpl<char> {
    using Type = std::conditional_t<std::is_signed_v<char>, Int64, UInt64>;
};
template <>
struct NearestFieldTypeImpl<signed char> {
    using Type = Int64;
};
template <>
struct NearestFieldTypeImpl<unsigned char> {
    using Type = UInt64;
};

template <>
struct NearestFieldTypeImpl<UInt16> {
    using Type = UInt64;
};
template <>
struct NearestFieldTypeImpl<UInt32> {
    using Type = UInt64;
};

template <>
struct NearestFieldTypeImpl<UInt128> {
    using Type = UInt128;
};
//template <> struct NearestFieldTypeImpl<UUID> { using Type = UInt128; };
template <>
struct NearestFieldTypeImpl<Int16> {
    using Type = Int64;
};
template <>
struct NearestFieldTypeImpl<Int32> {
    using Type = Int64;
};

/// long and long long are always different types that may behave identically or not.
/// This is different on Linux and Mac.
template <>
struct NearestFieldTypeImpl<long> {
    using Type = Int64;
};
template <>
struct NearestFieldTypeImpl<long long> {
    using Type = Int64;
};
template <>
struct NearestFieldTypeImpl<unsigned long> {
    using Type = UInt64;
};
template <>
struct NearestFieldTypeImpl<unsigned long long> {
    using Type = UInt64;
};

template <>
struct NearestFieldTypeImpl<Int128> {
    using Type = Int128;
};
template <>
struct NearestFieldTypeImpl<Decimal32> {
    using Type = DecimalField<Decimal32>;
};
template <>
struct NearestFieldTypeImpl<Decimal64> {
    using Type = DecimalField<Decimal64>;
};
template <>
struct NearestFieldTypeImpl<Decimal128> {
    using Type = DecimalField<Decimal128>;
};
template <>
struct NearestFieldTypeImpl<DecimalField<Decimal32>> {
    using Type = DecimalField<Decimal32>;
};
template <>
struct NearestFieldTypeImpl<DecimalField<Decimal64>> {
    using Type = DecimalField<Decimal64>;
};
template <>
struct NearestFieldTypeImpl<DecimalField<Decimal128>> {
    using Type = DecimalField<Decimal128>;
};
template <>
struct NearestFieldTypeImpl<Float32> {
    using Type = Float64;
};
template <>
struct NearestFieldTypeImpl<Float64> {
    using Type = Float64;
};
template <>
struct NearestFieldTypeImpl<const char*> {
    using Type = String;
};
template <>
struct NearestFieldTypeImpl<String> {
    using Type = String;
};
template <>
struct NearestFieldTypeImpl<Array> {
    using Type = Array;
};
template <>
struct NearestFieldTypeImpl<Tuple> {
    using Type = Tuple;
};
template <>
struct NearestFieldTypeImpl<bool> {
    using Type = UInt64;
};
template <>
struct NearestFieldTypeImpl<Null> {
    using Type = Null;
};

template <>
struct NearestFieldTypeImpl<AggregateFunctionStateData> {
    using Type = AggregateFunctionStateData;
};

template <typename T>
decltype(auto) cast_to_nearest_field_type(T&& x) {
    using U = NearestFieldType<std::decay_t<T>>;
    if constexpr (std::is_same_v<std::decay_t<T>, U>)
        return std::forward<T>(x);
    else
        return U(x);
}

/// This (rather tricky) code is to avoid ambiguity in expressions like
/// Field f = 1;
/// instead of
/// Field f = Int64(1);
/// Things to note:
/// 1. float <--> int needs explicit cast
/// 2. customized types needs explicit cast
template <typename T>
Field::Field(T&& rhs, std::enable_if_t<!std::is_same_v<std::decay_t<T>, Field>, void*>) {
    auto&& val = cast_to_nearest_field_type(std::forward<T>(rhs));
    create_concrete(std::forward<decltype(val)>(val));
}

template <typename T>
std::enable_if_t<!std::is_same_v<std::decay_t<T>, Field>, Field&> Field::operator=(T&& rhs) {
    auto&& val = cast_to_nearest_field_type(std::forward<T>(rhs));
    using U = decltype(val);
    if (which != TypeToEnum<std::decay_t<U>>::value) {
        destroy();
        create_concrete(std::forward<U>(val));
    } else
        assign_concrete(std::forward<U>(val));

    return *this;
}

class ReadBuffer;
class WriteBuffer;

} // namespace doris::vectorized
