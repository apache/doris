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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Field.h
// and modified by Doris

#pragma once

#include <fmt/format.h>
#include <glog/logging.h>
#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <cassert>
#include <functional>
#include <map>
#include <new>
#include <ostream>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "olap/hll.h"
#include "util/bitmap_value.h"
#include "util/quantile_state.h"
#include "vec/common/uint128.h"
#include "vec/core/types.h"

namespace doris {
namespace vectorized {
template <typename T>
struct TypeId;
template <typename T>
struct TypeName;
} // namespace vectorized
struct PackedInt128;
} // namespace doris

namespace doris::vectorized {

template <typename T>
struct NearestFieldTypeImpl {
    using Type = T; // for HLL or some origin types. see def. of storage
};

template <typename T>
using NearestFieldType = typename NearestFieldTypeImpl<T>::Type;

template <typename T>
struct AvgNearestFieldTypeTrait {
    using Type = typename NearestFieldTypeImpl<T>::Type;
};

template <>
struct AvgNearestFieldTypeTrait<Decimal32> {
    using Type = Decimal128V3;
};

template <>
struct AvgNearestFieldTypeTrait<Decimal64> {
    using Type = Decimal128V3;
};

template <>
struct AvgNearestFieldTypeTrait<Decimal128V2> {
    using Type = Decimal128V2;
};

template <>
struct AvgNearestFieldTypeTrait<Decimal128V3> {
    using Type = Decimal128V3;
};

template <>
struct AvgNearestFieldTypeTrait<Decimal256> {
    using Type = Decimal256;
};

template <>
struct AvgNearestFieldTypeTrait<Int64> {
    using Type = double;
};

template <typename T>
struct AvgNearestFieldTypeTrait256 {
    using Type = typename NearestFieldTypeImpl<T>::Type;
};

template <>
struct AvgNearestFieldTypeTrait256<Decimal32> {
    using Type = Decimal256;
};

template <>
struct AvgNearestFieldTypeTrait256<Decimal64> {
    using Type = Decimal256;
};

template <>
struct AvgNearestFieldTypeTrait256<Decimal128V2> {
    using Type = Decimal128V2;
};

template <>
struct AvgNearestFieldTypeTrait256<Decimal128V3> {
    using Type = Decimal256;
};

template <>
struct AvgNearestFieldTypeTrait256<Decimal256> {
    using Type = Decimal256;
};

template <>
struct AvgNearestFieldTypeTrait256<Int64> {
    using Type = double;
};

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
DEFINE_FIELD_VECTOR(Map);
#undef DEFINE_FIELD_VECTOR

using FieldMap = std::map<String, Field, std::less<String>>;
#define DEFINE_FIELD_MAP(X)       \
    struct X : public FieldMap {  \
        using FieldMap::FieldMap; \
    }
DEFINE_FIELD_MAP(VariantMap);
#undef DEFINE_FIELD_MAP

class JsonbField {
public:
    JsonbField() = default;

    JsonbField(const char* ptr, uint32_t len) : size(len) {
        data = new char[size];
        if (!data) {
            LOG(FATAL) << "new data buffer failed, size: " << size;
        }
        memcpy(data, ptr, size);
    }

    JsonbField(const JsonbField& x) : size(x.size) {
        data = new char[size];
        if (!data) {
            LOG(FATAL) << "new data buffer failed, size: " << size;
        }
        memcpy(data, x.data, size);
    }

    JsonbField(JsonbField&& x) : data(x.data), size(x.size) {
        x.data = nullptr;
        x.size = 0;
    }

    JsonbField& operator=(const JsonbField& x) {
        data = new char[size];
        if (!data) {
            LOG(FATAL) << "new data buffer failed, size: " << size;
        }
        memcpy(data, x.data, size);
        return *this;
    }

    JsonbField& operator=(JsonbField&& x) {
        if (data) {
            delete[] data;
        }
        data = x.data;
        size = x.size;
        x.data = nullptr;
        x.size = 0;
        return *this;
    }

    ~JsonbField() {
        if (data) {
            delete[] data;
        }
    }

    const char* get_value() const { return data; }
    uint32_t get_size() const { return size; }

    bool operator<(const JsonbField& r) const {
        LOG(FATAL) << "comparing between JsonbField is not supported";
        __builtin_unreachable();
    }
    bool operator<=(const JsonbField& r) const {
        LOG(FATAL) << "comparing between JsonbField is not supported";
        __builtin_unreachable();
    }
    bool operator==(const JsonbField& r) const {
        LOG(FATAL) << "comparing between JsonbField is not supported";
        __builtin_unreachable();
    }
    bool operator>(const JsonbField& r) const {
        LOG(FATAL) << "comparing between JsonbField is not supported";
        __builtin_unreachable();
    }
    bool operator>=(const JsonbField& r) const {
        LOG(FATAL) << "comparing between JsonbField is not supported";
        __builtin_unreachable();
    }
    bool operator!=(const JsonbField& r) const {
        LOG(FATAL) << "comparing between JsonbField is not supported";
        __builtin_unreachable();
    }

    const JsonbField& operator+=(const JsonbField& r) {
        LOG(FATAL) << "Not support plus opration on JsonbField";
        __builtin_unreachable();
    }

    const JsonbField& operator-=(const JsonbField& r) {
        LOG(FATAL) << "Not support minus opration on JsonbField";
        __builtin_unreachable();
    }

private:
    char* data = nullptr;
    uint32_t size = 0;
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
            __builtin_unreachable();
        }
        dec += r.get_value();
        return *this;
    }

    const DecimalField<T>& operator-=(const DecimalField<T>& r) {
        if (scale != r.get_scale()) {
            LOG(FATAL) << "Sub different decimal fields";
            __builtin_unreachable();
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
            FixedLengthObject = 6,
            IPv6 = 7,

            /// Non-POD types.

            String = 16,
            Array = 17,
            Tuple = 18,
            Decimal32 = 19,
            Decimal64 = 20,
            Decimal128V2 = 21,
            AggregateFunctionState = 22,
            JSONB = 23,
            Decimal128V3 = 24,
            Map = 25,
            VariantMap = 26,
            Bitmap = 27,
            HyperLogLog = 28,
            QuantileState = 29,
            Int256 = 30,
            Decimal256 = 31
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
            case JSONB:
                return "JSONB";
            case Array:
                return "Array";
            case Tuple:
                return "Tuple";
            case Map:
                return "Map";
            case Decimal32:
                return "Decimal32";
            case Decimal64:
                return "Decimal64";
            case Decimal128V2:
                return "Decimal128V2";
            case Decimal128V3:
                return "Decimal128V3";
            case Decimal256:
                return "Decimal256";
            case FixedLengthObject:
                return "FixedLengthObject";
            case VariantMap:
                return "VariantMap";
            case Bitmap:
                return "Bitmap";
            case HyperLogLog:
                return "HyperLogLog";
            case QuantileState:
                return "QuantileState";
            case IPv6:
                return "IPv6";
            default:
                LOG(FATAL) << "type not supported, type=" << Types::to_string(which);
                break;
            }
            __builtin_unreachable();
        }
    };

    /// Returns an identifier for the type or vice versa.
    template <typename T>
    struct TypeToEnum;
    template <Types::Which which>
    struct EnumToType;

    static bool is_decimal(Types::Which which) {
        return (which >= Types::Decimal32 && which <= Types::Decimal128V2) ||
               which == Types::Decimal128V3 || which == Types::Decimal256;
    }

    Field() : which(Types::Null) {}

    // set Types::Null explictly and avoid other types
    Field(Types::Which w) : which(w) { DCHECK_EQ(Types::Null, which); }

    /** Despite the presence of a template constructor, this constructor is still needed,
      *  since, in its absence, the compiler will still generate the default constructor.
      */
    Field(const Field& rhs) { create(rhs); }

    Field(Field&& rhs) { create(std::move(rhs)); }

    template <typename T>
        requires(!std::is_same_v<std::decay_t<T>, Field>)
    Field(T&& rhs);

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

    void assign_jsonb(const char* data, size_t size) {
        destroy();
        create_jsonb(data, size);
    }

    void assign_jsonb(const unsigned char* data, size_t size) {
        destroy();
        create_jsonb(data, size);
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

    bool is_complex_field() const {
        return which == Types::Array || which == Types::Map || which == Types::Tuple ||
               which == Types::VariantMap;
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
        requires(!std::is_same_v<std::decay_t<T>, Field>)
    Field& operator=(T&& rhs);

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

    bool operator==(const Field& rhs) const {
        return operator<=>(rhs) == std::strong_ordering::equal;
    }

    std::strong_ordering operator<=>(const Field& rhs) const {
        if (which == Types::Null || rhs == Types::Null) {
            return which <=> rhs.which;
        }
        if (which != rhs.which) {
            LOG(FATAL) << "lhs type not equal with rhs, lhs=" << Types::to_string(which)
                       << ", rhs=" << Types::to_string(rhs.which);
        }

        switch (which) {
        case Types::Bitmap:
        case Types::HyperLogLog:
        case Types::QuantileState:
        case Types::FixedLengthObject:
        case Types::JSONB:
        case Types::Null:
        case Types::Array:
        case Types::Tuple:
        case Types::Map:
        case Types::VariantMap:
            return std::strong_ordering::equal;
        case Types::UInt64:
            return get<UInt64>() <=> rhs.get<UInt64>();
        case Types::UInt128:
            return get<UInt128>() <=> rhs.get<UInt128>();
        case Types::Int64:
            return get<Int64>() <=> rhs.get<Int64>();
        case Types::Int128:
            return get<Int128>() <=> rhs.get<Int128>();
        case Types::IPv6:
            return get<IPv6>() <=> rhs.get<IPv6>();
        case Types::Float64:
            return get<Float64>() < rhs.get<Float64>()    ? std::strong_ordering::less
                   : get<Float64>() == rhs.get<Float64>() ? std::strong_ordering::equal
                                                          : std::strong_ordering::greater;
        case Types::String:
            return get<String>() <=> rhs.get<String>();
        case Types::Decimal32:
            return get<Decimal32>() <=> rhs.get<Decimal32>();
        case Types::Decimal64:
            return get<Decimal64>() <=> rhs.get<Decimal64>();
        case Types::Decimal128V2:
            return get<Decimal128V2>() <=> rhs.get<Decimal128V2>();
        case Types::Decimal128V3:
            return get<Decimal128V3>() <=> rhs.get<Decimal128V3>();
        case Types::Decimal256:
            return get<Decimal256>() <=> rhs.get<Decimal256>();
        default:
            LOG(FATAL) << "lhs type not equal with rhs, lhs=" << Types::to_string(which)
                       << ", rhs=" << Types::to_string(rhs.which);
            break;
        }
    }

    template <typename F,
              typename Field> /// Field template parameter may be const or non-const Field.
    static void dispatch(F&& f, Field& field) {
        switch (field.which) {
        case Types::Null:
            f(field.template get<Null>());
            return;
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
        case Types::IPv6:
            f(field.template get<IPv6>());
            return;
        case Types::Float64:
            f(field.template get<Float64>());
            return;
        case Types::String:
            f(field.template get<String>());
            return;
        case Types::JSONB:
            f(field.template get<JsonbField>());
            return;
        case Types::Array:
            f(field.template get<Array>());
            return;
        case Types::Tuple:
            f(field.template get<Tuple>());
            return;
        case Types::Map:
            f(field.template get<Map>());
            return;
        case Types::Decimal32:
            f(field.template get<DecimalField<Decimal32>>());
            return;
        case Types::Decimal64:
            f(field.template get<DecimalField<Decimal64>>());
            return;
        case Types::Decimal128V2:
            f(field.template get<DecimalField<Decimal128V2>>());
            return;
        case Types::Decimal128V3:
            f(field.template get<DecimalField<Decimal128V3>>());
            return;
        case Types::Decimal256:
            f(field.template get<DecimalField<Decimal256>>());
            return;
        case Types::VariantMap:
            f(field.template get<VariantMap>());
            return;
        case Types::Bitmap:
            f(field.template get<BitmapValue>());
            return;
        case Types::HyperLogLog:
            f(field.template get<HyperLogLog>());
            return;
        case Types::QuantileState:
            f(field.template get<QuantileState>());
            return;
        default:
            LOG(FATAL) << "type not supported, type=" << Types::to_string(field.which);
            break;
        }
    }

private:
    std::aligned_union_t<DBMS_MIN_FIELD_SIZE - sizeof(Types::Which), Null, UInt64, UInt128, Int64,
                         Int128, IPv6, Float64, String, JsonbField, Array, Tuple, Map, VariantMap,
                         DecimalField<Decimal32>, DecimalField<Decimal64>,
                         DecimalField<Decimal128V2>, DecimalField<Decimal128V3>,
                         DecimalField<Decimal256>, BitmapValue, HyperLogLog, QuantileState>
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

private:
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

    void create_jsonb(const char* data, size_t size) {
        new (&storage) JsonbField(data, size);
        which = Types::JSONB;
    }

    void create_jsonb(const unsigned char* data, size_t size) {
        new (&storage) JsonbField(reinterpret_cast<const char*>(data), size);
        which = Types::JSONB;
    }

    ALWAYS_INLINE void destroy() {
        if (which < Types::MIN_NON_POD) {
            return;
        }

        switch (which) {
        case Types::String:
            destroy<String>();
            break;
        case Types::JSONB:
            destroy<JsonbField>();
            break;
        case Types::Array:
            destroy<Array>();
            break;
        case Types::Tuple:
            destroy<Tuple>();
            break;
        case Types::Map:
            destroy<Map>();
            break;
        case Types::VariantMap:
            destroy<VariantMap>();
            break;
        case Types::Bitmap:
            destroy<BitmapValue>();
            break;
        case Types::HyperLogLog:
            destroy<HyperLogLog>();
            break;
        case Types::QuantileState:
            destroy<QuantileState>();
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
struct TypeId<Tuple> {
    static constexpr const TypeIndex value = TypeIndex::Tuple;
};
template <>
struct TypeId<DecimalField<Decimal32>> {
    static constexpr const TypeIndex value = TypeIndex::Decimal32;
};
template <>
struct TypeId<DecimalField<Decimal64>> {
    static constexpr const TypeIndex value = TypeIndex::Decimal64;
};
template <>
struct TypeId<DecimalField<Decimal128V2>> {
    static constexpr const TypeIndex value = TypeIndex::Decimal128V2;
};
template <>
struct TypeId<DecimalField<Decimal128V3>> {
    static constexpr const TypeIndex value = TypeIndex::Decimal128V3;
};
template <>
struct TypeId<DecimalField<Decimal256>> {
    static constexpr const TypeIndex value = TypeIndex::Decimal256;
};
template <>
struct Field::TypeToEnum<Null> {
    static constexpr Types::Which value = Types::Null;
};
template <>
struct Field::TypeToEnum<UInt64> {
    static constexpr Types::Which value = Types::UInt64;
};
template <>
struct Field::TypeToEnum<UInt128> {
    static constexpr Types::Which value = Types::UInt128;
};
template <>
struct Field::TypeToEnum<Int64> {
    static constexpr Types::Which value = Types::Int64;
};
template <>
struct Field::TypeToEnum<Int128> {
    static constexpr Types::Which value = Types::Int128;
};
template <>
struct Field::TypeToEnum<wide::Int256> {
    static constexpr Types::Which value = Types::Int256;
};
template <>
struct Field::TypeToEnum<Float64> {
    static constexpr Types::Which value = Types::Float64;
};
template <>
struct Field::TypeToEnum<IPv6> {
    static constexpr Types::Which value = Types::IPv6;
};
template <>
struct Field::TypeToEnum<String> {
    static constexpr Types::Which value = Types::String;
};
template <>
struct Field::TypeToEnum<JsonbField> {
    static const Types::Which value = Types::JSONB;
};
template <>
struct Field::TypeToEnum<Array> {
    static constexpr Types::Which value = Types::Array;
};
template <>
struct Field::TypeToEnum<Tuple> {
    static constexpr Types::Which value = Types::Tuple;
};
template <>
struct Field::TypeToEnum<Map> {
    static const Types::Which value = Types::Map;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal32>> {
    static constexpr Types::Which value = Types::Decimal32;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal64>> {
    static constexpr Types::Which value = Types::Decimal64;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal128V2>> {
    static constexpr Types::Which value = Types::Decimal128V2;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal128V3>> {
    static constexpr Types::Which value = Types::Decimal128V3;
};
template <>
struct Field::TypeToEnum<DecimalField<Decimal256>> {
    static constexpr Types::Which value = Types::Decimal256;
};
template <>
struct Field::TypeToEnum<VariantMap> {
    static constexpr Types::Which value = Types::VariantMap;
};

template <>
struct Field::TypeToEnum<BitmapValue> {
    static constexpr Types::Which value = Types::Bitmap;
};

template <>
struct Field::TypeToEnum<HyperLogLog> {
    static constexpr Types::Which value = Types::HyperLogLog;
};

template <>
struct Field::TypeToEnum<QuantileState> {
    static constexpr Types::Which value = Types::QuantileState;
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
struct Field::EnumToType<Field::Types::IPv6> {
    using Type = IPv6;
};
template <>
struct Field::EnumToType<Field::Types::String> {
    using Type = String;
};
template <>
struct Field::EnumToType<Field::Types::JSONB> {
    using Type = JsonbField;
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
struct Field::EnumToType<Field::Types::Map> {
    using Type = Map;
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
struct Field::EnumToType<Field::Types::Decimal128V2> {
    using Type = DecimalField<Decimal128V2>;
};
template <>
struct Field::EnumToType<Field::Types::Decimal128V3> {
    using Type = DecimalField<Decimal128V3>;
};
template <>
struct Field::EnumToType<Field::Types::Decimal256> {
    using Type = DecimalField<Decimal256>;
};
template <>
struct Field::EnumToType<Field::Types::VariantMap> {
    using Type = VariantMap;
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
struct TypeName<VariantMap> {
    static std::string get() { return "VariantMap"; }
};
template <>
struct TypeName<Map> {
    static std::string get() { return "Map"; }
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
    using Type = Int64;
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
struct NearestFieldTypeImpl<Decimal32> {
    using Type = DecimalField<Decimal32>;
};
template <>
struct NearestFieldTypeImpl<Decimal64> {
    using Type = DecimalField<Decimal64>;
};
template <>
struct NearestFieldTypeImpl<Decimal128V2> {
    using Type = DecimalField<Decimal128V2>;
};
template <>
struct NearestFieldTypeImpl<Decimal128V3> {
    using Type = DecimalField<Decimal128V3>;
};
template <>
struct NearestFieldTypeImpl<Decimal256> {
    using Type = DecimalField<Decimal256>;
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
struct NearestFieldTypeImpl<DecimalField<Decimal128V2>> {
    using Type = DecimalField<Decimal128V2>;
};
template <>
struct NearestFieldTypeImpl<DecimalField<Decimal128V3>> {
    using Type = DecimalField<Decimal128V3>;
};
template <>
struct NearestFieldTypeImpl<DecimalField<Decimal256>> {
    using Type = DecimalField<Decimal256>;
};
template <>
struct NearestFieldTypeImpl<Float32> {
    using Type = Float64;
};
template <>
struct NearestFieldTypeImpl<const char*> {
    using Type = String;
};
template <>
struct NearestFieldTypeImpl<bool> {
    using Type = UInt64;
};

template <>
struct NearestFieldTypeImpl<std::string_view> {
    using Type = String;
};

template <>
struct Field::TypeToEnum<PackedInt128> {
    static const Types::Which value = Types::Int128;
};

template <>
struct NearestFieldTypeImpl<PackedInt128> {
    using Type = Int128;
};

template <typename T>
decltype(auto) cast_to_nearest_field_type(T&& x) {
    using U = NearestFieldType<std::decay_t<T>>;
    if constexpr (std::is_same_v<PackedInt128, std::decay_t<T>>) {
        return U(x.value);
    } else if constexpr (std::is_same_v<std::decay_t<T>, U>) {
        return std::forward<T>(x);
    } else {
        return U(x);
    }
}

/// This (rather tricky) code is to avoid ambiguity in expressions like
/// Field f = 1;
/// instead of
/// Field f = Int64(1);
/// Things to note:
/// 1. float <--> int needs explicit cast
/// 2. customized types needs explicit cast
template <typename T>
    requires(!std::is_same_v<std::decay_t<T>, Field>)
Field::Field(T&& rhs) {
    auto&& val = cast_to_nearest_field_type(std::forward<T>(rhs));
    create_concrete(std::forward<decltype(val)>(val));
}

template <typename T>
    requires(!std::is_same_v<std::decay_t<T>, Field>)
Field& Field::operator=(T&& rhs) {
    auto&& val = cast_to_nearest_field_type(std::forward<T>(rhs));
    using U = decltype(val);
    if (which != TypeToEnum<std::decay_t<U>>::value) {
        destroy();
        create_concrete(std::forward<U>(val));
    } else {
        assign_concrete(std::forward<U>(val));
    }

    return *this;
}

} // namespace doris::vectorized
