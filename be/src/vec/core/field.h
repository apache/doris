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

#include <algorithm>
#include <cassert>
#include <cstring>
#include <map>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "olap/hll.h"
#include "runtime/primitive_type.h"
#include "util/bitmap_value.h"
#include "util/quantile_state.h"
#include "vec/common/string_view.h"
#include "vec/common/uint128.h"
#include "vec/core/types.h"
#include "vec/json/path_in_data.h"

namespace doris {
template <PrimitiveType type>
struct PrimitiveTypeTraits;
namespace vectorized {
template <typename T>
struct TypeName;
} // namespace vectorized
struct PackedInt128;
} // namespace doris

namespace doris::vectorized {

class Field;

using FieldVector = std::vector<Field>;

/// Array and Tuple use the same storage type -- FieldVector, but we declare
/// distinct types for them, so that the caller can choose whether it wants to
/// construct a Field of Array or a Tuple type. An alternative approach would be
/// to construct both of these types from FieldVector, and have the caller
/// specify the desired Field type explicitly.
struct Array : public FieldVector {
    using FieldVector::FieldVector;
};

struct Tuple : public FieldVector {
    using FieldVector::FieldVector;
};

struct Map : public FieldVector {
    using FieldVector::FieldVector;
};

struct FieldWithDataType;

using VariantMap = std::map<PathInData, FieldWithDataType>;

//TODO: rethink if we really need this? it only save one pointer from std::string
// not POD type so could only use read/write_json_binary instead of read/write_binary
class JsonbField {
public:
    JsonbField() = default;
    ~JsonbField() = default; // unique_ptr will handle cleanup automatically

    JsonbField(const char* ptr, size_t len) : size(len) {
        data = std::make_unique<char[]>(size);
        if (!data) {
            throw Exception(Status::FatalError("new data buffer failed, size: {}", size));
        }
        if (size > 0) {
            memcpy(data.get(), ptr, size);
        }
    }

    JsonbField(const JsonbField& x) : size(x.size) {
        data = std::make_unique<char[]>(size);
        if (!data) {
            throw Exception(Status::FatalError("new data buffer failed, size: {}", size));
        }
        if (size > 0) {
            memcpy(data.get(), x.data.get(), size);
        }
    }

    JsonbField(JsonbField&& x) noexcept : data(std::move(x.data)), size(x.size) { x.size = 0; }

    // dispatch for all type of storage. so need this. but not really used now.
    JsonbField& operator=(const JsonbField& x) {
        if (this != &x) {
            data = std::make_unique<char[]>(x.size);
            if (!data) {
                throw Exception(Status::FatalError("new data buffer failed, size: {}", x.size));
            }
            if (x.size > 0) {
                memcpy(data.get(), x.data.get(), x.size);
            }
            size = x.size;
        }
        return *this;
    }

    JsonbField& operator=(JsonbField&& x) noexcept {
        if (this != &x) {
            data = std::move(x.data);
            size = x.size;
            x.size = 0;
        }
        return *this;
    }

    const char* get_value() const { return data.get(); }
    size_t get_size() const { return size; }

    bool operator<(const JsonbField& r) const {
        throw Exception(Status::FatalError("comparing between JsonbField is not supported"));
    }
    bool operator<=(const JsonbField& r) const {
        throw Exception(Status::FatalError("comparing between JsonbField is not supported"));
    }
    bool operator==(const JsonbField& r) const {
        throw Exception(Status::FatalError("comparing between JsonbField is not supported"));
    }
    bool operator>(const JsonbField& r) const {
        throw Exception(Status::FatalError("comparing between JsonbField is not supported"));
    }
    bool operator>=(const JsonbField& r) const {
        throw Exception(Status::FatalError("comparing between JsonbField is not supported"));
    }
    bool operator!=(const JsonbField& r) const {
        throw Exception(Status::FatalError("comparing between JsonbField is not supported"));
    }

    const JsonbField& operator+=(const JsonbField& r) {
        throw Exception(Status::FatalError("Not support plus opration on JsonbField"));
    }

    const JsonbField& operator-=(const JsonbField& r) {
        throw Exception(Status::FatalError("Not support minus opration on JsonbField"));
    }

private:
    std::unique_ptr<char[]> data = nullptr;
    size_t size = 0;
};

template <typename T>
bool decimal_equal(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T>
bool decimal_less(T x, T y, UInt32 x_scale, UInt32 y_scale);
template <typename T>
bool decimal_less_or_equal(T x, T y, UInt32 x_scale, UInt32 y_scale);

/** 32 is enough. Round number is used for alignment and for better arithmetic inside std::vector.
  * NOTE: Actually, sizeof(std::string) is 32 when using libc++, so Field is 40 bytes.
  */
constexpr size_t DBMS_MIN_FIELD_SIZE = 32;

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
    static const int MIN_NON_POD = 16;
    Field() : type(PrimitiveType::TYPE_NULL) {}
    // set Types::Null explictly and avoid other types
    Field(PrimitiveType w) : type(w) {}
    template <PrimitiveType T>
    static Field create_field(const typename PrimitiveTypeTraits<T>::CppType& data) {
        auto f = Field(T);
        f.template create_concrete<T>(data);
        return f;
    }
    template <PrimitiveType T>
    static Field create_field(typename PrimitiveTypeTraits<T>::CppType&& data) {
        auto f = Field(T);
        f.template create_concrete<T>(std::move(data));
        return f;
    }

    template <PrimitiveType PType, typename ValType = std::conditional_t<
                                           doris::is_string_type(PType), StringRef,
                                           typename PrimitiveTypeTraits<PType>::StorageFieldType>>
    static Field create_field_from_olap_value(const ValType& data) {
        auto f = Field(PType);
        typename PrimitiveTypeTraits<PType>::CppType cpp_value;
        if constexpr (is_string_type(PType)) {
            auto min_size =
                    MAX_ZONE_MAP_INDEX_SIZE >= data.size ? data.size : MAX_ZONE_MAP_INDEX_SIZE;
            cpp_value = String(data.data, min_size);
        } else if constexpr (is_date_or_datetime(PType)) {
            if constexpr (PType == TYPE_DATE) {
                cpp_value.from_olap_date(data);
            } else {
                cpp_value.from_olap_datetime(data);
            }
        } else if constexpr (is_decimalv2(PType)) {
            cpp_value = DecimalV2Value(data.integer, data.fraction);
        } else {
            cpp_value = typename PrimitiveTypeTraits<PType>::CppType(data);
        }
        f.template create_concrete<PType>(std::move(cpp_value));
        return f;
    }

    /** Despite the presence of a template constructor, this constructor is still needed,
      *  since, in its absence, the compiler will still generate the default constructor.
      */
    Field(const Field& rhs);

    Field(Field&& rhs);

    Field& operator=(const Field& rhs);

    bool is_complex_field() const {
        return type == PrimitiveType::TYPE_ARRAY || type == PrimitiveType::TYPE_MAP ||
               type == PrimitiveType::TYPE_STRUCT || type == PrimitiveType::TYPE_VARIANT;
    }

    Field& operator=(Field&& rhs) {
        if (this != &rhs) {
            if (type != rhs.type) {
                destroy();
                create(std::move(rhs));
            } else {
                assign(std::move(rhs));
            }
        }
        return *this;
    }

    ~Field() { destroy(); }

    PrimitiveType get_type() const { return type; }
    std::string get_type_name() const;

    bool is_null() const { return type == PrimitiveType::TYPE_NULL; }

    // The template parameter T needs to be consistent with `which`.
    // If not, use NearestFieldType<> externally.
    // Maybe modify this in the future, reference: https://github.com/ClickHouse/ClickHouse/pull/22003
    template <PrimitiveType T>
    typename PrimitiveTypeTraits<T>::CppType& get();

    template <PrimitiveType T>
    const typename PrimitiveTypeTraits<T>::CppType& get() const;

    bool operator==(const Field& rhs) const {
        return operator<=>(rhs) == std::strong_ordering::equal;
    }

    std::strong_ordering operator<=>(const Field& rhs) const;

    std::string_view as_string_view() const;

private:
    std::aligned_union_t<DBMS_MIN_FIELD_SIZE - sizeof(PrimitiveType), Null, UInt64, UInt128, Int64,
                         Int128, IPv6, Float64, String, JsonbField, StringView, Array, Tuple, Map,
                         VariantMap, Decimal32, Decimal64, DecimalV2Value, Decimal128V3, Decimal256,
                         BitmapValue, HyperLogLog, QuantileState>
            storage;

    PrimitiveType type;

    /// Assuming there was no allocated state or it was deallocated (see destroy).
    template <PrimitiveType Type>
    void create_concrete(typename PrimitiveTypeTraits<Type>::CppType&& x);
    template <PrimitiveType Type>
    void create_concrete(const typename PrimitiveTypeTraits<Type>::CppType& x);
    /// Assuming same types.
    template <PrimitiveType Type>
    void assign_concrete(typename PrimitiveTypeTraits<Type>::CppType&& x);
    template <PrimitiveType Type>
    void assign_concrete(const typename PrimitiveTypeTraits<Type>::CppType& x);

    void create(const Field& field);
    void create(Field&& field);

    void assign(const Field& x);
    void assign(Field&& x);

    void destroy();

    template <PrimitiveType T>
    void destroy();
};

struct FieldWithDataType {
    Field field;
    // used for nested type of array
    PrimitiveType base_scalar_type_id = PrimitiveType::INVALID_TYPE;
    uint8_t num_dimensions = 0;
    int precision = -1;
    int scale = -1;
};

} // namespace doris::vectorized

template <>
struct std::hash<doris::vectorized::Field> {
    size_t operator()(const doris::vectorized::Field& field) const {
        if (field.is_null()) {
            return 0;
        }
        std::hash<std::string_view> hasher;
        return hasher(field.as_string_view());
    }
};
