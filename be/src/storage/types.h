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

#include <fmt/format.h>
#include <glog/logging.h>

#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/decimal12.h"
#include "core/extended_types.h"
#include "core/packed_int128.h"
#include "core/type_limit.h"
#include "core/uint24.h"
#include "core/value/ipv4_value.h"
#include "core/value/ipv6_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/cast/cast_to_timestamptz.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "util/slice.h"

namespace doris {

bool is_scalar_type(FieldType field_type);

// support following formats when convert varchar to date
static const std::vector<std::string> DATE_FORMATS {
        "%Y-%m-%d", "%y-%m-%d", "%Y%m%d", "%y%m%d", "%Y/%m/%d", "%y/%m/%d",
};

// Maps a storage FieldType to its in-memory cell representation.
//
// ARRAY / MAP / STRUCT are intentionally NOT specialized here: they are
// containers of other types, so only their element types have a storage-layer
// cell representation. The primary template below uses a deferred
// static_assert so that instantiating `CppTypeTraits<X>` for any
// unspecialized FieldType — most importantly ARRAY/MAP/STRUCT — fails at
// build time with a clear message, rather than at runtime.
namespace detail {
template <FieldType>
inline constexpr bool cpp_type_traits_unspecialized = false;
} // namespace detail

template <FieldType field_type>
struct CppTypeTraits {
    static_assert(detail::cpp_type_traits_unspecialized<field_type>,
                  "CppTypeTraits not specialized for this FieldType. "
                  "ARRAY / MAP / STRUCT and similar container types have no "
                  "storage-layer cell representation — operate on the element "
                  "type instead.");
};

template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_BOOL> {
    using CppType = uint8_t;
    using UnsignedCppType = uint8_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_TINYINT> {
    using CppType = int8_t;
    using UnsignedCppType = uint8_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_SMALLINT> {
    using CppType = int16_t;
    using UnsignedCppType = uint16_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_INT> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT> {
    using CppType = uint32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_BIGINT> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_LARGEINT> {
    using CppType = int128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_FLOAT> {
    using CppType = float;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DOUBLE> {
    using CppType = double;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL> {
    using CppType = decimal12_t;
    using UnsignedCppType = decimal12_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL32> {
    using CppType = int32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL64> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL128I> {
    using CppType = int128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL256> {
    using CppType = wide::Int256;
    using UnsignedCppType = wide::UInt256;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE> {
    using CppType = uint24_t;
    using UnsignedCppType = uint24_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2> {
    using CppType = uint32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMEV2> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ> {
    using CppType = uint64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME> {
    using CppType = int64_t;
    using UnsignedCppType = uint64_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV4> {
    using CppType = uint32_t;
    using UnsignedCppType = uint32_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV6> {
    using CppType = uint128_t;
    using UnsignedCppType = uint128_t;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_STRING> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_JSONB> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_VARIANT> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_HLL> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_BITMAP> {
    using CppType = Slice;
};
template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE> {
    using CppType = Slice;
};

template <>
struct CppTypeTraits<FieldType::OLAP_FIELD_TYPE_AGG_STATE> {
    using CppType = Slice;
};

template <FieldType field_type>
struct BaseFieldTypeTraits : public CppTypeTraits<field_type> {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static inline CppType get_cpp_type_value(const void* address) {
        if constexpr (field_type == FieldType::OLAP_FIELD_TYPE_LARGEINT) {
            return get_int128_from_unalign(address);
        }
        return *reinterpret_cast<const CppType*>(address);
    }

    static inline void set_cpp_type_value(void* address, const CppType& value) {
        memcpy(address, &value, sizeof(CppType));
    }

    static inline int cmp(const void* left, const void* right) {
        CppType left_value = get_cpp_type_value(left);
        CppType right_value = get_cpp_type_value(right);
        if (left_value < right_value) {
            return -1;
        } else if (left_value > right_value) {
            return 1;
        } else {
            return 0;
        }
    }
};

// Using NumericFieldtypeTraits to Derived code for FieldType::OLAP_FIELD_TYPE_XXXINT, FieldType::OLAP_FIELD_TYPE_FLOAT,
// FieldType::OLAP_FIELD_TYPE_DOUBLE, to reduce redundant code
template <FieldType fieldType, bool isArithmetic>
struct NumericFieldTypeTraits : public BaseFieldTypeTraits<fieldType> {
    using CppType = typename CppTypeTraits<fieldType>::CppType;
};

template <FieldType fieldType>
struct NumericFieldTypeTraits<fieldType, false> : public BaseFieldTypeTraits<fieldType> {};

template <FieldType fieldType>
struct FieldTypeTraits
        : public NumericFieldTypeTraits<
                  fieldType, IsArithmeticV<typename BaseFieldTypeTraits<fieldType>::CppType> &&
                                     IsSignedV<typename BaseFieldTypeTraits<fieldType>::CppType>> {
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_BOOL>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_BOOL> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_LARGEINT>
        : public NumericFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_LARGEINT, true> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV4>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV4> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV6>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_IPV6> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DECIMAL> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATE> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATEV2> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIMEV2> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_DATETIME> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR>
        : public BaseFieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {
    static int cmp(const void* left, const void* right) {
        auto l_slice = reinterpret_cast<const Slice*>(left);
        auto r_slice = reinterpret_cast<const Slice*>(right);
        return l_slice->compare(*r_slice);
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_STRING>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_CHAR> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_JSONB>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {
    static int cmp(const void* left, const void* right) {
        LOG(WARNING) << "can not compare JSONB values";
        return -1; // always update ?
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARIANT>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_JSONB> {
    static int cmp(const void* left, const void* right) {
        LOG(WARNING) << "can not compare VARIANT values";
        return -1; // always update ?
    }
};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_HLL>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_BITMAP>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {};

template <>
struct FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_AGG_STATE>
        : public FieldTypeTraits<FieldType::OLAP_FIELD_TYPE_VARCHAR> {};

// Instantiate this template to get static access to the type traits.
template <FieldType field_type>
struct TypeTraits : public FieldTypeTraits<field_type> {
    using CppType = typename CppTypeTraits<field_type>::CppType;

    static const FieldType type = field_type;
    static const int32_t size = sizeof(CppType);
};

// In-memory storage cell footprint for one value of `field_type`,
// i.e. sizeof(CppTypeTraits<field_type>::CppType).
//
// This is NOT the schema-declared length:
//   - CHAR(N) / VARCHAR(N) / STRING / JSONB / VARIANT / HLL / BITMAP /
//     QUANTILE_STATE / AGG_STATE all return sizeof(Slice) == 16 (the ptr+len
//     descriptor in a row buffer); for the user-declared N see
//     TabletColumn::get_field_length_by_type.
//
// ARRAY / MAP / STRUCT are containers of other types — only their element
// types have a storage-layer cell size. The container itself has no such size
// at this layer, so it is not handled here. Passing one in is a programming
// error and trips the default LOG(FATAL) below.
//
// VARIANT root data is still routed through ColumnReader/EncodingInfo at read
// time, so VARIANT keeps its full traits chain even though the column-writer
// step path doesn't reach it.
//
// Used for cell-level pointer arithmetic on row buffers, BKD bytes_per_dim
// fallback when the index file has no header, and per-row footprint estimation
// during compaction.
inline size_t field_type_size(FieldType field_type) {
    switch (field_type) {
#define DORIS_FIELD_TYPE_SIZE_CASE(ft) \
    case FieldType::ft:                \
        return sizeof(typename CppTypeTraits<FieldType::ft>::CppType);
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_BOOL)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_TINYINT)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_SMALLINT)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_INT)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_UNSIGNED_INT)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_BIGINT)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_UNSIGNED_BIGINT)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_LARGEINT)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_FLOAT)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DOUBLE)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DECIMAL)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DECIMAL32)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DECIMAL64)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DECIMAL128I)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DECIMAL256)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DATE)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DATETIME)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DATEV2)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_DATETIMEV2)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_TIMEV2)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_TIMESTAMPTZ)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_IPV4)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_IPV6)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_CHAR)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_VARCHAR)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_STRING)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_JSONB)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_VARIANT)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_HLL)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_BITMAP)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_QUANTILE_STATE)
        DORIS_FIELD_TYPE_SIZE_CASE(OLAP_FIELD_TYPE_AGG_STATE)
#undef DORIS_FIELD_TYPE_SIZE_CASE
    default:
        LOG(FATAL) << "field_type_size: unsupported FieldType " << int(field_type);
        return 0;
    }
}

} // namespace doris
