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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypesDecimal.h
// and modified by Doris

#pragma once
#include <fmt/format.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <cmath>
#include <memory>
#include <string>
#include <type_traits>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "runtime/primitive_type.h"
#include "runtime/type_limit.h"
#include "runtime/types.h"
#include "serde/data_type_decimal_serde.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column_decimal.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
class DecimalV2Value;
class PColumnMeta;

namespace vectorized {
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <PrimitiveType T>
constexpr size_t default_decimal_scale() {
    return 0;
}
template <>
constexpr size_t default_decimal_scale<TYPE_DECIMALV2>() {
    return BeConsts::MAX_DECIMALV2_SCALE;
}

constexpr size_t min_decimal_precision() {
    return 1;
}
template <PrimitiveType T>
constexpr size_t max_decimal_precision() {
    return 0;
}
template <>
constexpr size_t max_decimal_precision<TYPE_DECIMAL32>() {
    return BeConsts::MAX_DECIMAL32_PRECISION;
}
template <>
constexpr size_t max_decimal_precision<TYPE_DECIMAL64>() {
    return BeConsts::MAX_DECIMAL64_PRECISION;
}
template <>
constexpr size_t max_decimal_precision<TYPE_DECIMALV2>() {
    return BeConsts::MAX_DECIMALV2_PRECISION;
}
template <>
constexpr size_t max_decimal_precision<TYPE_DECIMAL128I>() {
    return BeConsts::MAX_DECIMAL128_PRECISION;
}
template <>
constexpr size_t max_decimal_precision<TYPE_DECIMAL256>() {
    return BeConsts::MAX_DECIMAL256_PRECISION;
}

// Change precision to default value for DecimalV2. This transformation needs to be removed later.
DataTypePtr get_data_type_with_default_argument(DataTypePtr type);

DataTypePtr create_decimal(UInt64 precision, UInt64 scale, bool use_v2);

/// Implements Decimal(P, S), where P is precision, S is scale.
/// Maximum precisions for underlying types are:
/// Int32    9
/// Int64   18
/// Int128  38
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38); equals to the maximum precision for the biggest underlying type of operands.
///     S is maximum scale of operands. The allowed values are [0, precision]
template <PrimitiveType T>
class DataTypeDecimal final : public IDataType {
    static_assert(is_decimal(T));

public:
    using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;
    using FieldType = typename PrimitiveTypeTraits<T>::ColumnItemType;
    static constexpr PrimitiveType PType = T;

    static constexpr bool is_parametric = true;

    static constexpr size_t max_precision() { return max_decimal_precision<T>(); }

    DataTypeDecimal(UInt32 precision = max_decimal_precision<T>(),
                    UInt32 scale = default_decimal_scale<T>(),
                    UInt32 arg_original_precision = UINT32_MAX,
                    UInt32 arg_original_scale = UINT32_MAX)
            : precision(precision),
              scale(scale),
              original_precision(arg_original_precision),
              original_scale(arg_original_scale) {
        check_type_precision(precision);
        check_type_scale(scale);
        if (scale > precision) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "scale({}) is greater than precision({})",
                            scale, precision);
        }
        if constexpr (T == TYPE_DECIMALV2) {
            if (UINT32_MAX != original_precision) {
                check_type_precision(original_precision);
            }
            if (UINT32_MAX != original_scale) {
                check_type_scale(scale);
            }
            if (original_scale > original_precision) {
                throw Exception(ErrorCode::INVALID_ARGUMENT,
                                "original_scale({}) is greater than original_precision({})",
                                original_scale, original_precision);
            }
        }
    }

    DataTypeDecimal(const DataTypeDecimal& rhs)
            : precision(rhs.precision),
              scale(rhs.scale),
              original_precision(rhs.original_precision),
              original_scale(rhs.original_scale) {}

    const std::string get_family_name() const override { return type_to_string(T); }
    std::string do_get_name() const override;
    PrimitiveType get_primitive_type() const override { return T; }

    doris::FieldType get_storage_field_type() const override {
        if constexpr (T == TYPE_DECIMAL32) {
            return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL32;
        }
        if constexpr (T == TYPE_DECIMAL64) {
            return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL64;
        }
        if constexpr (T == TYPE_DECIMAL128I) {
            return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL128I;
        }
        if constexpr (T == TYPE_DECIMAL256) {
            return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL256;
        }
        return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL;
    }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    Field get_default() const override;

    Field get_field(const TExprNode& node) const override {
        DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
        DCHECK(node.__isset.decimal_literal);
        // decimalv2
        if constexpr (T == TYPE_DECIMALV2) {
            DecimalV2Value value;
            if (value.parse_from_str(node.decimal_literal.value.c_str(),
                                     cast_set<int>(node.decimal_literal.value.size())) ==
                E_DEC_OK) {
                return Field::create_field<TYPE_DECIMALV2>(
                        DecimalField<FieldType>(value.value(), value.scale()));
            } else {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Invalid decimal(scale: {}) value: {}", value.scale(),
                                       node.decimal_literal.value);
            }
        }
        // decimal
        FieldType val;
        if (!parse_from_string(node.decimal_literal.value, &val)) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid value: {} for type {}", node.decimal_literal.value,
                                   do_get_name());
        };
        return Field::create_field<T>(DecimalField<FieldType>(val, scale));
    }

    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;
    bool equals(const IDataType& rhs) const override;

    bool have_subtypes() const override { return false; }
    bool should_align_right_in_pretty_formats() const override { return true; }
    bool text_can_contain_only_valid_utf8() const override { return true; }
    bool is_comparable() const override { return true; }
    bool is_value_represented_by_number() const override { return true; }
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return true;
    }
    bool have_maximum_size_of_value() const override { return true; }
    size_t get_size_of_value_in_memory() const override { return sizeof(FieldType); }

    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    void to_string_batch(const IColumn& column, ColumnString& column_to) const override;
    template <bool is_const>
    void to_string_batch_impl(const ColumnPtr& column_ptr, ColumnString& column_to) const;
    std::string to_string(const FieldType& value) const;
    using SerDeType = DataTypeDecimalSerDe<T>;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(precision, scale, nesting_level);
    };

    /// Decimal specific

    [[nodiscard]] UInt32 get_precision() const override { return precision; }
    [[nodiscard]] UInt32 get_scale() const override { return scale; }
    [[nodiscard]] UInt32 get_original_precision() const {
        return UINT32_MAX == original_precision ? precision : original_precision;
    }
    [[nodiscard]] UInt32 get_original_scale() const { return get_format_scale(); }
    [[nodiscard]] UInt32 get_format_scale() const {
        return UINT32_MAX == original_scale ? scale : original_scale;
    }
    FieldType::NativeType get_scale_multiplier() const { return get_scale_multiplier(scale); }
    void to_protobuf(PTypeDesc* ptype, PTypeNode* node, PScalarType* scalar_type) const override {
        scalar_type->set_precision(precision);
        scalar_type->set_scale(scale);
    }

    /// @returns multiplier for U to become T with correct scale
    template <PrimitiveType U>
    FieldType::NativeType scale_factor_for(const DataTypeDecimal<U>& x) const {
        if (get_scale() < x.get_scale()) {
            throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                                   "Decimal result's scale is less then argument's one");
        }

        UInt32 scale_delta = get_scale() - x.get_scale(); /// scale_delta >= 0
        return get_scale_multiplier(scale_delta);
    }

    static constexpr FieldType::NativeType get_scale_multiplier(UInt32 scale);

    static constexpr FieldType::NativeType get_max_digits_number(UInt32 digit_count);

    bool parse_from_string(const std::string& str, FieldType* res) const;

    FieldWithDataType get_field_with_data_type(const IColumn& column,
                                               size_t row_num) const override;

    static void check_type_precision(const UInt32 precision) {
        if (precision > max_decimal_precision<T>() || precision < 1) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "meet invalid precision: real_precision={}, max_decimal_precision={}, "
                            "min_decimal_precision=1",
                            precision, max_decimal_precision<T>());
        }
    }

    static void check_type_scale(const UInt32 scale) {
        if (scale > max_decimal_precision<T>()) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "meet invalid scale: real_scale={}, max_decimal_precision={}", scale,
                            max_decimal_precision<T>());
        }
    }

private:
    const UInt32 precision;
    const UInt32 scale;

    // For decimalv2 only, record the original(schema) precision and scale.
    // UINT32_MAX means original precision and scale are unknown.
    // Decimalv2 will be converted to Decimal(27, 9) in memory when doing any calculations,
    // but when casting decimalv2 to string, it's better to keep the presion and
    // scale of it's original value in schema.
    UInt32 original_precision = UINT32_MAX;
    UInt32 original_scale = UINT32_MAX;
};

using DataTypeDecimal32 = DataTypeDecimal<TYPE_DECIMAL32>;
using DataTypeDecimal64 = DataTypeDecimal<TYPE_DECIMAL64>;
using DataTypeDecimalV2 = DataTypeDecimal<TYPE_DECIMALV2>;
using DataTypeDecimal128 = DataTypeDecimal<TYPE_DECIMAL128I>;
using DataTypeDecimal256 = DataTypeDecimal<TYPE_DECIMAL256>;

template <>
constexpr Decimal32::NativeType DataTypeDecimal<TYPE_DECIMAL32>::get_scale_multiplier(
        UInt32 scale) {
    return common::exp10_i32(scale);
}

template <>
constexpr Decimal64::NativeType DataTypeDecimal<TYPE_DECIMAL64>::get_scale_multiplier(
        UInt32 scale) {
    return common::exp10_i64(scale);
}

template <>
constexpr Decimal128V2::NativeType DataTypeDecimal<TYPE_DECIMALV2>::get_scale_multiplier(
        UInt32 scale) {
    return common::exp10_i128(scale);
}

template <>
constexpr Decimal128V3::NativeType DataTypeDecimal<TYPE_DECIMAL128I>::get_scale_multiplier(
        UInt32 scale) {
    return common::exp10_i128(scale);
}

template <>
constexpr Decimal256::NativeType DataTypeDecimal<TYPE_DECIMAL256>::get_scale_multiplier(
        UInt32 scale) {
    return common::exp10_i256(scale);
}

template <>
constexpr Decimal32::NativeType DataTypeDecimal<TYPE_DECIMAL32>::get_max_digits_number(
        UInt32 digit_count) {
    return common::max_i32(digit_count);
}
template <>
constexpr Decimal64::NativeType DataTypeDecimal<TYPE_DECIMAL64>::get_max_digits_number(
        UInt32 digit_count) {
    return common::max_i64(digit_count);
}
template <>
constexpr Decimal128V2::NativeType DataTypeDecimal<TYPE_DECIMALV2>::get_max_digits_number(
        UInt32 digit_count) {
    return common::max_i128(digit_count);
}
template <>
constexpr Decimal128V3::NativeType DataTypeDecimal<TYPE_DECIMAL128I>::get_max_digits_number(
        UInt32 digit_count) {
    return common::max_i128(digit_count);
}
template <>
constexpr Decimal256::NativeType DataTypeDecimal<TYPE_DECIMAL256>::get_max_digits_number(
        UInt32 digit_count) {
    return common::max_i256(digit_count);
}

template <PrimitiveType T, PrimitiveType U>
DataTypePtr decimal_result_type(const DataTypeDecimal<T>& tx, const DataTypeDecimal<U>& ty,
                                bool is_multiply, bool is_divide, bool is_plus_minus) {
    constexpr PrimitiveType Type =
            sizeof(typename PrimitiveTypeTraits<T>::ColumnItemType) >=
                            sizeof(typename PrimitiveTypeTraits<U>::ColumnItemType)
                    ? T
                    : U;
    if constexpr (T == TYPE_DECIMALV2 && U == TYPE_DECIMALV2) {
        return std::make_shared<DataTypeDecimal<Type>>(max_decimal_precision<T>(), 9);
    } else {
        UInt32 scale = std::max(tx.get_scale(), ty.get_scale());
        auto precision = max_decimal_precision<Type>();

        size_t multiply_precision = tx.get_precision() + ty.get_precision();
        size_t divide_precision = tx.get_precision() + ty.get_scale();
        size_t plus_minus_precision =
                std::max(tx.get_precision() - tx.get_scale(), ty.get_precision() - ty.get_scale()) +
                scale + 1;
        if (is_multiply) {
            scale = tx.get_scale() + ty.get_scale();
            precision = std::min(multiply_precision, max_decimal_precision<TYPE_DECIMAL256>());
        } else if (is_divide) {
            scale = tx.get_scale();
            precision = std::min(divide_precision, max_decimal_precision<TYPE_DECIMAL256>());
        } else if (is_plus_minus) {
            precision = std::min(plus_minus_precision, max_decimal_precision<TYPE_DECIMAL256>());
        }
        return create_decimal(precision, scale, false);
    }
}

template <PrimitiveType T>
const DataTypeDecimal<T>* check_decimal(const IDataType& data_type) {
    return typeid_cast<const DataTypeDecimal<T>*>(&data_type);
}

inline UInt32 get_decimal_scale(const IDataType& data_type, UInt32 default_value = 0) {
    if (const auto* decimal_type = check_decimal<TYPE_DECIMAL32>(data_type)) {
        return decimal_type->get_scale();
    }
    if (const auto* decimal_type = check_decimal<TYPE_DECIMAL64>(data_type)) {
        return decimal_type->get_scale();
    }
    if (const auto* decimal_type = check_decimal<TYPE_DECIMALV2>(data_type)) {
        return decimal_type->get_scale();
    }
    if (const auto* decimal_type = check_decimal<TYPE_DECIMAL128I>(data_type)) {
        return decimal_type->get_scale();
    }
    if (const auto* decimal_type = check_decimal<TYPE_DECIMAL256>(data_type)) {
        return decimal_type->get_scale();
    }
    return default_value;
}

///

template <typename DataType>
constexpr bool IsDataTypeDecimal = false;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal32> = true;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal64> = true;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimalV2> = true;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal128> = true;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal256> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimalV3 = false;
template <>
inline constexpr bool IsDataTypeDecimalV3<DataTypeDecimal32> = true;
template <>
inline constexpr bool IsDataTypeDecimalV3<DataTypeDecimal64> = true;
template <>
inline constexpr bool IsDataTypeDecimalV3<DataTypeDecimal128> = true;
template <>
inline constexpr bool IsDataTypeDecimalV3<DataTypeDecimal256> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimalV2 = false;
template <>
inline constexpr bool IsDataTypeDecimalV2<DataTypeDecimalV2> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimal128V3 = false;
template <>
inline constexpr bool IsDataTypeDecimal128V3<DataTypeDecimal128> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimal256 = false;
template <>
inline constexpr bool IsDataTypeDecimal256<DataTypeDecimal256> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimalOrNumber =
        IsDataTypeDecimal<DataType> || IsDataTypeNumber<DataType>;

template <PrimitiveType T>
    requires(is_decimal(T))
typename PrimitiveTypeTraits<T>::CppNativeType max_decimal_value(UInt32 precision) {
    return type_limit<typename PrimitiveTypeTraits<T>::ColumnItemType>::max().value /
           DataTypeDecimal<T>::get_scale_multiplier(
                   (UInt32)(max_decimal_precision<T>() - precision));
}

template <PrimitiveType T>
    requires(is_decimal(T))
typename PrimitiveTypeTraits<T>::CppNativeType min_decimal_value(UInt32 precision) {
    return type_limit<typename PrimitiveTypeTraits<T>::ColumnItemType>::min().value /
           DataTypeDecimal<T>::get_scale_multiplier(
                   (UInt32)(max_decimal_precision<T>() - precision));
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
