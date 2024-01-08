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
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>
#include <memory>
#include <ostream>
#include <string>
#include <type_traits>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
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
#include "vec/data_types/number_traits.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/utils/template_helpers.hpp"

namespace doris {
class DecimalV2Value;
class PColumnMeta;

namespace vectorized {
class BufferWritable;
class IColumn;
class ReadBuffer;
template <typename T>
struct TypeId;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

constexpr size_t min_decimal_precision() {
    return 1;
}
template <typename T>
constexpr size_t max_decimal_precision() {
    return 0;
}
template <>
constexpr size_t max_decimal_precision<Decimal32>() {
    return BeConsts::MAX_DECIMAL32_PRECISION;
}
template <>
constexpr size_t max_decimal_precision<Decimal64>() {
    return BeConsts::MAX_DECIMAL64_PRECISION;
}
template <>
constexpr size_t max_decimal_precision<Decimal128V2>() {
    return BeConsts::MAX_DECIMALV2_PRECISION;
}
template <>
constexpr size_t max_decimal_precision<Decimal128V3>() {
    return BeConsts::MAX_DECIMAL128_PRECISION;
}
template <>
constexpr size_t max_decimal_precision<Decimal256>() {
    return BeConsts::MAX_DECIMAL256_PRECISION;
}

DataTypePtr create_decimal(UInt64 precision, UInt64 scale, bool use_v2);

inline UInt32 least_decimal_precision_for(TypeIndex int_type) {
    switch (int_type) {
    case TypeIndex::Int8:
        [[fallthrough]];
    case TypeIndex::UInt8:
        return 3;
    case TypeIndex::Int16:
        [[fallthrough]];
    case TypeIndex::UInt16:
        return 5;
    case TypeIndex::Int32:
        [[fallthrough]];
    case TypeIndex::UInt32:
        return 10;
    case TypeIndex::Int64:
        return 19;
    case TypeIndex::UInt64:
        return 20;
    default:
        break;
    }
    return 0;
}

/// Implements Decimal(P, S), where P is precision, S is scale.
/// Maximum precisions for underlying types are:
/// Int32    9
/// Int64   18
/// Int128  38
/// Operation between two decimals leads to Decimal(P, S), where
///     P is one of (9, 18, 38); equals to the maximum precision for the biggest underlying type of operands.
///     S is maximum scale of operands. The allowed values are [0, precision]
template <typename T>
class DataTypeDecimal final : public IDataType {
    static_assert(IsDecimalNumber<T>);

public:
    using ColumnType = ColumnDecimal<T>;
    using FieldType = T;

    static constexpr bool is_parametric = true;

    static constexpr size_t max_precision() { return max_decimal_precision<T>(); }

    DataTypeDecimal(UInt32 precision = 27, UInt32 scale = 9) : precision(precision), scale(scale) {
        check_type_precision(precision);
        check_type_scale(scale);
    }

    DataTypeDecimal(const DataTypeDecimal& rhs) : precision(rhs.precision), scale(rhs.scale) {}

    const char* get_family_name() const override { return "Decimal"; }
    std::string do_get_name() const override;
    TypeIndex get_type_id() const override { return TypeId<T>::value; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        TypeDescriptor desc;
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal32>>) {
            desc = TypeDescriptor(TYPE_DECIMAL32);
        } else if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal64>>) {
            desc = TypeDescriptor(TYPE_DECIMAL64);
        } else if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal128V3>>) {
            desc = TypeDescriptor(TYPE_DECIMAL128I);
        } else if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal256>>) {
            desc = TypeDescriptor(TYPE_DECIMAL256);
        } else {
            desc = TypeDescriptor(TYPE_DECIMALV2);
        }
        desc.scale = scale;
        desc.precision = precision;
        return desc;
    }

    doris::FieldType get_storage_field_type() const override {
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal32>>) {
            return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL32;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal64>>) {
            return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL64;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal128V3>>) {
            return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL128I;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal256>>) {
            return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL256;
        }
        return doris::FieldType::OLAP_FIELD_TYPE_DECIMAL;
    }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;

    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    Field get_default() const override;

    Field get_field(const TExprNode& node) const override {
        DCHECK_EQ(node.node_type, TExprNodeType::DECIMAL_LITERAL);
        DCHECK(node.__isset.decimal_literal);
        // decimalv2
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal128V2>>) {
            DecimalV2Value value;
            if (value.parse_from_str(node.decimal_literal.value.c_str(),
                                     node.decimal_literal.value.size()) == E_DEC_OK) {
                return DecimalField<Decimal128V2>(value.value(), value.scale());
            } else {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Invalid decimal(scale: {}) value: {}", value.scale(),
                                       node.decimal_literal.value);
            }
        }
        // decimal
        T val;
        if (!parse_from_string(node.decimal_literal.value, &val)) {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid value: {} for type {}", node.decimal_literal.value,
                                   do_get_name());
        };
        return DecimalField<T>(val, scale);
    }

    MutableColumnPtr create_column() const override;
    bool equals(const IDataType& rhs) const override;

    bool get_is_parametric() const override { return true; }
    bool have_subtypes() const override { return false; }
    bool should_align_right_in_pretty_formats() const override { return true; }
    bool text_can_contain_only_valid_utf8() const override { return true; }
    bool is_comparable() const override { return true; }
    bool is_value_represented_by_number() const override { return true; }
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return true;
    }
    bool have_maximum_size_of_value() const override { return true; }
    size_t get_size_of_value_in_memory() const override { return sizeof(T); }

    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeDecimalSerDe<T>>(scale, precision, nesting_level);
    };

    /// Decimal specific

    [[nodiscard]] UInt32 get_precision() const override { return precision; }
    [[nodiscard]] UInt32 get_scale() const override { return scale; }
    T get_scale_multiplier() const { return get_scale_multiplier(scale); }

    T whole_part(T x) const {
        if (scale == 0) {
            return x;
        }
        return x / get_scale_multiplier();
    }

    T fractional_part(T x) const {
        if (scale == 0) {
            return T();
        }
        if (x < T()) {
            x *= -1;
        }
        return x % get_scale_multiplier();
    }

    T max_whole_value() const { return get_scale_multiplier(max_precision() - scale) - T(1); }

    bool can_store_whole(T x) const {
        T max = max_whole_value();
        if (x > max || x < T(-max)) {
            return false;
        }
        return true;
    }

    /// @returns multiplier for U to become T with correct scale
    template <typename U>
    T scale_factor_for(const DataTypeDecimal<U>& x, bool) const {
        if (get_scale() < x.get_scale()) {
            LOG(FATAL) << "Decimal result's scale is less then argiment's one";
        }

        UInt32 scale_delta = get_scale() - x.get_scale(); /// scale_delta >= 0
        return get_scale_multiplier(scale_delta);
    }

    template <typename U>
    T scale_factor_for(const DataTypeNumber<U>&, bool is_multiply_or_divisor) const {
        if (is_multiply_or_divisor) {
            return 1;
        }
        return get_scale_multiplier();
    }

    static T get_scale_multiplier(UInt32 scale);

    static T get_max_digits_number(UInt32 digit_count);

    bool parse_from_string(const std::string& str, T* res) const;

    static void check_type_precision(const vectorized::UInt32 precision) {
        if (precision > max_decimal_precision<T>() || precision < 1) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "meet invalid precision: real_precision={}, max_decimal_precision={}, "
                            "min_decimal_precision=1",
                            precision, max_decimal_precision<T>());
        }
    }

    static void check_type_scale(const vectorized::UInt32 scale) {
        if (scale > max_decimal_precision<T>()) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "meet invalid scale: real_scale={}, max_decimal_precision={}", scale,
                            max_decimal_precision<T>());
        }
    }

private:
    const UInt32 precision;
    const UInt32 scale;
};

template <typename T, typename U>
DataTypePtr decimal_result_type(const DataTypeDecimal<T>& tx, const DataTypeDecimal<U>& ty,
                                bool is_multiply, bool is_divide, bool is_plus_minus) {
    using Type = std::conditional_t<sizeof(T) >= sizeof(U), T, U>;
    if constexpr (IsDecimalV2<T> && IsDecimalV2<U>) {
        return std::make_shared<DataTypeDecimal<Type>>((max_decimal_precision<T>(), 9));
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
            precision = std::min(multiply_precision, max_decimal_precision<Decimal256>());
        } else if (is_divide) {
            scale = tx.get_scale();
            precision = std::min(divide_precision, max_decimal_precision<Decimal256>());
        } else if (is_plus_minus) {
            precision = std::min(plus_minus_precision, max_decimal_precision<Decimal256>());
        }
        return create_decimal(precision, scale, false);
    }
}

template <typename T>
const DataTypeDecimal<T>* check_decimal(const IDataType& data_type) {
    return typeid_cast<const DataTypeDecimal<T>*>(&data_type);
}

inline UInt32 get_decimal_scale(const IDataType& data_type, UInt32 default_value = 0) {
    if (auto* decimal_type = check_decimal<Decimal32>(data_type)) {
        return decimal_type->get_scale();
    }
    if (auto* decimal_type = check_decimal<Decimal64>(data_type)) {
        return decimal_type->get_scale();
    }
    if (auto* decimal_type = check_decimal<Decimal128V2>(data_type)) {
        return decimal_type->get_scale();
    }
    if (auto* decimal_type = check_decimal<Decimal128V3>(data_type)) {
        return decimal_type->get_scale();
    }
    if (auto* decimal_type = check_decimal<Decimal256>(data_type)) {
        return decimal_type->get_scale();
    }
    return default_value;
}

///

template <typename DataType>
constexpr bool IsDataTypeDecimal = false;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal32>> = true;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal64>> = true;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal128V2>> = true;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal128V3>> = true;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal256>> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimalV2 = false;
template <>
inline constexpr bool IsDataTypeDecimalV2<DataTypeDecimal<Decimal128V2>> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimal128V3 = false;
template <>
inline constexpr bool IsDataTypeDecimal128V3<DataTypeDecimal<Decimal128V3>> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimal256 = false;
template <>
inline constexpr bool IsDataTypeDecimal256<DataTypeDecimal<Decimal256>> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimalOrNumber =
        IsDataTypeDecimal<DataType> || IsDataTypeNumber<DataType>;

// only for casting between other integral types and decimals
template <typename FromDataType, typename ToDataType, bool multiply_may_overflow,
          bool narrow_integral, typename RealFrom, typename RealTo>
    requires IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>
void convert_to_decimals(RealTo* dst, const RealFrom* src, UInt32 scale_from, UInt32 scale_to,
                         const typename ToDataType::FieldType& min_result,
                         const typename ToDataType::FieldType& max_result, size_t size) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                            FromFieldType, ToFieldType>;

    DCHECK_GE(scale_to, scale_from);
    // from integer to decimal
    MaxFieldType multiplier =
            DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_to - scale_from);
    MaxFieldType tmp;
    for (size_t i = 0; i < size; i++) {
        if constexpr (multiply_may_overflow) {
            if (common::mul_overflow(static_cast<MaxFieldType>(src[i]).value, multiplier.value,
                                     tmp.value)) {
                throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR, "Arithmetic overflow");
            }
            if constexpr (narrow_integral) {
                if (tmp.value < min_result.value || tmp.value > max_result.value) {
                    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                    "Arithmetic overflow, convert failed from {}, "
                                    "expected data is [{}, {}]",
                                    tmp.value, min_result.value, max_result.value);
                }
            }
            dst[i].value = tmp.value;
        } else {
            dst[i].value = multiplier.value * static_cast<MaxFieldType>(src[i]).value;
        }
    }

    if constexpr (!multiply_may_overflow && narrow_integral) {
        for (size_t i = 0; i < size; i++) {
            if (dst[i].value < min_result.value || dst[i].value > max_result.value) {
                throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                "Arithmetic overflow, convert failed from {}, "
                                "expected data is [{}, {}]",
                                dst[i].value, min_result.value, max_result.value);
            }
        }
    }
}

// only for casting between other integral types and decimals
template <typename FromDataType, typename ToDataType, bool narrow_integral, typename RealFrom,
          typename RealTo>
    requires IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>
void convert_from_decimals(RealTo* dst, const RealFrom* src, UInt32 scale_from,
                           const typename ToDataType::FieldType& min_result,
                           const typename ToDataType::FieldType& max_result, size_t size) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                            FromFieldType, ToFieldType>;

    // from decimal to integer
    MaxFieldType multiplier = DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_from);
    for (size_t i = 0; i < size; i++) {
        auto tmp = static_cast<MaxFieldType>(src[i]).value / multiplier.value;
        if constexpr (narrow_integral) {
            if (tmp < min_result.value || tmp > max_result.value) {
                throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                "Arithmetic overflow, convert failed from {}, "
                                "expected data is [{}, {}]",
                                tmp, min_result.value, max_result.value);
            }
        }
        dst[i] = tmp;
    }
}

template <typename FromDataType, typename ToDataType, bool multiply_may_overflow,
          bool narrow_integral>
void convert_decimal_cols(
        const typename ColumnDecimal<
                typename FromDataType::FieldType>::Container::value_type* __restrict vec_from,
        typename ColumnDecimal<typename ToDataType::FieldType>::Container::value_type* vec_to,
        const UInt32 precision_from, const UInt32 scale_from, const UInt32 precision_to,
        const UInt32 scale_to, const size_t sz) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType =
            std::conditional_t<(sizeof(FromFieldType) == sizeof(ToFieldType)) &&
                                       (std::is_same_v<ToFieldType, Decimal128V3> ||
                                        std::is_same_v<FromFieldType, Decimal128V3>),
                               Decimal128V3,
                               std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                                  FromFieldType, ToFieldType>>;
    using MaxNativeType = typename MaxFieldType::NativeType;

    auto max_result = DataTypeDecimal<ToFieldType>::get_max_digits_number(precision_to);
    if (scale_to > scale_from) {
        const MaxNativeType multiplier =
                DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_to - scale_from);
        MaxNativeType res;
        for (size_t i = 0; i < sz; i++) {
            if constexpr (multiply_may_overflow) {
                if (common::mul_overflow(static_cast<MaxNativeType>(vec_from[i].value), multiplier,
                                         res)) {
                    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR, "Arithmetic overflow");
                } else {
                    if (UNLIKELY(res > max_result.value || res < -max_result.value)) {
                        throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                        "Arithmetic overflow, convert failed from {}, "
                                        "expected data is [{}, {}]",
                                        res, -max_result.value, max_result.value);
                    } else {
                        vec_to[i] = ToFieldType(res);
                    }
                }
            } else {
                res = vec_from[i].value * multiplier;
                if constexpr (narrow_integral) {
                    if (UNLIKELY(res > max_result.value || res < -max_result.value)) {
                        throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                        "Arithmetic overflow, convert failed from {}, "
                                        "expected data is [{}, {}]",
                                        res, -max_result.value, max_result.value);
                    }
                }
                vec_to[i] = ToFieldType(res);
            }
        }
    } else if (scale_to == scale_from) {
        for (size_t i = 0; i < sz; i++) {
            if constexpr (narrow_integral) {
                if (UNLIKELY(vec_from[i].value > max_result.value ||
                             vec_from[i].value < -max_result.value)) {
                    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                    "Arithmetic overflow, convert failed from {}, "
                                    "expected data is [{}, {}]",
                                    vec_from[i].value, -max_result.value, max_result.value);
                }
            }
            vec_to[i] = ToFieldType(vec_from[i].value);
        }
    } else {
        MaxNativeType multiplier =
                DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_from - scale_to);
        MaxNativeType res;
        for (size_t i = 0; i < sz; i++) {
            if (vec_from[i] >= FromFieldType(0)) {
                if constexpr (narrow_integral) {
                    res = (vec_from[i].value + multiplier / 2) / multiplier;
                    if (UNLIKELY(res > max_result.value)) {
                        throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                        "Arithmetic overflow, convert failed from {}, "
                                        "expected data is [{}, {}]",
                                        res, -max_result.value, max_result.value);
                    }
                    vec_to[i] = ToFieldType(res);
                } else {
                    vec_to[i] = ToFieldType((vec_from[i].value + multiplier / 2) / multiplier);
                }
            } else {
                if constexpr (narrow_integral) {
                    res = (vec_from[i].value - multiplier / 2) / multiplier;
                    if (UNLIKELY(res < -max_result.value)) {
                        throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                        "Arithmetic overflow, convert failed from {}, "
                                        "expected data is [{}, {}]",
                                        res, -max_result.value, max_result.value);
                    }
                    vec_to[i] = ToFieldType(res);
                } else {
                    vec_to[i] = ToFieldType((vec_from[i].value - multiplier / 2) / multiplier);
                }
            }
        }
    }
}

template <typename FromDataType, typename ToDataType, bool narrow_integral>
    requires IsDataTypeDecimal<FromDataType>
void convert_from_decimal(typename ToDataType::FieldType* dst,
                          const typename FromDataType::FieldType* src, UInt32 scale,
                          const typename ToDataType::FieldType& min_result,
                          const typename ToDataType::FieldType& max_result, size_t size) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    if constexpr (std::is_floating_point_v<ToFieldType>) {
        if constexpr (IsDecimalV2<FromFieldType>) {
            for (size_t i = 0; i < size; ++i) {
                dst[i] = binary_cast<int128_t, DecimalV2Value>(src[i]);
            }
        } else {
            auto multiplier = FromDataType::get_scale_multiplier(scale);
            for (size_t i = 0; i < size; ++i) {
                dst[i] = static_cast<ToFieldType>(src[i].value) / multiplier.value;
            }
        }
        if constexpr (narrow_integral) {
            for (size_t i = 0; i < size; i++) {
                if (dst[i] < min_result || dst[i] > max_result) {
                    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                    "Arithmetic overflow, convert failed from {}, "
                                    "expected data is [{}, {}]",
                                    dst[i], min_result, max_result);
                }
            }
        }
    } else {
        convert_from_decimals<FromDataType, FromDataType, narrow_integral>(
                dst, src, scale, FromFieldType(min_result), FromFieldType(max_result), size);
    }
}

template <typename FromDataType, typename ToDataType, bool multiply_may_overflow,
          bool narrow_integral>
    requires IsDataTypeDecimal<ToDataType>
void convert_to_decimal(typename ToDataType::FieldType* dst,
                        const typename FromDataType::FieldType* src, UInt32 from_scale,
                        UInt32 to_scale, const typename ToDataType::FieldType& min_result,
                        const typename ToDataType::FieldType& max_result, size_t size) {
    using FromFieldType = typename FromDataType::FieldType;

    if constexpr (std::is_floating_point_v<FromFieldType>) {
        auto multiplier = ToDataType::get_scale_multiplier(to_scale);
        if constexpr (narrow_integral) {
            for (size_t i = 0; i < size; ++i) {
                if (!std::isfinite(src[i])) {
                    throw Exception(
                            ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                            "Decimal convert overflow. Cannot convert infinity or NaN to decimal");
                }
                FromFieldType tmp = src[i] * multiplier;
                if (tmp <= FromFieldType(min_result) || tmp >= FromFieldType(max_result)) {
                    throw Exception(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                    "Arithmetic overflow, convert failed from {}, "
                                    "expected data is [{}, {}]",
                                    FromFieldType(tmp), FromFieldType(min_result),
                                    FromFieldType(max_result));
                }
            }
        }
        for (size_t i = 0; i < size; ++i) {
            dst[i].value = FromFieldType(src[i] * multiplier.value + ((src[i] >= 0) ? 0.5 : -0.5));
        }
    } else {
        using DecimalFrom =
                std::conditional_t<std::is_same_v<FromFieldType, Int128>, Decimal128V2,
                                   std::conditional_t<std::is_same_v<FromFieldType, wide::Int256>,
                                                      Decimal256, Decimal64>>;
        convert_to_decimals<DataTypeDecimal<DecimalFrom>, ToDataType, multiply_may_overflow,
                            narrow_integral>(dst, src, from_scale, to_scale, min_result, max_result,
                                             size);
    }
}

template <typename T>
    requires IsDecimalNumber<T>
typename T::NativeType max_decimal_value(UInt32 precision) {
    return type_limit<T>::max().value / DataTypeDecimal<T>::get_scale_multiplier(
                                                (UInt32)(max_decimal_precision<T>() - precision))
                                                .value;
}

template <typename T>
    requires IsDecimalNumber<T>
typename T::NativeType min_decimal_value(UInt32 precision) {
    return type_limit<T>::min().value / DataTypeDecimal<T>::get_scale_multiplier(
                                                (UInt32)(max_decimal_precision<T>() - precision))
                                                .value;
}
} // namespace doris::vectorized
