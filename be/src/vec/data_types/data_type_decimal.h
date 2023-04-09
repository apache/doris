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
#include <cmath>
#include <type_traits>

#include "common/config.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column_decimal.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"

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
    return 9;
}
template <>
constexpr size_t max_decimal_precision<Decimal64>() {
    return 18;
}
template <>
constexpr size_t max_decimal_precision<Decimal128>() {
    return 38;
}
template <>
constexpr size_t max_decimal_precision<Decimal128I>() {
    return 38;
}

template <typename T>
constexpr typename T::NativeType max_decimal_value() {
    return 0;
}
template <>
constexpr Int32 max_decimal_value<Decimal32>() {
    return 999999999;
}
template <>
constexpr Int64 max_decimal_value<Decimal64>() {
    return 999999999999999999;
}
template <>
constexpr Int128 max_decimal_value<Decimal128>() {
    return static_cast<int128_t>(999999999999999999ll) * 100000000000000000ll * 1000ll +
           static_cast<int128_t>(99999999999999999ll) * 1000ll + 999ll;
}
template <>
constexpr Int128 max_decimal_value<Decimal128I>() {
    return static_cast<int128_t>(999999999999999999ll) * 100000000000000000ll * 1000ll +
           static_cast<int128_t>(99999999999999999ll) * 1000ll + 999ll;
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
        if (UNLIKELY(precision < 1 || precision > max_precision())) {
            LOG(FATAL) << fmt::format("Precision {} is out of bounds", precision);
        }

        if (UNLIKELY(static_cast<UInt32>(scale) > max_precision())) {
            LOG(FATAL) << fmt::format("Scale {} is out of bounds", scale);
        }
    }

    DataTypeDecimal(const DataTypeDecimal& rhs) : precision(rhs.precision), scale(rhs.scale) {}

    const char* get_family_name() const override { return "Decimal"; }
    std::string do_get_name() const override;
    TypeIndex get_type_id() const override { return TypeId<T>::value; }
    PrimitiveType get_type_as_primitive_type() const override {
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal32>>) {
            return TYPE_DECIMAL32;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal64>>) {
            return TYPE_DECIMAL64;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal128I>>) {
            return TYPE_DECIMAL128I;
        }
        __builtin_unreachable();
    }

    TPrimitiveType::type get_type_as_tprimitive_type() const override {
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal32>>) {
            return TPrimitiveType::DECIMAL32;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal64>>) {
            return TPrimitiveType::DECIMAL64;
        }
        if constexpr (std::is_same_v<TypeId<T>, TypeId<Decimal128I>>) {
            return TPrimitiveType::DECIMAL128I;
        }
        __builtin_unreachable();
    }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;

    void to_pb_column_meta(PColumnMeta* col_meta) const override;

    Field get_default() const override;
    bool can_be_promoted() const override { return true; }
    DataTypePtr promote_numeric_type() const override;
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

    bool is_summable() const override { return true; }
    bool can_be_used_in_boolean_context() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;

    /// Decimal specific

    UInt32 get_precision() const { return precision; }
    UInt32 get_scale() const { return scale; }
    T get_scale_multiplier() const { return get_scale_multiplier(scale); }

    T whole_part(T x) const {
        if (scale == 0) return x;
        return x / get_scale_multiplier();
    }

    T fractional_part(T x) const {
        if (scale == 0) return 0;
        if (x < T(0)) x *= T(-1);
        return x % get_scale_multiplier();
    }

    T max_whole_value() const { return get_scale_multiplier(max_precision() - scale) - T(1); }

    bool can_store_whole(T x) const {
        T max = max_whole_value();
        if (x > max || x < -max) return false;
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
        if (is_multiply_or_divisor) return 1;
        return get_scale_multiplier();
    }

    static T get_scale_multiplier(UInt32 scale);

    T parse_from_string(const std::string& str) const;

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
            precision = std::min(multiply_precision, max_decimal_precision<Decimal128I>());
        } else if (is_divide) {
            scale = tx.get_scale();
            precision = std::min(divide_precision, max_decimal_precision<Decimal128I>());
        } else if (is_plus_minus) {
            precision = std::min(plus_minus_precision, max_decimal_precision<Decimal128I>());
        }
        return create_decimal(precision, scale, false);
    }
}

template <typename T>
const DataTypeDecimal<T>* check_decimal(const IDataType& data_type) {
    return typeid_cast<const DataTypeDecimal<T>*>(&data_type);
}

inline UInt32 get_decimal_scale(const IDataType& data_type, UInt32 default_value = 0) {
    if (auto* decimal_type = check_decimal<Decimal32>(data_type)) return decimal_type->get_scale();
    if (auto* decimal_type = check_decimal<Decimal64>(data_type)) return decimal_type->get_scale();
    if (auto* decimal_type = check_decimal<Decimal128>(data_type)) return decimal_type->get_scale();
    if (auto* decimal_type = check_decimal<Decimal128I>(data_type))
        return decimal_type->get_scale();
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
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal128>> = true;
template <>
inline constexpr bool IsDataTypeDecimal<DataTypeDecimal<Decimal128I>> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimalV2 = false;
template <>
inline constexpr bool IsDataTypeDecimalV2<DataTypeDecimal<Decimal128>> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimal128I = false;
template <>
inline constexpr bool IsDataTypeDecimal128I<DataTypeDecimal<Decimal128I>> = true;

template <typename DataType>
constexpr bool IsDataTypeDecimalOrNumber =
        IsDataTypeDecimal<DataType> || IsDataTypeNumber<DataType>;

template <typename FromDataType, typename ToDataType>
std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>,
                 typename ToDataType::FieldType>
convert_decimals(const typename FromDataType::FieldType& value, UInt32 scale_from, UInt32 scale_to,
                 UInt8* overflow_flag = nullptr) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType =
            std::conditional_t<(sizeof(FromFieldType) == sizeof(ToFieldType)) &&
                                       (std::is_same_v<ToFieldType, Decimal128I> ||
                                        std::is_same_v<FromFieldType, Decimal128I>),
                               Decimal128I,
                               std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                                  FromFieldType, ToFieldType>>;
    using MaxNativeType = typename MaxFieldType::NativeType;

    MaxNativeType converted_value;
    if (scale_to > scale_from) {
        converted_value =
                DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_to - scale_from);
        if (common::mul_overflow(static_cast<MaxNativeType>(value), converted_value,
                                 converted_value)) {
            if (overflow_flag) {
                *overflow_flag = 1;
            }
            VLOG_DEBUG << "Decimal convert overflow";
            return converted_value < 0
                           ? std::numeric_limits<typename ToFieldType::NativeType>::min()
                           : std::numeric_limits<typename ToFieldType::NativeType>::max();
        }
    } else {
        converted_value =
                value / DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_from - scale_to);
    }

    if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType)) {
        if (converted_value < std::numeric_limits<typename ToFieldType::NativeType>::min()) {
            if (overflow_flag) {
                *overflow_flag = 1;
            }
            VLOG_DEBUG << "Decimal convert overflow";
            return std::numeric_limits<typename ToFieldType::NativeType>::min();
        }
        if (converted_value > std::numeric_limits<typename ToFieldType::NativeType>::max()) {
            if (overflow_flag) {
                *overflow_flag = 1;
            }
            VLOG_DEBUG << "Decimal convert overflow";
            return std::numeric_limits<typename ToFieldType::NativeType>::max();
        }
    }

    return converted_value;
}

template <typename FromDataType, typename ToDataType>
void convert_decimal_cols(
        const typename ColumnDecimal<
                typename FromDataType::FieldType>::Container::value_type* __restrict vec_from,
        typename ColumnDecimal<typename ToDataType::FieldType>::Container::value_type* vec_to,
        const UInt32 scale_from, const UInt32 scale_to, const size_t sz,
        UInt8* overflow_flag = nullptr) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType =
            std::conditional_t<(sizeof(FromFieldType) == sizeof(ToFieldType)) &&
                                       (std::is_same_v<ToFieldType, Decimal128I> ||
                                        std::is_same_v<FromFieldType, Decimal128I>),
                               Decimal128I,
                               std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                                  FromFieldType, ToFieldType>>;
    using MaxNativeType = typename MaxFieldType::NativeType;

    if (scale_to > scale_from) {
        const MaxNativeType multiplier =
                DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_to - scale_from);
        MaxNativeType res;
        for (size_t i = 0; i < sz; i++) {
            if (std::is_same_v<MaxNativeType, Int128>) {
                if (common::mul_overflow(static_cast<MaxNativeType>(vec_from[i]), multiplier,
                                         res)) {
                    if (overflow_flag) {
                        overflow_flag[i] = 1;
                    }
                    VLOG_DEBUG << "Decimal convert overflow";
                    vec_to[i] =
                            res < 0 ? std::numeric_limits<typename ToFieldType::NativeType>::min()
                                    : std::numeric_limits<typename ToFieldType::NativeType>::max();
                } else {
                    vec_to[i] = res;
                }
            } else {
                vec_to[i] = vec_from[i] * multiplier;
            }
        }
    } else {
        MaxNativeType multiplier =
                DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_from - scale_to);
        for (size_t i = 0; i < sz; i++) {
            vec_to[i] = vec_from[i] / multiplier;
        }
    }

    if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType)) {
        for (size_t i = 0; i < sz; i++) {
            if (vec_to[i] < std::numeric_limits<typename ToFieldType::NativeType>::min()) {
                if (overflow_flag) {
                    *overflow_flag = 1;
                }
                VLOG_DEBUG << "Decimal convert overflow";
                vec_to[i] = std::numeric_limits<typename ToFieldType::NativeType>::min();
            }
            if (vec_to[i] > std::numeric_limits<typename ToFieldType::NativeType>::max()) {
                if (overflow_flag) {
                    *overflow_flag = 1;
                }
                VLOG_DEBUG << "Decimal convert overflow";
                vec_to[i] = std::numeric_limits<typename ToFieldType::NativeType>::max();
            }
        }
    }
}

template <typename FromDataType, typename ToDataType>
std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeNumber<ToDataType>,
                 typename ToDataType::FieldType>
convert_from_decimal(const typename FromDataType::FieldType& value, UInt32 scale) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    if constexpr (std::is_floating_point_v<ToFieldType>) {
        if constexpr (IsDecimalV2<FromFieldType>) {
            return binary_cast<int128_t, DecimalV2Value>(value);
        } else {
            return static_cast<ToFieldType>(value) / FromDataType::get_scale_multiplier(scale);
        }
    } else {
        FromFieldType converted_value =
                convert_decimals<FromDataType, FromDataType>(value, scale, 0);

        if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType) ||
                      !std::numeric_limits<ToFieldType>::is_signed) {
            if constexpr (std::numeric_limits<ToFieldType>::is_signed) {
                if (converted_value < std::numeric_limits<ToFieldType>::min()) {
                    VLOG_DEBUG << "Decimal convert overflow";
                    return std::numeric_limits<ToFieldType>::min();
                }
                if (converted_value > std::numeric_limits<ToFieldType>::max()) {
                    VLOG_DEBUG << "Decimal convert overflow";
                    return std::numeric_limits<ToFieldType>::max();
                }
            } else {
                using CastIntType =
                        std::conditional_t<std::is_same_v<ToFieldType, UInt64>, Int128, Int64>;

                if (converted_value < 0 ||
                    converted_value >
                            static_cast<CastIntType>(std::numeric_limits<ToFieldType>::max())) {
                    VLOG_DEBUG << "Decimal convert overflow";
                    return std::numeric_limits<ToFieldType>::max();
                }
            }
        }
        return converted_value;
    }
}

template <typename FromDataType, typename ToDataType>
std::enable_if_t<IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>,
                 typename ToDataType::FieldType>
convert_to_decimal(const typename FromDataType::FieldType& value, UInt32 scale,
                   UInt8* overflow_flag) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToNativeType = typename ToDataType::FieldType::NativeType;

    if constexpr (std::is_floating_point_v<FromFieldType>) {
        if (!std::isfinite(value)) {
            if (overflow_flag) {
                *overflow_flag = 1;
            }
            VLOG_DEBUG << "Decimal convert overflow. Cannot convert infinity or NaN to decimal";
            return value < 0 ? std::numeric_limits<ToNativeType>::min()
                             : std::numeric_limits<ToNativeType>::max();
        }

        FromFieldType out;
        out = value * ToDataType::get_scale_multiplier(scale);
        if (out <= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::min())) {
            if (overflow_flag) {
                *overflow_flag = 1;
            }
            VLOG_DEBUG << "Decimal convert overflow. Float is out of Decimal range";
            return std::numeric_limits<ToNativeType>::min();
        }
        if (out >= static_cast<FromFieldType>(std::numeric_limits<ToNativeType>::max())) {
            if (overflow_flag) {
                *overflow_flag = 1;
            }
            VLOG_DEBUG << "Decimal convert overflow. Float is out of Decimal range";
            return std::numeric_limits<ToNativeType>::max();
        }
        return out;
    } else {
        if constexpr (std::is_same_v<FromFieldType, UInt64>) {
            if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max())) {
                return convert_decimals<DataTypeDecimal<Decimal128>, ToDataType>(value, 0, scale);
            }
        }
        return convert_decimals<DataTypeDecimal<Decimal64>, ToDataType>(value, 0, scale);
    }
}

template <typename T>
typename T::NativeType max_decimal_value(UInt32 precision);

template <typename T>
typename T::NativeType min_decimal_value(UInt32 precision);

} // namespace doris::vectorized
