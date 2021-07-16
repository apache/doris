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
#include <cmath>

#include "vec/columns/column_decimal.h"
#include "vec/common/arithmetic_overflow.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {

static constexpr size_t min_decimal_precision() {
    return 1;
}
template <typename T>
static constexpr size_t max_decimal_precision() {
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

DataTypePtr create_decimal(UInt64 precision, UInt64 scale);

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
///     S is maximum scale of operands. The allowed valuas are [0, precision]
template <typename T>
class DataTypeDecimal final : public IDataType {
    static_assert(IsDecimalNumber<T>);

public:
    using FieldType = T;
    using ColumnType = ColumnDecimal<T>;

    static constexpr bool is_parametric = true;

    static constexpr size_t max_precision() { return max_decimal_precision<T>(); }

    DataTypeDecimal(UInt32 precision_, UInt32 scale_) : precision(precision_), scale(scale_) {
        if (UNLIKELY(precision < 1 || precision > max_precision())) {
            LOG(FATAL) << fmt::format("Precision {} is out of bounds", precision);
        }

        if (UNLIKELY(scale < 0 || static_cast<UInt32>(scale) > max_precision())) {
            LOG(FATAL) << fmt::format("Scale {} is out of bounds", scale);
        }
    }

    const char* get_family_name() const override { return "Decimal"; }
    std::string do_get_name() const override;
    TypeIndex get_type_id() const override { return TypeId<T>::value; }

    size_t serialize(const IColumn& column, PColumn* pcolumn) const override;
    void deserialize(const PColumn& pcolumn, IColumn* column) const override;
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
    std::string to_string(const IColumn& column, size_t row_num) const;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const;

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

private:
    const UInt32 precision;
    const UInt32 scale;
};

template <typename T, typename U>
typename std::enable_if_t<(sizeof(T) >= sizeof(U)), const DataTypeDecimal<T>> decimal_result_type(
        const DataTypeDecimal<T>& tx, const DataTypeDecimal<U>& ty, bool is_multiply,
        bool is_divide) {
    UInt32 scale = (tx.get_scale() > ty.get_scale() ? tx.get_scale() : ty.get_scale());
    if (is_multiply)
        scale = tx.get_scale() + ty.get_scale();
    else if (is_divide)
        scale = tx.get_scale();
    return DataTypeDecimal<T>(max_decimal_precision<T>(), scale);
}

template <typename T, typename U>
typename std::enable_if_t<(sizeof(T) < sizeof(U)), const DataTypeDecimal<U>> decimal_result_type(
        const DataTypeDecimal<T>& tx, const DataTypeDecimal<U>& ty, bool is_multiply,
        bool is_divide) {
    UInt32 scale = (tx.get_scale() > ty.get_scale() ? tx.get_scale() : ty.get_scale());
    if (is_multiply)
        scale = tx.get_scale() * ty.get_scale();
    else if (is_divide)
        scale = tx.get_scale();
    return DataTypeDecimal<U>(max_decimal_precision<U>(), scale);
}

template <typename T, typename U>
const DataTypeDecimal<T> decimal_result_type(const DataTypeDecimal<T>& tx, const DataTypeNumber<U>&,
                                             bool, bool) {
    return DataTypeDecimal<T>(max_decimal_precision<T>(), tx.get_scale());
}

template <typename T, typename U>
const DataTypeDecimal<U> decimal_result_type(const DataTypeNumber<T>&, const DataTypeDecimal<U>& ty,
                                             bool, bool) {
    return DataTypeDecimal<U>(max_decimal_precision<U>(), ty.get_scale());
}

template <typename T>
inline const DataTypeDecimal<T>* check_decimal(const IDataType& data_type) {
    return typeid_cast<const DataTypeDecimal<T>*>(&data_type);
}

inline UInt32 get_decimal_scale(const IDataType& data_type,
                                UInt32 default_value = std::numeric_limits<UInt32>::max()) {
    if (auto* decimal_type = check_decimal<Decimal32>(data_type)) return decimal_type->get_scale();
    if (auto* decimal_type = check_decimal<Decimal64>(data_type)) return decimal_type->get_scale();
    if (auto* decimal_type = check_decimal<Decimal128>(data_type)) return decimal_type->get_scale();
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

template <typename DataType>
constexpr bool IsDataTypeDecimalOrNumber =
        IsDataTypeDecimal<DataType> || IsDataTypeNumber<DataType>;

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeDecimal<ToDataType>,
                        typename ToDataType::FieldType>
convert_decimals(const typename FromDataType::FieldType& value, UInt32 scale_from,
                 UInt32 scale_to) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    using MaxFieldType = std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                            FromFieldType, ToFieldType>;
    using MaxNativeType = typename MaxFieldType::NativeType;

    MaxNativeType converted_value;
    if (scale_to > scale_from) {
        converted_value =
                DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_to - scale_from);
        if (common::mul_overflow(static_cast<MaxNativeType>(value), converted_value,
                                 converted_value)) {
            LOG(FATAL) << "Decimal convert overflow";
        }
    } else
        converted_value =
                value / DataTypeDecimal<MaxFieldType>::get_scale_multiplier(scale_from - scale_to);

    if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType)) {
        if (converted_value < std::numeric_limits<typename ToFieldType::NativeType>::min() ||
            converted_value > std::numeric_limits<typename ToFieldType::NativeType>::max()) {
            LOG(FATAL) << "Decimal convert overflow";
        }
    }

    return converted_value;
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeDecimal<FromDataType> && IsDataTypeNumber<ToDataType>,
                        typename ToDataType::FieldType>
convert_from_decimal(const typename FromDataType::FieldType& value, UInt32 scale) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;

    if constexpr (std::is_floating_point_v<ToFieldType>)
        return static_cast<ToFieldType>(value) / FromDataType::get_scale_multiplier(scale);
    else {
        FromFieldType converted_value =
                convert_decimals<FromDataType, FromDataType>(value, scale, 0);

        if constexpr (sizeof(FromFieldType) > sizeof(ToFieldType) ||
                      !std::numeric_limits<ToFieldType>::is_signed) {
            if constexpr (std::numeric_limits<ToFieldType>::is_signed) {
                if (converted_value < std::numeric_limits<ToFieldType>::min() ||
                    converted_value > std::numeric_limits<ToFieldType>::max()) {
                    LOG(FATAL) << "Decimal convert overflow";
                }
            } else {
                using CastIntType =
                        std::conditional_t<std::is_same_v<ToFieldType, UInt64>, Int128, Int64>;

                if (converted_value < 0 ||
                    converted_value >
                            static_cast<CastIntType>(std::numeric_limits<ToFieldType>::max())) {
                    LOG(FATAL) << "Decimal convert overflow";
                }
            }
        }
        return converted_value;
    }
}

template <typename FromDataType, typename ToDataType>
inline std::enable_if_t<IsDataTypeNumber<FromDataType> && IsDataTypeDecimal<ToDataType>,
                        typename ToDataType::FieldType>
convert_to_decimal(const typename FromDataType::FieldType& value, UInt32 scale) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToNativeType = typename ToDataType::FieldType::NativeType;

    if constexpr (std::is_floating_point_v<FromFieldType>) {
        if (!std::isfinite(value)) {
            LOG(FATAL) << "Decimal convert overflow. Cannot convert infinity or NaN to decimal";
        }

        auto out = value * ToDataType::get_scale_multiplier(scale);
        if constexpr (std::is_same_v<ToNativeType, Int128>) {
            static constexpr __int128 min_int128 = __int128(0x8000000000000000ll) << 64;
            static constexpr __int128 max_int128 =
                    (__int128(0x7fffffffffffffffll) << 64) + 0xffffffffffffffffll;
            if (out <= static_cast<ToNativeType>(min_int128) ||
                out >= static_cast<ToNativeType>(max_int128)) {
                LOG(FATAL) << "Decimal convert overflow. Float is out of Decimal range";
            }
        } else {
            if (out <= std::numeric_limits<ToNativeType>::min() ||
                out >= std::numeric_limits<ToNativeType>::max()) {
                LOG(FATAL) << "Decimal convert overflow. Float is out of Decimal range";
            }
        }
        return out;
    } else {
        if constexpr (std::is_same_v<FromFieldType, UInt64>)
            if (value > static_cast<UInt64>(std::numeric_limits<Int64>::max()))
                return convert_decimals<DataTypeDecimal<Decimal128>, ToDataType>(value, 0, scale);
        return convert_decimals<DataTypeDecimal<Decimal64>, ToDataType>(value, 0, scale);
    }
}

} // namespace doris::vectorized
