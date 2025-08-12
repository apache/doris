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

#include <limits>
#include <type_traits>

#include "cast_base.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <typename CppT>
static inline constexpr const char* int_type_name = std::is_same_v<CppT, vectorized::UInt8> ? "bool"
                                                    : std::is_same_v<CppT, int8_t>     ? "tinyint"
                                                    : std::is_same_v<CppT, int16_t>    ? "smallint"
                                                    : std::is_same_v<CppT, int32_t>    ? "int"
                                                    : std::is_same_v<CppT, int64_t>    ? "bigint"
                                                    : std::is_same_v<CppT, __int128_t> ? "largeint"
                                                                                       : "unknown";

template <typename CppT>
constexpr bool IsCppTypeInt =
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_TINYINT>::ColumnItemType> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_SMALLINT>::ColumnItemType> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_INT>::ColumnItemType> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_BIGINT>::ColumnItemType> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_LARGEINT>::ColumnItemType>;

template <typename CppT>
constexpr bool IsCppTypeFloat =
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_FLOAT>::ColumnItemType> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_DOUBLE>::ColumnItemType>;

template <typename CppT>
constexpr bool IsCppTypeNumberOrTime =
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_BOOLEAN>::ColumnItemType> ||
        IsCppTypeInt<CppT> || IsCppTypeFloat<CppT> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_TIMEV2>::ColumnItemType>;

template <typename T, typename FloatingType>
    requires(IsCppTypeInt<T> and IsCppTypeFloat<FloatingType>)
struct ValidFloatingRange {};

template <typename FloatingType>
struct ValidFloatingRange<int8_t, FloatingType> {
    static constexpr FloatingType UPPER = 0x1p7;
    static constexpr FloatingType LOWER = -0x1p7;
};

template <typename FloatingType>
struct ValidFloatingRange<int16_t, FloatingType> {
    static constexpr FloatingType UPPER = 0x1p15;
    static constexpr FloatingType LOWER = -0x1p15;
};

template <typename FloatingType>
struct ValidFloatingRange<int32_t, FloatingType> {
    static constexpr FloatingType UPPER = 0x1p31;
    static constexpr FloatingType LOWER = -0x1p31;
};

template <typename FloatingType>
struct ValidFloatingRange<int64_t, FloatingType> {
    static constexpr FloatingType UPPER = 0x1p63;
    static constexpr FloatingType LOWER = -0x1p63;
};

template <typename FloatingType>
struct ValidFloatingRange<int128_t, FloatingType> {
    static constexpr FloatingType UPPER = 0x1p127;
    static constexpr FloatingType LOWER = -0x1p127;
};

// cast to int, may overflow if:
// 1. from wider int to narrower int
// 2. from float/double to int
// 3. from time to tinyint, smallint and int
template <typename FromCppT, typename ToCppT>
constexpr bool CastToIntFromWiderInt = IsCppTypeInt<FromCppT> && IsCppTypeInt<ToCppT> &&
                                       sizeof(FromCppT) > sizeof(ToCppT);

template <typename FromCppT, typename ToCppT>
constexpr bool CastToIntFromTimeMayOverflow =
        std::is_same_v<FromCppT, PrimitiveTypeTraits<TYPE_TIMEV2>::ColumnItemType> &&
        (std::is_same_v<FromCppT, PrimitiveTypeTraits<TYPE_INT>::ColumnItemType> ||
         std::is_same_v<FromCppT, PrimitiveTypeTraits<TYPE_SMALLINT>::ColumnItemType> ||
         std::is_same_v<FromCppT, PrimitiveTypeTraits<TYPE_TINYINT>::ColumnItemType>);

template <typename FromCppT, typename ToCppT>
constexpr bool CastToIntCppTypeMayOverflow =
        CastToIntFromWiderInt<FromCppT, ToCppT> || IsCppTypeFloat<FromCppT> ||
        CastToIntFromTimeMayOverflow<FromCppT, ToCppT>;

template <typename CppT>
constexpr static bool IntAllowCastFromDate =
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_INT>::ColumnItemType> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_BIGINT>::ColumnItemType> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_LARGEINT>::ColumnItemType>;

template <typename CppT>
constexpr static bool IntAllowCastFromDatetime =
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_BIGINT>::ColumnItemType> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_LARGEINT>::ColumnItemType>;

template <typename CppT>
constexpr bool IsCppTypeDate = std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_DATE>::CppType> ||
                               std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_DATEV2>::CppType>;

template <typename CppT>
constexpr bool IsCppTypeDateTime =
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_DATETIME>::CppType> ||
        std::is_same_v<CppT, PrimitiveTypeTraits<TYPE_DATETIMEV2>::CppType>;
struct CastToInt {
    template <typename ToCppT>
        requires(IsCppTypeInt<ToCppT>)
    static inline bool from_string(const StringRef& from, ToCppT& to, CastParameters& params) {
        return std::visit(
                [&](auto is_strict_mode) {
                    if constexpr (is_strict_mode) {
                        return try_read_int_text<ToCppT, true>(to, from);
                    } else {
                        return try_read_int_text<ToCppT, false>(to, from);
                    }
                },
                vectorized::make_bool_variant(params.is_strict));
    }

    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeInt<ToCppT> &&
                 std::is_same_v<FromCppT, PrimitiveTypeTraits<TYPE_BOOLEAN>::ColumnItemType>)
    static inline bool from_bool(FromCppT from, ToCppT& to, CastParameters& params) {
        CastUtil::static_cast_set(to, from);
        return true;
    }

    // from wider int to narrower int, may overflow
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeInt<ToCppT> && IsCppTypeInt<FromCppT> &&
                 CastToIntFromWiderInt<FromCppT, ToCppT>)
    static inline bool from_int(FromCppT from, ToCppT& to, CastParameters& params) {
        constexpr auto min_to_value = std::numeric_limits<ToCppT>::min();
        constexpr auto max_to_value = std::numeric_limits<ToCppT>::max();
        if (from < min_to_value || from > max_to_value) {
            // overflow
            if (params.is_strict) {
                params.status = Status::InternalError(fmt::format(
                        "Value {} out of range for type {}", from, int_type_name<ToCppT>));
            }
            return false;
        }
        CastUtil::static_cast_set(to, from);
        return true;
    }

    // from narrower int to wider int, no overflow
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeInt<ToCppT> && IsCppTypeInt<FromCppT> &&
                 !CastToIntFromWiderInt<FromCppT, ToCppT>)
    static inline bool from_int(FromCppT from, ToCppT& to, CastParameters& params) {
        CastUtil::static_cast_set(to, from);
        return true;
    }

    // from float/double to int, may overflow
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeInt<ToCppT> && IsCppTypeFloat<FromCppT>)
    static inline bool from_float(FromCppT from, ToCppT& to, CastParameters& params) {
        if (std::isinf(from) || std::isnan(from)) {
            if (params.is_strict) {
                params.status = Status::InternalError(fmt::format(
                        "Value {} out of range for type {}", from, int_type_name<ToCppT>));
            }
            return false;
        }
        auto truncated_value = std::trunc(from);
        if (truncated_value < ValidFloatingRange<ToCppT, FromCppT>::LOWER ||
            truncated_value >= ValidFloatingRange<ToCppT, FromCppT>::UPPER) {
            // overflow
            if (params.is_strict) {
                params.status = Status::InternalError(fmt::format(
                        "Value {} out of range for type {}", from, int_type_name<ToCppT>));
            }
            return false;
        }
        CastUtil::static_cast_set(to, from);
        return true;
    }

    // from decimal to int, may overflow
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeInt<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool from_decimal(FromCppT from, UInt32 from_precision, UInt32 from_scale,
                                    ToCppT& to, CastParameters& params) {
        typename FromCppT::NativeType scale_multiplier =
                DataTypeDecimal<FromCppT::PType>::get_scale_multiplier(from_scale);
        constexpr UInt32 to_max_digits = NumberTraits::max_ascii_len<ToCppT>();
        bool narrow_integral = (from_precision - from_scale) >= to_max_digits;
        return _from_decimal(from, from_precision, from_scale, to, scale_multiplier,
                             narrow_integral, params);
    }

    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeInt<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool _from_decimal(FromCppT from, UInt32 from_precision, UInt32 from_scale,
                                     ToCppT& to,
                                     const typename FromCppT::NativeType& scale_multiplier,
                                     bool narrow_integral, CastParameters& params) {
        constexpr auto min_result = std::numeric_limits<ToCppT>::lowest();
        constexpr auto max_result = std::numeric_limits<ToCppT>::max();
        auto tmp = from.value / scale_multiplier;
        if (narrow_integral) {
            if (tmp < min_result || tmp > max_result) {
                params.status = Status::Error(
                        ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                        fmt::format("Arithmetic overflow when converting "
                                    "value {} from type {} to type {}",
                                    decimal_to_string(from.value, from_scale),
                                    type_to_string(FromCppT::PType), int_type_name<ToCppT>));
                return false;
            }
        }
        to = static_cast<ToCppT>(tmp);
        return true;
    }

    // cast from date and datetime to int
    template <typename FromCppT, typename ToCppT>
        requires((IsCppTypeDate<FromCppT> && IntAllowCastFromDate<ToCppT>) ||
                 (IsCppTypeDateTime<FromCppT> && IntAllowCastFromDatetime<ToCppT>))
    static inline bool from_datetime(FromCppT from, ToCppT& to, CastParameters& params) {
        CastUtil::static_cast_set(to, from.to_int64());
        return true;
    }

    // from time to bigint and largeint, will not overflow
    template <typename FromCppT, typename ToCppT>
        requires(std::is_same_v<ToCppT, PrimitiveTypeTraits<TYPE_BIGINT>::CppType> ||
                 std::is_same_v<ToCppT, PrimitiveTypeTraits<TYPE_LARGEINT>::CppType>)
    static inline bool from_time(FromCppT from, ToCppT& to, CastParameters& params) {
        CastUtil::static_cast_set(to, from);
        return true;
    }

    // from time to tinyint, smallint and int, may overflow
    template <typename FromCppT, typename ToCppT>
        requires(std::is_same_v<ToCppT, PrimitiveTypeTraits<TYPE_TINYINT>::CppType> ||
                 std::is_same_v<ToCppT, PrimitiveTypeTraits<TYPE_SMALLINT>::CppType> ||
                 std::is_same_v<ToCppT, PrimitiveTypeTraits<TYPE_INT>::CppType>)
    static inline bool from_time(FromCppT from, ToCppT& to, CastParameters& params) {
        constexpr auto min_to_value = std::numeric_limits<ToCppT>::min();
        constexpr auto max_to_value = std::numeric_limits<ToCppT>::max();
        if (from < min_to_value || from > max_to_value) {
            // overflow
            if (params.is_strict) {
                params.status = Status::InternalError(fmt::format(
                        "Value {} out of range for type {}", from, int_type_name<ToCppT>));
            }
            return false;
        }
        CastUtil::static_cast_set(to, from);
        return true;
    }
};

struct CastToFloat {
    template <typename ToCppT>
        requires(IsCppTypeFloat<ToCppT>)
    static inline bool from_string(const StringRef& from, ToCppT& to, CastParameters& params) {
        return try_read_float_text(to, from);
    }
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeFloat<ToCppT> &&
                 std::is_same_v<FromCppT, PrimitiveTypeTraits<TYPE_BOOLEAN>::ColumnItemType>)
    static inline bool from_bool(const FromCppT& from, ToCppT& to, CastParameters& params) {
        CastUtil::static_cast_set(to, from);
        return true;
    }
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeFloat<ToCppT> && IsCppTypeInt<FromCppT>)
    static inline bool from_int(const FromCppT& from, ToCppT& to, CastParameters& params) {
        CastUtil::static_cast_set(to, from);
        return true;
    }
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeFloat<ToCppT> && IsCppTypeFloat<FromCppT>)
    static inline bool from_float(const FromCppT& from, ToCppT& to, CastParameters& params) {
        CastUtil::static_cast_set(to, from);
        return true;
    }
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeFloat<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool from_decimal(const FromCppT& from, UInt32 from_scale, ToCppT& to,
                                    CastParameters& params) {
        if constexpr (IsDecimalV2<FromCppT>) {
            to = binary_cast<int128_t, DecimalV2Value>(from);
            return true;
        } else {
            typename FromCppT::NativeType scale_multiplier =
                    DataTypeDecimal<FromCppT::PType>::get_scale_multiplier(from_scale);
            return _from_decimalv3(from, from_scale, to, scale_multiplier, params);
        }
    }
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeFloat<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool _from_decimalv3(const FromCppT& from, UInt32 from_scale, ToCppT& to,
                                       const typename FromCppT::NativeType& scale_multiplier,
                                       CastParameters& params) {
        if constexpr (IsDecimal256<FromCppT>) {
            to = static_cast<ToCppT>(static_cast<long double>(from.value) /
                                     static_cast<long double>(scale_multiplier));
        } else {
            to = static_cast<ToCppT>(static_cast<double>(from.value) /
                                     static_cast<double>(scale_multiplier));
        }
        return true;
    }
    // cast from date and datetime to float/double, will not overflow
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeFloat<ToCppT> && (IsCppTypeDate<FromCppT> || IsCppTypeDateTime<FromCppT>))
    static inline bool from_datetime(FromCppT from, ToCppT& to, CastParameters& params) {
        CastUtil::static_cast_set(to, from.to_int64());
        return true;
    }

    // from time to float/double, will not overflow
    template <typename FromCppT, typename ToCppT>
        requires(IsCppTypeFloat<ToCppT>)
    static inline bool from_time(FromCppT from, ToCppT& to, CastParameters& params) {
        CastUtil::static_cast_set(to, from);
        return true;
    }
};

template <typename FromDataType, typename ToDataType>
Status static_cast_no_overflow(FunctionContext* context, Block& block,
                               const ColumnNumbers& arguments, uint32_t result,
                               size_t input_rows_count) {
    const auto* col_from = check_and_get_column<typename FromDataType::ColumnType>(
            block.get_by_position(arguments[0]).column.get());
    if (!col_from) {
        return Status::InternalError(fmt::format(
                "Column type mismatch: expected {}, got {}", type_to_string(FromDataType::PType),
                block.get_by_position(arguments[0]).column->get_name()));
    }
    auto col_to = ToDataType::ColumnType::create(input_rows_count);
    const auto& vec_from = col_from->get_data();
    auto& vec_to = col_to->get_data();

    CastParameters params;
    for (size_t i = 0; i < input_rows_count; ++i) {
        if constexpr (IsDataTypeInt<ToDataType>) {
            if constexpr (IsDataTypeBool<FromDataType>) {
                CastToInt::from_bool(vec_from[i], vec_to[i], params);
            } else if constexpr (IsDataTypeInt<FromDataType>) {
                CastToInt::from_int(vec_from[i], vec_to[i], params);
            } else if constexpr (IsDatelikeV1Types<FromDataType>) {
                CastToInt::from_datetime(reinterpret_cast<const VecDateTimeValue&>(vec_from[i]),
                                         vec_to[i], params);
            } else if constexpr (IsDateTimeV2Type<FromDataType>) {
                CastToInt::from_datetime(
                        reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(vec_from[i]),
                        vec_to[i], params);
            } else if constexpr (IsDateV2Type<FromDataType>) {
                CastToInt::from_datetime(
                        reinterpret_cast<const DateV2Value<DateV2ValueType>&>(vec_from[i]),
                        vec_to[i], params);
            } else if constexpr (std::is_same_v<FromDataType, DataTypeTimeV2>) {
                CastToInt::from_time(vec_from[i], vec_to[i], params);
            } else {
                return Status::InternalError(fmt::format("Unsupported cast from {} to {}",
                                                         type_to_string(FromDataType::PType),
                                                         type_to_string(ToDataType::PType)));
            }
        } else if constexpr (IsDataTypeFloat<ToDataType>) {
            if constexpr (IsDataTypeBool<FromDataType>) {
                CastToFloat::from_bool(vec_from[i], vec_to[i], params);
            } else if constexpr (IsDataTypeInt<FromDataType>) {
                CastToFloat::from_int(vec_from[i], vec_to[i], params);
            } else if constexpr (IsDataTypeFloat<FromDataType>) {
                CastToFloat::from_float(vec_from[i], vec_to[i], params);
            } else if constexpr (IsDatelikeV1Types<FromDataType>) {
                CastToFloat::from_datetime(reinterpret_cast<const VecDateTimeValue&>(vec_from[i]),
                                           vec_to[i], params);
            } else if constexpr (IsDateTimeV2Type<FromDataType>) {
                CastToFloat::from_datetime(
                        reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(vec_from[i]),
                        vec_to[i], params);
            } else if constexpr (IsDateV2Type<FromDataType>) {
                CastToFloat::from_datetime(
                        reinterpret_cast<const DateV2Value<DateV2ValueType>&>(vec_from[i]),
                        vec_to[i], params);
            } else if constexpr (std::is_same_v<FromDataType, DataTypeTimeV2>) {
                CastToFloat::from_time(vec_from[i], vec_to[i], params);
            } else {
                return Status::InternalError(fmt::format("Unsupported cast from {} to {}",
                                                         type_to_string(FromDataType::PType),
                                                         type_to_string(ToDataType::PType)));
            }
        } else {
            return Status::InternalError(fmt::format("Unsupported cast from {} to {}",
                                                     type_to_string(FromDataType::PType),
                                                     type_to_string(ToDataType::PType)));
        }
    }

    block.get_by_position(result).column = std::move(col_to);
    return Status::OK();
}

template <typename T>
constexpr static bool type_allow_cast_to_basic_number =
        std::is_same_v<T, DataTypeString> || IsDataTypeNumber<T> || IsDataTypeDecimal<T> ||
        IsDatelikeV1Types<T> || IsDatelikeV2Types<T> || std::is_same_v<T, DataTypeTimeV2>;

// common implementation for casting string to basic number types,
// including integer, float and double
template <CastModeType Mode, typename ToDataType>
    requires(IsDataTypeNumber<ToDataType>)
class CastToImpl<Mode, DataTypeString, ToDataType> : public CastToBase {
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
                block.get_by_position(arguments[0]).column.get());
        auto to_type = block.get_by_position(result).type;
        auto serde = remove_nullable(to_type)->get_serde();
        MutableColumnPtr column_to;

        DataTypeSerDe::FormatOptions format_options;
        format_options.converted_from_string = true;

        if constexpr (Mode == CastModeType::NonStrictMode) {
            auto to_nullable_type = make_nullable(to_type);
            column_to = to_nullable_type->create_column();
            auto& nullable_col_to = assert_cast<ColumnNullable&>(*column_to);
            RETURN_IF_ERROR(serde->from_string_batch(*col_from, nullable_col_to, format_options));
        } else if constexpr (Mode == CastModeType::StrictMode) {
            if (to_type->is_nullable()) {
                return Status::InternalError(
                        "result type should be not nullable when casting string to number in "
                        "strict cast mode");
            }
            column_to = to_type->create_column();
            RETURN_IF_ERROR(serde->from_string_strict_mode_batch(*col_from, *column_to,
                                                                 format_options, null_map));
        } else {
            return Status::InternalError("Unsupported cast mode");
        }

        block.get_by_position(result).column = std::move(column_to);
        return Status::OK();
    }
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
