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
#include <type_traits>

#include "cast_to_basic_number_common.h"
#include "common/status.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

#define DECIMAL_CONVERT_OVERFLOW_ERROR(value, from_type_name, precision, scale)                    \
    Status(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,                                                  \
           fmt::format(                                                                            \
                   "Arithmetic overflow when converting value {} from type {} to decimal({}, {})", \
                   value, from_type_name, precision, scale))
struct CastToDecimal {
    template <typename ToCppT>
        requires(IsDecimalNumber<ToCppT>)
    static inline bool from_string(const StringRef& from, ToCppT& to, UInt32 precision,
                                   UInt32 scale, CastParameters& params) {
        if constexpr (IsDecimalV2<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMALV2>(to, from, precision, scale);
        }

        if constexpr (IsDecimal32<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMAL32>(to, from, precision, scale);
        }

        if constexpr (IsDecimal64<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMAL64>(to, from, precision, scale);
        }

        if constexpr (IsDecimal128V3<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMAL128I>(to, from, precision, scale);
        }

        if constexpr (IsDecimal256<ToCppT>) {
            return StringParser::PARSE_SUCCESS ==
                   try_read_decimal_text<TYPE_DECIMAL256>(to, from, precision, scale);
        }
    }

    // cast int to decimal
    template <typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
              typename MaxFieldType =
                      std::conditional_t<(sizeof(FromCppT) > sizeof(typename ToCppT::NativeType)),
                                         FromCppT, typename ToCppT::NativeType>>
        requires(IsDecimalNumber<ToCppT> && IsCppTypeInt<FromCppT>)
    static inline bool from_int(const FromCppT& from, ToCppT& to, UInt32 precision, UInt32 scale,
                                const MaxFieldType& scale_multiplier,
                                const typename ToCppT::NativeType& min_result,
                                const typename ToCppT::NativeType& max_result,
                                CastParameters& params) {
        return _from_int<FromCppT, ToCppT, multiply_may_overflow, narrow_integral>(
                from, to, precision, scale, scale_multiplier, min_result, max_result, params);
    }

    // cast bool to decimal
    template <typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
              typename MaxFieldType =
                      std::conditional_t<(sizeof(FromCppT) > sizeof(typename ToCppT::NativeType)),
                                         FromCppT, typename ToCppT::NativeType>>
        requires(IsDecimalNumber<ToCppT> && std::is_same_v<FromCppT, vectorized::UInt8>)
    static inline bool from_bool(const FromCppT& from, ToCppT& to, UInt32 precision, UInt32 scale,
                                 const MaxFieldType& scale_multiplier,
                                 const typename ToCppT::NativeType& min_result,
                                 const typename ToCppT::NativeType& max_result,
                                 CastParameters& params) {
        return _from_int<FromCppT, ToCppT, multiply_may_overflow, narrow_integral>(
                from, to, precision, scale, scale_multiplier, min_result, max_result, params);
    }

    template <typename FromCppT, typename ToCppT>
        requires(IsDecimalNumber<ToCppT> && IsCppTypeFloat<FromCppT>)
    static inline bool from_float(const FromCppT& from, ToCppT& to, UInt32 precision, UInt32 scale,
                                  const ToCppT::NativeType& scale_multiplier,
                                  const typename ToCppT::NativeType& min_result,
                                  const typename ToCppT::NativeType& max_result,
                                  CastParameters& params) {
        if (!std::isfinite(from)) {
            params.status = Status(ErrorCode::ARITHMETIC_OVERFLOW_ERRROR,
                                   "Decimal convert overflow. Cannot convert infinity or NaN "
                                   "to decimal");
            return false;
        }
        // For decimal256, we need to use long double to avoid overflow when
        // static casting the multiplier to floating type, and also to be as precise as possible;
        // For other decimal types, we use double to be as precise as possible.
        using DoubleType = std::conditional_t<IsDecimal256<ToCppT>, long double, double>;
        auto tmp = static_cast<double>(from * static_cast<DoubleType>(scale_multiplier) +
                                       ((from >= 0) ? 0.5 : -0.5));
        if (tmp <= static_cast<FromCppT>(min_result) || tmp >= static_cast<FromCppT>(max_result)) {
            params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(from, "float/double", precision, scale);
            return false;
        }
        to.value = static_cast<ToCppT::NativeType>(tmp);
        return true;
    }

    template <
            typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
            typename MaxFieldType = std::conditional_t<
                    (sizeof(FromCppT) == sizeof(ToCppT)) &&
                            (std::is_same_v<ToCppT, Decimal128V3> ||
                             std::is_same_v<FromCppT, Decimal128V3>),
                    Decimal128V3::NativeType,
                    std::conditional_t<(sizeof(FromCppT) > sizeof(ToCppT)),
                                       typename FromCppT::NativeType, typename ToCppT::NativeType>>>
        requires(IsDecimalNumber<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool from_decimal_smaller_scale(
            const FromCppT& from, const UInt32 precision_from, const UInt32 scale_from, ToCppT& to,
            UInt32 precision_to, UInt32 scale_to, const MaxFieldType& scale_multiplier,
            const ToCppT& min_result, const ToCppT& max_result, CastParameters& params) {
        MaxFieldType res;
        return std::visit(
                [&](auto is_strict_cast) {
                    if constexpr (multiply_may_overflow) {
                        if (common::mul_overflow(static_cast<MaxFieldType>(from.value),
                                                 scale_multiplier, res)) {
                            if constexpr (is_strict_cast) {
                                params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                        decimal_to_string(from.value, scale_from),
                                        fmt::format("decimal({}, {})", precision_from, scale_from),
                                        precision_to, scale_to);
                            }
                            return false;
                        } else {
                            if (UNLIKELY(res > max_result.value || res < -max_result.value)) {
                                if constexpr (is_strict_cast) {
                                    params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                            decimal_to_string(from.value, scale_from),
                                            fmt::format("decimal({}, {})", precision_from,
                                                        scale_from),
                                            precision_to, scale_to);
                                }
                                return false;
                            } else {
                                to = ToCppT(res);
                            }
                        }
                    } else {
                        res = from.value * scale_multiplier;
                        if constexpr (narrow_integral) {
                            if (UNLIKELY(res > max_result.value || res < -max_result.value)) {
                                if constexpr (is_strict_cast) {
                                    params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                            decimal_to_string(from.value, scale_from),
                                            fmt::format("decimal({}, {})", precision_from,
                                                        scale_from),
                                            precision_to, scale_to);
                                }
                                return false;
                            }
                        }
                        to = ToCppT(res);
                    }
                    return true;
                },
                vectorized::make_bool_variant(params.is_strict));
    }

    template <typename FromCppT, typename ToCppT, typename ScaleT, bool multiply_may_overflow,
              bool narrow_integral>
        requires(IsDecimalNumber<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool from_decimal_same_scale(const FromCppT& from, const UInt32 precision_from,
                                               const UInt32 scale_from, ToCppT& to,
                                               UInt32 precision_to, UInt32 scale_to,
                                               const ToCppT& min_result, const ToCppT& max_result,
                                               CastParameters& params) {
        if constexpr (narrow_integral) {
            if (UNLIKELY(from.value > max_result.value || from.value < -max_result.value)) {
                std::visit(
                        [&](auto is_strict_cast) {
                            if constexpr (is_strict_cast) {
                                params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                        decimal_to_string(from.value, scale_from),
                                        fmt::format("decimal({}, {})", precision_from, scale_from),
                                        precision_to, scale_to);
                            }
                        },
                        vectorized::make_bool_variant(params.is_strict));
                return false;
            }
        }
        to = ToCppT(from.value);
        return true;
    }

    template <
            typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
            typename MaxFieldType = std::conditional_t<
                    (sizeof(FromCppT) == sizeof(ToCppT)) &&
                            (std::is_same_v<ToCppT, Decimal128V3> ||
                             std::is_same_v<FromCppT, Decimal128V3>),
                    Decimal128V3::NativeType,
                    std::conditional_t<(sizeof(FromCppT) > sizeof(ToCppT)),
                                       typename FromCppT::NativeType, typename ToCppT::NativeType>>>
        requires(IsDecimalNumber<ToCppT> && IsDecimalNumber<FromCppT>)
    static inline bool from_decimal_bigger_scale(const FromCppT& from, const UInt32 precision_from,
                                                 const UInt32 scale_from, ToCppT& to,
                                                 UInt32 precision_to, UInt32 scale_to,
                                                 const MaxFieldType& scale_multiplier,
                                                 const ToCppT& min_result, const ToCppT& max_result,
                                                 CastParameters& params) {
        MaxFieldType res;
        if (from >= FromCppT(0)) {
            if constexpr (narrow_integral) {
                res = (from.value + scale_multiplier / 2) / scale_multiplier;
                if (UNLIKELY(res > max_result.value)) {
                    std::visit(
                            [&](auto is_strict_cast) {
                                if constexpr (is_strict_cast) {
                                    params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                            decimal_to_string(from.value, scale_from),
                                            fmt::format("decimal({}, {})", precision_from,
                                                        scale_from),
                                            precision_to, scale_to);
                                }
                            },
                            vectorized::make_bool_variant(params.is_strict));
                    return false;
                }
                to = ToCppT(res);
            } else {
                to = ToCppT((from.value + scale_multiplier / 2) / scale_multiplier);
            }
        } else {
            if constexpr (narrow_integral) {
                res = (from.value - scale_multiplier / 2) / scale_multiplier;
                if (UNLIKELY(res < -max_result.value)) {
                    std::visit(
                            [&](auto is_strict_cast) {
                                if constexpr (is_strict_cast) {
                                    params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                            decimal_to_string(from.value, scale_from),
                                            fmt::format("decimal({}, {})", precision_from,
                                                        scale_from),
                                            precision_to, scale_to);
                                }
                            },
                            vectorized::make_bool_variant(params.is_strict));
                    return false;
                }
                to = ToCppT(res);
            } else {
                to = ToCppT((from.value - scale_multiplier / 2) / scale_multiplier);
            }
        }
        return true;
    }

    template <typename FromCppT, typename ToCppT, bool multiply_may_overflow, bool narrow_integral,
              typename MaxFieldType =
                      std::conditional_t<(sizeof(FromCppT) > sizeof(typename ToCppT::NativeType)),
                                         FromCppT, typename ToCppT::NativeType>>
        requires(IsDecimalNumber<ToCppT> &&
                 (IsCppTypeInt<FromCppT> || std::is_same_v<FromCppT, vectorized::UInt8>))
    static inline bool _from_int(const FromCppT& from, ToCppT& to, UInt32 precision, UInt32 scale,
                                 const MaxFieldType& scale_multiplier,
                                 const typename ToCppT::NativeType& min_result,
                                 const typename ToCppT::NativeType& max_result,
                                 CastParameters& params) {
        if constexpr (multiply_may_overflow) {
            MaxFieldType tmp;
            if (common::mul_overflow(static_cast<MaxFieldType>(from), scale_multiplier, tmp)) {
                std::visit(
                        [&](auto is_strict_cast) {
                            if constexpr (is_strict_cast) {
                                params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                        from, int_type_name<FromCppT>, precision, scale);
                            }
                        },
                        vectorized::make_bool_variant(params.is_strict));
                return false;
            }
            if constexpr (narrow_integral) {
                if (tmp < min_result || tmp > max_result) {
                    std::visit(
                            [&](auto is_strict_cast) {
                                if constexpr (is_strict_cast) {
                                    params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                            from, int_type_name<FromCppT>, precision, scale);
                                }
                            },
                            vectorized::make_bool_variant(params.is_strict));
                    return false;
                }
            }
            to.value = static_cast<typename ToCppT::NativeType>(tmp);
        } else {
            to.value = static_cast<typename ToCppT::NativeType>(
                    scale_multiplier * static_cast<typename ToCppT::NativeType>(from));
            if constexpr (narrow_integral) {
                if (to.value < min_result || to.value > max_result) {
                    std::visit(
                            [&](auto is_strict_cast) {
                                if constexpr (is_strict_cast) {
                                    params.status = DECIMAL_CONVERT_OVERFLOW_ERROR(
                                            from, int_type_name<FromCppT>, precision, scale);
                                }
                            },
                            vectorized::make_bool_variant(params.is_strict));
                    return false;
                }
            }
        }

        return true;
    }
};

// Casting from string to decimal types.
template <CastModeType Mode, typename ToDataType>
    requires(IsDataTypeDecimal<ToDataType>)
class CastToImpl<Mode, DataTypeString, ToDataType> : public CastToBase {
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
                block.get_by_position(arguments[0]).column.get());

        auto to_type = block.get_by_position(result).type;
        auto serde = remove_nullable(to_type)->get_serde();
        MutableColumnPtr column_to;

        if constexpr (Mode == CastModeType::NonStrictMode) {
            auto to_nullable_type = make_nullable(to_type);
            column_to = to_nullable_type->create_column();
            auto& nullable_col_to = assert_cast<ColumnNullable&>(*column_to);
            RETURN_IF_ERROR(serde->from_string_batch(*col_from, nullable_col_to, {}));
        } else if constexpr (Mode == CastModeType::StrictMode) {
            if (to_type->is_nullable()) {
                return Status::InternalError(
                        "result type should be not nullable when casting string to decimal in "
                        "strict cast mode");
            }
            column_to = to_type->create_column();
            RETURN_IF_ERROR(
                    serde->from_string_strict_mode_batch(*col_from, *column_to, {}, null_map));
        } else {
            return Status::InternalError("Unsupported cast mode");
        }

        block.get_by_position(result).column = std::move(column_to);
        return Status::OK();
    }
};

// cast bool and int to decimal
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimal<ToDataType> &&
             (IsDataTypeInt<FromDataType> || IsDataTypeBool<FromDataType>))
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using FromFieldType = typename FromDataType::FieldType;
        using ToFieldType = typename ToDataType::FieldType;
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        UInt32 from_precision = NumberTraits::max_ascii_len<FromFieldType>();
        constexpr UInt32 from_scale = 0;
        constexpr UInt32 to_max_digits =
                NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();

        const ColumnWithTypeAndName& named_to = block.get_by_position(result);
        const auto& to_decimal_type = assert_cast<const ToDataType&>(*named_to.type);
        UInt32 to_precision = to_decimal_type.get_precision();
        ToDataType::check_type_precision(to_precision);
        UInt32 to_scale = to_decimal_type.get_scale();
        ToDataType::check_type_scale(to_scale);

        auto from_max_int_digit_count = from_precision - from_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral =
                (to_max_int_digit_count < from_max_int_digit_count) ||
                (to_max_int_digit_count == from_max_int_digit_count && to_scale < from_scale);
        bool multiply_may_overflow = false;
        if (to_scale > from_scale || IsDataTypeInt<FromDataType>) {
            multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
        }
        bool result_is_nullable = (CastMode == CastModeType::NonStrictMode) && narrow_integral;

        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* null_map_data = nullptr;
        if (result_is_nullable) {
            col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
            null_map_data = col_null_map_to->get_data().data();
        }

        auto col_to = ToDataType::ColumnType::create(input_rows_count, to_scale);
        const auto& vec_from = col_from->get_data();
        const auto* vec_from_data = vec_from.data();
        auto& vec_to = col_to->get_data();
        auto* vec_to_data = vec_to.data();

        ToFieldType max_result = ToDataType::get_max_digits_number(to_precision);
        ToFieldType min_result = -max_result;

        CastParameters params;
        size_t size = vec_from.size();
        RETURN_IF_ERROR(std::visit(
                [&](auto multiply_may_overflow, auto narrow_integral, auto result_is_nullable) {
                    using MaxFieldType =
                            std::conditional_t<(sizeof(FromFieldType) >
                                                sizeof(typename ToFieldType::NativeType)),
                                               FromFieldType, typename ToFieldType::NativeType>;
                    MaxFieldType multiplier = ToDataType::get_scale_multiplier(to_scale);
                    for (size_t i = 0; i < size; i++) {
                        if (!CastToDecimal::_from_int<typename FromDataType::FieldType,
                                                      typename ToDataType::FieldType,
                                                      multiply_may_overflow, narrow_integral>(
                                    vec_from_data[i], vec_to_data[i], to_precision, to_scale,
                                    multiplier, min_result, max_result, params)) {
                            if constexpr (result_is_nullable) {
                                null_map_data[i] = 1;
                            } else {
                                return params.status;
                            }
                        }
                    }
                    return Status::OK();
                },
                make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral),
                make_bool_variant(result_is_nullable)));

        if (result_is_nullable) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

// cast float and double to decimal
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimal<ToDataType> && IsDataTypeFloat<FromDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using FromFieldType = typename FromDataType::FieldType;
        using ToFieldType = typename ToDataType::FieldType;
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        UInt32 from_precision = NumberTraits::max_ascii_len<FromFieldType>();
        UInt32 from_scale = 0;

        const ColumnWithTypeAndName& named_to = block.get_by_position(result);
        const auto& to_decimal_type = assert_cast<const ToDataType&>(*named_to.type);
        UInt32 to_precision = to_decimal_type.get_precision();
        ToDataType::check_type_precision(to_precision);
        UInt32 to_scale = to_decimal_type.get_scale();
        ToDataType::check_type_scale(to_scale);

        ToFieldType max_result = ToDataType::get_max_digits_number(to_precision);
        ToFieldType min_result = -max_result;

        auto from_max_int_digit_count = from_precision - from_scale;
        auto to_max_int_digit_count = to_precision - to_scale;
        bool narrow_integral =
                (to_max_int_digit_count < from_max_int_digit_count) ||
                (to_max_int_digit_count == from_max_int_digit_count && to_scale < from_scale);
        bool result_is_nullable = (CastMode == CastModeType::NonStrictMode) && narrow_integral;

        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* null_map_data = nullptr;
        if (result_is_nullable) {
            col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
            null_map_data = col_null_map_to->get_data().data();
        }

        auto col_to = ToDataType::ColumnType::create(input_rows_count, to_scale);
        const auto& vec_from = col_from->get_data();
        const auto* vec_from_data = vec_from.data();
        auto& vec_to = col_to->get_data();
        auto* vec_to_data = vec_to.data();

        CastParameters params;
        size_t size = vec_from.size();
        RETURN_IF_ERROR(std::visit(
                [&](auto result_is_nullable) {
                    auto multiplier = ToDataType::get_scale_multiplier(to_scale);
                    for (size_t i = 0; i < size; i++) {
                        if (!CastToDecimal::from_float<typename FromDataType::FieldType,
                                                       typename ToDataType::FieldType>(
                                    vec_from_data[i], vec_to_data[i], to_precision, to_scale,
                                    multiplier.value, min_result, max_result, params)) {
                            if constexpr (result_is_nullable) {
                                null_map_data[i] = 1;
                            } else {
                                return params.status;
                            }
                        }
                    }
                    return Status::OK();
                },
                make_bool_variant(result_is_nullable)));

        if (result_is_nullable) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

// cast decimalv3 types to decimalv2 types
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimalV3<ToDataType> && IsDataTypeDecimalV2<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        return Status::RuntimeError(
                "not support {} ",
                cast_mode_type_to_string(CastMode, block.get_by_position(arguments[0]).type,
                                         block.get_by_position(result).type));
    }
};

template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimal<FromDataType> && IsDataTypeDecimalV3<ToDataType>)
Status caset_between_decimal_types(Block& block, uint32_t result,
                                   const typename FromDataType::ColumnType* col_from,
                                   UInt32 from_precision, UInt32 from_scale,
                                   UInt32 from_max_int_digit_count) {
    using FromFieldType = typename FromDataType::FieldType;
    using ToFieldType = typename ToDataType::FieldType;
    const ColumnWithTypeAndName& named_to = block.get_by_position(result);
    const auto& to_decimal_type = assert_cast<const ToDataType&>(*named_to.type);
    UInt32 to_precision = to_decimal_type.get_precision();
    ToDataType::check_type_precision(to_precision);
    UInt32 to_scale = to_decimal_type.get_scale();
    ToDataType::check_type_scale(to_scale);

    auto to_max_int_digit_count = to_precision - to_scale;
    size_t size = col_from->size();
    UInt32 to_max_digits = NumberTraits::max_ascii_len<typename ToFieldType::NativeType>();
    bool narrow_integral =
            (to_max_int_digit_count < from_max_int_digit_count) ||
            (to_max_int_digit_count == from_max_int_digit_count && to_scale < from_scale);
    bool multiply_may_overflow = false;
    if (to_scale > from_scale) {
        multiply_may_overflow = (from_precision + to_scale - from_scale) >= to_max_digits;
    }
    bool result_is_nullable = (CastMode == CastModeType::NonStrictMode) && narrow_integral;

    ColumnUInt8::MutablePtr col_null_map_to;
    NullMap::value_type* null_map_data = nullptr;
    if (result_is_nullable) {
        col_null_map_to = ColumnUInt8::create(size, 0);
        null_map_data = col_null_map_to->get_data().data();
    }

    auto col_to = ToDataType::ColumnType::create(size, to_scale);
    const auto& vec_from = col_from->get_data();
    const auto* vec_from_data = vec_from.data();
    auto& vec_to = col_to->get_data();
    auto* vec_to_data = vec_to.data();

    ToFieldType max_result = ToDataType::get_max_digits_number(to_precision);
    ToFieldType min_result = -max_result;

    CastParameters params;
    RETURN_IF_ERROR(std::visit(
            [&](auto multiply_may_overflow, auto narrow_integral, auto result_is_nullable) {
                using MaxFieldType = std::conditional_t<
                        (sizeof(FromFieldType) == sizeof(ToFieldType)) &&
                                (std::is_same_v<ToFieldType, Decimal128V3> ||
                                 std::is_same_v<FromFieldType, Decimal128V3>),
                        Decimal128V3,
                        std::conditional_t<(sizeof(FromFieldType) > sizeof(ToFieldType)),
                                           FromFieldType, ToFieldType>>;
                using MaxNativeType = typename MaxFieldType::NativeType;
                if (from_scale < to_scale) {
                    const MaxNativeType multiplier =
                            DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(to_scale -
                                                                                       from_scale);
                    for (size_t i = 0; i < size; i++) {
                        if (!CastToDecimal::from_decimal_smaller_scale<
                                    typename FromDataType::FieldType,
                                    typename ToDataType::FieldType, multiply_may_overflow,
                                    narrow_integral>(vec_from_data[i], from_precision, from_scale,
                                                     vec_to_data[i], to_precision, to_scale,
                                                     multiplier, min_result, max_result, params)) {
                            if constexpr (result_is_nullable) {
                                null_map_data[i] = 1;
                            } else {
                                return params.status;
                            }
                        }
                    }
                } else if (from_scale == to_scale) {
                    for (size_t i = 0; i < size; i++) {
                        if (!CastToDecimal::from_decimal_same_scale<
                                    typename FromDataType::FieldType,
                                    typename ToDataType::FieldType, MaxNativeType,
                                    multiply_may_overflow, narrow_integral>(
                                    vec_from_data[i], from_precision, from_scale, vec_to_data[i],
                                    to_precision, to_scale, min_result, max_result, params)) {
                            if constexpr (result_is_nullable) {
                                null_map_data[i] = 1;
                            } else {
                                return params.status;
                            }
                        }
                    }
                } else {
                    const MaxNativeType multiplier =
                            DataTypeDecimal<MaxFieldType::PType>::get_scale_multiplier(from_scale -
                                                                                       to_scale);
                    for (size_t i = 0; i < size; i++) {
                        if (!CastToDecimal::from_decimal_bigger_scale<
                                    typename FromDataType::FieldType,
                                    typename ToDataType::FieldType, multiply_may_overflow,
                                    narrow_integral>(vec_from_data[i], from_precision, from_scale,
                                                     vec_to_data[i], to_precision, to_scale,
                                                     multiplier, min_result, max_result, params)) {
                            if constexpr (result_is_nullable) {
                                null_map_data[i] = 1;
                            } else {
                                return params.status;
                            }
                        }
                    }
                }
                return Status::OK();
            },
            make_bool_variant(multiply_may_overflow), make_bool_variant(narrow_integral),
            make_bool_variant(result_is_nullable)));

    if (result_is_nullable) {
        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
    } else {
        block.get_by_position(result).column = std::move(col_to);
    }
    return Status::OK();
}

// cast decimalv2 types to decimalv3 types
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimalV2<FromDataType> && IsDataTypeDecimalV3<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        const auto& from_decimal_type = assert_cast<const FromDataType&>(*named_from.type);
        UInt32 from_precision = from_decimal_type.get_precision();
        UInt32 from_scale = from_decimal_type.get_scale();
        UInt32 from_original_precision = from_decimal_type.get_original_precision();
        UInt32 from_original_scale = from_decimal_type.get_original_scale();

        auto from_max_int_digit_count = from_original_precision - from_original_scale;
        return caset_between_decimal_types<CastMode, FromDataType, ToDataType>(
                block, result, col_from, from_precision, from_scale, from_max_int_digit_count);
    }
};

// cast between decimalv3 types
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimalV3<ToDataType> && IsDataTypeDecimalV3<FromDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::RuntimeError("Illegal column {} of first argument of function cast",
                                        named_from.column->get_name());
        }

        const auto& from_decimal_type = assert_cast<const FromDataType&>(*named_from.type);
        UInt32 from_precision = from_decimal_type.get_precision();
        UInt32 from_scale = from_decimal_type.get_scale();

        auto from_max_int_digit_count = from_precision - from_scale;
        return caset_between_decimal_types<CastMode, FromDataType, ToDataType>(
                block, result, col_from, from_precision, from_scale, from_max_int_digit_count);
    }
};

template <typename T>
constexpr static bool type_allow_cast_to_decimal =
        std::is_same_v<T, DataTypeString> || IsDataTypeNumber<T> || IsDataTypeDecimal<T>;

namespace CastWrapper {

template <typename ToDataType>
WrapperType create_decimal_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_impl;

    auto make_cast_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (type_allow_cast_to_decimal<FromDataType>) {
            if (context->enable_strict_mode()) {
                cast_impl = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, ToDataType>>();
            } else {
                cast_impl = std::make_shared<
                        CastToImpl<CastModeType::NonStrictMode, FromDataType, ToDataType>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(), make_cast_wrapper)) {
        return create_unsupport_wrapper(
                fmt::format("CAST AS decimal not supported {}", from_type->get_name()));
    }

    return [cast_impl](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                       uint32_t result, size_t input_rows_count,
                       const NullMap::value_type* null_map = nullptr) {
        return cast_impl->execute_impl(context, block, arguments, result, input_rows_count,
                                       null_map);
    };
}
} // namespace CastWrapper
#include "common/compile_check_end.h"
} // namespace doris::vectorized