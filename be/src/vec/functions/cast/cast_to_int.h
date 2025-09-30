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
#include "runtime/primitive_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

// Types that can be cast to int: string, bool, int, float, double, decimal, date, datetime and time.
// It's not supported to cast date to tinyint and smallint, because it will definitely overflow.
// It's not supported to cast datetime to tinyint and smallint, because it will definitely overflow.
// Casting from string and decimal types are handled in `cast_to_basic_number_common.h`.

// may overflow if:
// 1. from wider int to narrower int
// 2. from float/double to int
// 3. from time to tinyint, smallint and int
template <typename FromDataType, typename ToDataType>
constexpr bool CastToIntMayOverflow =
        (IsDataTypeInt<FromDataType> &&
         sizeof(typename FromDataType::FieldType) > sizeof(typename ToDataType::FieldType)) ||
        IsDataTypeFloat<FromDataType> ||
        (std::is_same_v<FromDataType, DataTypeTimeV2> &&
         (std::is_same_v<ToDataType, DataTypeInt32> || std::is_same_v<ToDataType, DataTypeInt16> ||
          std::is_same_v<ToDataType, DataTypeInt8>));

template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeInt<ToDataType> &&
             (IsDataTypeNumber<FromDataType> || std::is_same_v<FromDataType, DataTypeTimeV2>) &&
             CastToIntMayOverflow<FromDataType, ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<typename FromDataType::ColumnType>(
                block.get_by_position(arguments[0]).column.get());
        if (!col_from) {
            return Status::InternalError(
                    fmt::format("Column type mismatch: expected {}, got {}",
                                type_to_string(FromDataType::PType),
                                block.get_by_position(arguments[0]).column->get_name()));
        }
        auto col_to = ToDataType::ColumnType::create(input_rows_count);
        const auto& vec_from = col_from->get_data();
        auto& vec_to = col_to->get_data();
        constexpr bool result_is_nullable = (CastMode == CastModeType::NonStrictMode);
        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* vec_null_map_to = nullptr;
        if constexpr (result_is_nullable) {
            col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
            vec_null_map_to = col_null_map_to->get_data().data();
        }

        CastParameters params;
        params.is_strict = (CastMode == CastModeType::StrictMode);
        for (size_t i = 0; i < input_rows_count; ++i) {
            if constexpr (IsDataTypeInt<FromDataType>) {
                if (!CastToInt::from_int(vec_from[i], vec_to[i], params)) {
                    if constexpr (CastMode == CastModeType::NonStrictMode) {
                        vec_null_map_to[i] = 1;
                        continue;
                    } else {
                        return params.status;
                    }
                }
            } else if constexpr (IsDataTypeFloat<FromDataType>) {
                if (!CastToInt::from_float(vec_from[i], vec_to[i], params)) {
                    if constexpr (CastMode == CastModeType::NonStrictMode) {
                        vec_null_map_to[i] = 1;
                        continue;
                    } else {
                        return params.status;
                    }
                }
            } else if constexpr (std::is_same_v<FromDataType, DataTypeTimeV2>) {
                if (!CastToInt::from_time(vec_from[i], vec_to[i], params)) {
                    if constexpr (CastMode == CastModeType::NonStrictMode) {
                        vec_null_map_to[i] = 1;
                        continue;
                    } else {
                        return params.status;
                    }
                }
            } else {
                return Status::InternalError(fmt::format("Unsupported cast from {} to {}",
                                                         type_to_string(FromDataType::PType),
                                                         type_to_string(ToDataType::PType)));
            }
        }
        if constexpr (result_is_nullable) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }

        return Status::OK();
    }
};

/// cast to int, will not overflow:
/// 1. from bool;
/// 2. from narrow int to wider int
/// 3. from time to bigint and largeint
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeInt<ToDataType> &&
             (IsDataTypeNumber<FromDataType> || std::is_same_v<FromDataType, DataTypeTimeV2>) &&
             !CastToIntMayOverflow<FromDataType, ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        return static_cast_no_overflow<FromDataType, ToDataType>(context, block, arguments, result,
                                                                 input_rows_count);
    }
};

// cast decimal to integer
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeInt<ToDataType> && IsDataTypeDecimal<FromDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using ToFieldType = typename ToDataType::FieldType;
        using FromFieldType = typename FromDataType::FieldType;

        const ColumnWithTypeAndName& named_from = block.get_by_position(arguments[0]);
        const auto* col_from =
                check_and_get_column<typename FromDataType::ColumnType>(named_from.column.get());
        if (!col_from) {
            return Status::InternalError(fmt::format("Column type mismatch: expected {}, got {}",
                                                     type_to_string(FromDataType::PType),
                                                     named_from.column->get_name()));
        }
        const auto& from_decimal_type = assert_cast<const FromDataType&>(*named_from.type);
        UInt32 from_precision = from_decimal_type.get_precision();
        UInt32 from_scale = from_decimal_type.get_scale();

        constexpr UInt32 to_max_digits = NumberTraits::max_ascii_len<ToFieldType>();
        bool narrow_integral = (from_precision - from_scale) >= to_max_digits;

        // may overflow if integer part of decimal is larger than to_max_digits
        bool may_overflow = (from_precision - from_scale) >= to_max_digits;
        bool result_is_nullable = (CastMode == CastModeType::NonStrictMode) && may_overflow;

        auto col_to = ToDataType::ColumnType::create(input_rows_count);
        const auto& vec_from = col_from->get_data();
        const auto* vec_from_data = vec_from.data();
        auto& vec_to = col_to->get_data();
        auto* vec_to_data = vec_to.data();

        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* null_map_data = nullptr;
        if (result_is_nullable) {
            col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
            null_map_data = col_null_map_to->get_data().data();
        }

        CastParameters params;
        params.is_strict = (CastMode == CastModeType::StrictMode);
        size_t size = vec_from.size();
        typename FromFieldType::NativeType scale_multiplier =
                DataTypeDecimal<FromFieldType::PType>::get_scale_multiplier(from_scale);
        for (size_t i = 0; i < size; i++) {
            if (!CastToInt::_from_decimal<typename FromDataType::FieldType,
                                          typename ToDataType::FieldType>(
                        vec_from_data[i], from_precision, from_scale, vec_to_data[i],
                        scale_multiplier, narrow_integral, params)) {
                if (result_is_nullable) {
                    null_map_data[i] = 1;
                } else {
                    return params.status;
                }
            }
        }

        if (result_is_nullable) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

template <typename T>
constexpr static bool int_allow_cast_from_date =
        std::is_same_v<T, DataTypeInt32> || std::is_same_v<T, DataTypeInt64> ||
        std::is_same_v<T, DataTypeInt128>;

template <typename T>
constexpr static bool int_allow_cast_from_datetime =
        std::is_same_v<T, DataTypeInt64> || std::is_same_v<T, DataTypeInt128>;

// cast from date and datetime to int
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(((IsDateType<FromDataType> ||
               IsDateV2Type<FromDataType>)&&int_allow_cast_from_date<ToDataType>) ||
             ((IsDateTimeType<FromDataType> ||
               IsDateTimeV2Type<FromDataType>)&&int_allow_cast_from_datetime<ToDataType>))
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        return static_cast_no_overflow<FromDataType, ToDataType>(context, block, arguments, result,
                                                                 input_rows_count);
    }
};
namespace CastWrapper {

template <typename ToDataType>
WrapperType create_int_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_impl;

    auto make_cast_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (type_allow_cast_to_basic_number<FromDataType>) {
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
                fmt::format("CAST AS number not supported {}", from_type->get_name()));
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