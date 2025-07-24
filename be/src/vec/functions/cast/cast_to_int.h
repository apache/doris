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
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
// cast bool, int, float, double and time to int
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires((IsDataTypeNumber<FromDataType> || std::is_same_v<FromDataType, DataTypeTimeV2>) &&
             IsDataTypeInt<ToDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        // may overflow if:
        // 1. from wider int to narrower int
        // 2. from float/double to int
        // 3. from time to tinyint, smallint and int
        constexpr bool may_overflow =
                (IsDataTypeInt<FromDataType> && sizeof(typename FromDataType::FieldType) >
                                                        sizeof(typename ToDataType::FieldType)) ||
                IsDataTypeFloat<FromDataType> ||
                (std::is_same_v<FromDataType, DataTypeTimeV2> &&
                 (std::is_same_v<ToDataType, DataTypeInt32> ||
                  std::is_same_v<ToDataType, DataTypeInt16> ||
                  std::is_same_v<ToDataType, DataTypeInt8>));
        constexpr bool result_is_nullable =
                (CastMode == CastModeType::NonStrictMode) && may_overflow;

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

        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* vec_null_map_to = nullptr;
        if constexpr (result_is_nullable) {
            col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
            vec_null_map_to = col_null_map_to->get_data().data();
        }

        using ToFieldType = typename ToDataType::FieldType;
        constexpr auto min_to_value = std::numeric_limits<ToFieldType>::min();
        constexpr auto max_to_value = std::numeric_limits<ToFieldType>::max();

        for (size_t i = 0; i < input_rows_count; ++i) {
            if constexpr (may_overflow) {
                if constexpr (IsDataTypeFloat<FromDataType>) {
                    if (std::isinf(vec_from[i]) || std::isnan(vec_from[i])) {
                        if constexpr (CastMode == CastModeType::NonStrictMode) {
                            vec_null_map_to[i] = 1;
                            continue;
                        } else {
                            return Status::InternalError(
                                    fmt::format("Value {} out of range for type {}", vec_from[i],
                                                type_to_string(ToDataType::PType)));
                        }
                    }
                    auto truncated_value = std::trunc(vec_from[i]);
                    if (truncated_value < min_to_value ||
                        truncated_value > static_cast<double>(max_to_value)) {
                        // overflow
                        if constexpr (CastMode == CastModeType::NonStrictMode) {
                            vec_null_map_to[i] = 1;
                            continue;
                        } else {
                            return Status::InternalError(
                                    fmt::format("Value {} out of range for type {}", vec_from[i],
                                                type_to_string(ToDataType::PType)));
                        }
                    }
                } else {
                    if (vec_from[i] < min_to_value || vec_from[i] > max_to_value) {
                        // overflow
                        if constexpr (CastMode == CastModeType::NonStrictMode) {
                            vec_null_map_to[i] = 1;
                            continue;
                        } else {
                            return Status::InternalError(
                                    fmt::format("Value {} out of range for type {}", vec_from[i],
                                                type_to_string(ToDataType::PType)));
                        }
                    }
                }
            }
            CastUtil::static_cast_set(vec_to[i], vec_from[i]);
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