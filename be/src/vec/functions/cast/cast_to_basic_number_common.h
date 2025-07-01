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

#include <type_traits>

#include "cast_base.h"
#include "common/status.h"
#include "runtime/primitive_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized {

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

    for (size_t i = 0; i < input_rows_count; ++i) {
        if constexpr (IsDatelikeV1Types<FromDataType>) {
            CastUtil::static_cast_set(
                    vec_to[i], reinterpret_cast<const VecDateTimeValue&>(vec_from[i]).to_int64());

        } else if constexpr (IsDateTimeV2Type<FromDataType>) {
            CastUtil::static_cast_set(
                    vec_to[i],
                    reinterpret_cast<const DateV2Value<DateTimeV2ValueType>&>(vec_from[i])
                            .to_int64());
        } else if constexpr (IsDateV2Type<FromDataType>) {
            CastUtil::static_cast_set(
                    vec_to[i],
                    reinterpret_cast<const DateV2Value<DateV2ValueType>&>(vec_from[i]).to_int64());
        } else {
            CastUtil::static_cast_set(vec_to[i], vec_from[i]);
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

// cast decimal types to integer, float and double
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeDecimal<FromDataType> &&
             (IsDataTypeInt<ToDataType> || IsDataTypeFloat<ToDataType>))
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        using ToFieldType = typename ToDataType::FieldType;

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

        ToFieldType max_result = type_limit<ToFieldType>::max();
        ToFieldType min_result = type_limit<ToFieldType>::min();

        UInt32 to_max_digits = NumberTraits::max_ascii_len<ToFieldType>();

        // may overflow if:
        // 1. cast to int and integer part of decimal is larger than to_max_digits
        // 2. cast to float and integer part of decimal is larger than 39
        bool may_overflow =
                (IsDataTypeInt<ToDataType> && (from_precision - from_scale) >= to_max_digits) ||
                (from_precision - from_scale >= 39 && std::is_same_v<ToDataType, DataTypeFloat32>);
        bool result_is_nullable = (CastMode == CastModeType::NonStrictMode) && may_overflow;

        auto col_to = ToDataType::ColumnType::create(input_rows_count);
        const auto& vec_from = col_from->get_data();
        auto& vec_to = col_to->get_data();

        ColumnUInt8::MutablePtr col_null_map_to;
        NullMap::value_type* vec_null_map_to = nullptr;
        if (result_is_nullable) {
            col_null_map_to = ColumnUInt8::create(input_rows_count, 0);
            vec_null_map_to = col_null_map_to->get_data().data();
        }

        size_t size = vec_from.size();
        RETURN_IF_ERROR(std::visit(
                [&](auto narrow_integral, auto result_is_nullable) {
                    return convert_from_decimal<FromDataType, ToDataType, narrow_integral,
                                                result_is_nullable>(
                            vec_to.data(), vec_from.data(), from_precision, vec_from.get_scale(),
                            min_result, max_result, size, vec_null_map_to);
                },
                make_bool_variant(may_overflow), make_bool_variant(result_is_nullable)));

        if (result_is_nullable) {
            block.get_by_position(result).column =
                    ColumnNullable::create(std::move(col_to), std::move(col_null_map_to));
        } else {
            block.get_by_position(result).column = std::move(col_to);
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
