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

#include "core/data_type/data_type_decimal.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "exprs/function/cast/cast_to_basic_number_common.h"

namespace doris {
// cast bool, integer, float to double, will not overflow
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeFloat<ToDataType> && IsDataTypeNumber<FromDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        return static_cast_no_overflow<FromDataType, ToDataType>(context, block, arguments, result,
                                                                 input_rows_count);
    }
};

// cast decimal to float and double
template <CastModeType CastMode, typename FromDataType, typename ToDataType>
    requires(IsDataTypeFloat<ToDataType> && IsDataTypeDecimal<FromDataType>)
class CastToImpl<CastMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
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
        UInt32 from_scale = from_decimal_type.get_scale();

        auto col_to = ToDataType::ColumnType::create(input_rows_count);
        const auto& vec_from = col_from->get_data();
        const auto* vec_from_data = vec_from.data();
        auto& vec_to = col_to->get_data();
        auto* vec_to_data = vec_to.data();

        CastParameters params;
        params.is_strict = (CastMode == CastModeType::StrictMode);
        size_t size = vec_from.size();

        typename FromFieldType::NativeType scale_multiplier =
                DataTypeDecimal<FromFieldType::PType>::get_scale_multiplier(from_scale);
        for (size_t i = 0; i < size; ++i) {
            CastToFloat::_from_decimalv3(vec_from_data[i], from_scale, vec_to_data[i],
                                         scale_multiplier, params);
        }

        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

// cast date and datetime to float/double, will not overflow
// only support in non-strict mode. strict mode it's illegal!
template <typename FromDataType, typename ToDataType>
    requires(IsDataTypeFloat<ToDataType> &&
             (IsDatelikeV1Types<FromDataType> || IsDatelikeV2Types<FromDataType> ||
              std::is_same_v<FromDataType, DataTypeTimeV2>))
class CastToImpl<CastModeType::NonStrictMode, FromDataType, ToDataType> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        return static_cast_no_overflow<FromDataType, ToDataType>(context, block, arguments, result,
                                                                 input_rows_count);
    }
};
} // namespace doris
