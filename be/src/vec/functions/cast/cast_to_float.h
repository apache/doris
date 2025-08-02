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

#include "cast_to_basic_number_common.h"
#include "runtime/primitive_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
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
            if constexpr (IsDecimalV2<FromFieldType>) {
                vec_to_data[i] = binary_cast<int128_t, DecimalV2Value>(vec_from_data[i]);
            } else {
                CastToFloat::_from_decimalv3(vec_from_data[i], from_scale, vec_to_data[i],
                                             scale_multiplier, params);
            }
        }

        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

// cast date and datetime to float/double, will not overflow
// only support in non-strict mode
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

namespace CastWrapper {

// max float: 3.40282e+38
// max int128:
// >> 0x7fffffffffffffffffffffffffffffff
// 170141183460469231731687303715884105727
// >>> len('170141183460469231731687303715884105727')
// 39
template <typename ToDataType>
WrapperType create_float_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
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
