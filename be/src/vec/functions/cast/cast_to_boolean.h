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

#include "cast_base.h"

namespace doris::vectorized {

template <CastModeType Mode>
class CastToImpl<Mode, DataTypeString, DataTypeBool> : public CastToBase {
public:
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
                        "result type should be not nullable when casting string to boolean in "
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
template <CastModeType AllMode, typename NumberOrDecimalType>
    requires(IsDataTypeNumber<NumberOrDecimalType> || IsDataTypeDecimal<NumberOrDecimalType>)
class CastToImpl<AllMode, NumberOrDecimalType, DataTypeBool> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto* col_from = check_and_get_column<typename NumberOrDecimalType::ColumnType>(
                block.get_by_position(arguments[0]).column.get());
        DataTypeBool::ColumnType::MutablePtr col_to =
                DataTypeBool::ColumnType::create(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if constexpr (IsDataTypeDecimal<NumberOrDecimalType>) {
                using NativeType = typename NumberOrDecimalType::FieldType::NativeType;
                col_to->get_element(i) = ((NativeType)col_from->get_element(i)) != 0;
            } else {
                col_to->get_element(i) = col_from->get_element(i) != 0;
            }
        }

        block.get_by_position(result).column = std::move(col_to);
        return Status::OK();
    }
};

namespace CastWrapper {
WrapperType create_boolean_wrapper(FunctionContext* context, const DataTypePtr& from_type) {
    std::shared_ptr<CastToBase> cast_to_bool;

    auto make_bool_wrapper = [&](const auto& types) -> bool {
        using Types = std::decay_t<decltype(types)>;
        using FromDataType = typename Types::LeftType;
        if constexpr (CastUtil::IsBaseCastFromType<FromDataType>) {
            if (context->enable_strict_mode()) {
                cast_to_bool = std::make_shared<
                        CastToImpl<CastModeType::StrictMode, FromDataType, DataTypeBool>>();
            } else {
                cast_to_bool = std::make_shared<
                        CastToImpl<CastModeType::NonStrictMode, FromDataType, DataTypeBool>>();
            }
            return true;
        } else {
            return false;
        }
    };

    if (!call_on_index_and_data_type<void>(from_type->get_primitive_type(), make_bool_wrapper)) {
        return create_unsupport_wrapper(
                fmt::format("CAST AS bool not supported {}", from_type->get_name()));
    }

    return [cast_to_bool](FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                          uint32_t result, size_t input_rows_count,
                          const NullMap::value_type* null_map = nullptr) {
        return cast_to_bool->execute_impl(context, block, arguments, result, input_rows_count,
                                          null_map);
    };
}

}; // namespace CastWrapper

} // namespace doris::vectorized