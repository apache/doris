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
#include "vec/io/io_helper.h"

namespace doris::vectorized {

template <>
class CastToImpl<CastModeType::StrictMode, DataTypeString, DataTypeBool> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
                block.get_by_position(arguments[0]).column.get());
        DataTypeBool::ColumnType::MutablePtr col_to =
                DataTypeBool::ColumnType::create(input_rows_count);
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            const auto& from = col_from->get_data_at(i);
            ReadBuffer read_buffer((const unsigned char*)from.data, from.size);
            bool parsed = try_read_bool_text(col_to->get_element(i), read_buffer);

            if (!parsed || !read_buffer.eof()) {
                return Status::InvalidArgument(
                        "Illegal column {} of first argument of function cast",
                        col_from->get_name());
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        return Status::OK();
    }
};

template <>
class CastToImpl<CastModeType::NonStrictMode, DataTypeString, DataTypeBool> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto* col_from = check_and_get_column<DataTypeString::ColumnType>(
                block.get_by_position(arguments[0]).column.get());
        DataTypeBool::ColumnType::MutablePtr col_to =
                DataTypeBool::ColumnType::create(input_rows_count);
        ColumnUInt8::MutablePtr col_null_map = ColumnUInt8::create(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            const auto& from = col_from->get_data_at(i);
            ReadBuffer read_buffer((const unsigned char*)from.data, from.size);
            bool parsed = try_read_bool_text(col_to->get_element(i), read_buffer);
            col_null_map->get_data()[i] = !parsed || !read_buffer.eof();
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(col_to), std::move(col_null_map));
        return Status::OK();
    }
};

template <CastModeType AllMode, typename NumberOrDecimalType>
    requires(CastUtil::is_number<NumberOrDecimalType> || CastUtil::is_decimal<NumberOrDecimalType>)
class CastToImpl<AllMode, NumberOrDecimalType, DataTypeBool> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto* col_from = check_and_get_column<typename NumberOrDecimalType::ColumnType>(
                block.get_by_position(arguments[0]).column.get());
        DataTypeBool::ColumnType::MutablePtr col_to =
                DataTypeBool::ColumnType::create(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i) {
            if constexpr (CastUtil::is_decimal<NumberOrDecimalType>) {
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
        if constexpr (CastUtil::is_base_cast_from_type<FromDataType>) {
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
                          const uint32_t result, size_t input_rows_count) {
        return cast_to_bool->execute_impl(context, block, arguments, result, input_rows_count);
    };
}

}; // namespace CastWrapper

} // namespace doris::vectorized