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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayElement.cpp
// and modified by Doris
#pragma once

#include <string_view>

#include "vec/columns/column_array.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

class NullMapBuilder {
public:
    explicit operator bool() const { return src_null_map; }
    bool operator!() const { return !src_null_map; }

    void init_source(const UInt8* src_null_map_) {
        src_null_map = src_null_map_;
    }

    void init_sink(size_t size) {
        auto sink = ColumnUInt8::create(size);
        sink_null_map = sink->get_data().data();
        sink_null_map_holder = std::move(sink);
    }

    void update(size_t from) {
        sink_null_map[index] = bool(src_null_map && src_null_map[from]);
        ++index;
    }

    void update() {
        sink_null_map[index] = true;
        ++index;
    }

    ColumnPtr get_null_map_column_ptr() && { return std::move(sink_null_map_holder); }

private:
    const UInt8* src_null_map = nullptr;
    UInt8* sink_null_map = nullptr;
    MutableColumnPtr sink_null_map_holder;
    size_t index = 0;
};

class FunctionArrayElement : public IFunction
{
public:
    static constexpr auto name = "element_at";
    static FunctionPtr create() { return std::make_shared<FunctionArrayElement>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0])) << "first argument for function: " << name << " should be DataTypeArray";
        DCHECK(is_integer(arguments[1])) << "second argument for function: " << name << " should be Integer";
        return make_nullable(check_and_get_data_type<DataTypeArray>(arguments[0].get())->get_nested_type());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        NullMapBuilder builder;
        ColumnsWithTypeAndName args;
        auto col_left = block.get_by_position(arguments[0]);
        if (col_left.column->is_nullable()) {
            auto null_col = check_and_get_column<ColumnNullable>(*col_left.column);
            builder.init_source(null_col->get_null_map_column().get_data().data());
            args = {{null_col->get_nested_column_ptr(), remove_nullable(col_left.type), col_left.name},
                    block.get_by_position(arguments[1])};
        } else {
            args = {col_left,  block.get_by_position(arguments[1])};
        }
        builder.init_sink(input_rows_count);

        auto result_type = remove_nullable(check_and_get_data_type<DataTypeArray>(args[0].type.get())->get_nested_type());

        auto res_column = perform(args, result_type, builder, input_rows_count);
        if (!res_column) {
            return Status::RuntimeError(fmt::format("unsupported types for function {}({}, {})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name(),
                                        block.get_by_position(arguments[1]).type->get_name()));
        }
        block.replace_by_position(result, ColumnNullable::create(std::move(res_column), std::move(builder).get_null_map_column_ptr()));
        return Status::OK();
    }

    ColumnPtr perform(const ColumnsWithTypeAndName& arguments, const DataTypePtr& result_type, NullMapBuilder& builder, size_t input_rows_count) {
        ColumnPtr res;
        if (!((res = execute_number<Int8>(arguments, result_type, builder, input_rows_count))
             || (res = execute_number<Int16>(arguments, result_type, builder, input_rows_count))
             || (res = execute_number<Int32>(arguments, result_type, builder, input_rows_count))
             || (res = execute_number<Int64>(arguments, result_type, builder, input_rows_count))
             || (res = execute_number<Float32>(arguments, result_type, builder, input_rows_count))
             || (res = execute_number<Float64>(arguments, result_type, builder, input_rows_count))
             || (res = execute_string(arguments, result_type, builder, input_rows_count)))) {
            return nullptr;
        }
        return res;
    }

private:
    ColumnPtr execute_string(const ColumnsWithTypeAndName& arguments, const DataTypePtr & result_type, NullMapBuilder& builder, size_t input_rows_count) {
        // check array nested column type and get data
        auto array_column = check_and_get_column<ColumnArray>(*arguments[0].column);
        DCHECK(array_column != nullptr);
        auto nested_column = check_and_get_column<ColumnString>(array_column->get_data());
        if (!nested_column) {
            return nullptr;
        }
        const auto& offsets = array_column->get_offsets();
        const auto& src_str_offs = nested_column->get_offsets();
        const auto& src_str_chars = nested_column->get_chars();

        // prepare return data
        auto dst = ColumnString::create();
        auto& dst_str_offs = dst->get_offsets();
        dst_str_offs.resize(input_rows_count);
        auto& dst_str_chars = dst->get_chars();
        dst_str_chars.reserve(src_str_chars.size());

        // process
        for (size_t row = 0; row < input_rows_count; ++row) {
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;
            auto index = arguments[1].column->get_int(row);
            if (index > 0 && index <= len) {
                builder.update(row);
                index = off + index - 1;
                DCHECK(index >= 0);
            } else if (index < 0 && -index <= len) {
                builder.update(row);
                index = off + len + index;
                DCHECK(index >= 0);
            } else {
                builder.update();
                index = -1;
            }

            if (index >= 0) {
                auto element_size = src_str_offs[index] - src_str_offs[index - 1];
                dst_str_offs[row] = dst_str_offs[row - 1] + element_size;
                auto src_string_pos = src_str_offs[index - 1];
                auto dst_string_pos = dst_str_offs[row - 1];
                dst_str_chars.resize(dst_string_pos + element_size);
                memcpy(&dst_str_chars[dst_string_pos], &src_str_chars[src_string_pos], element_size);
            } else {
                dst_str_offs[row] = dst_str_offs[row - 1];
            }
        }

        VLOG_DEBUG << "function:" << get_name() << ", array type:" << arguments[0].type->get_name()
                   << ", index type:" << arguments[1].type->get_name() << ", result type:" << result_type->get_name();
        return dst;
    }

    template <typename ElementDataType>
    ColumnPtr execute_number(const ColumnsWithTypeAndName& arguments, const DataTypePtr & result_type, NullMapBuilder& builder, size_t input_rows_count) {
        // check array nested column type and get data
        auto array_column = check_and_get_column<ColumnArray>(*arguments[0].column);
        DCHECK(array_column != nullptr);
        auto nested_column = check_and_get_column<ColumnVector<ElementDataType>>(array_column->get_data());
        if (!nested_column) {
            return nullptr;
        }
        const auto& offsets = array_column->get_offsets();
        const auto& nested_data = nested_column->get_data();

        // prepare return data
        auto dst = ColumnVector<ElementDataType>::create(input_rows_count);
        auto& dst_data = dst->get_data();

        // process
        for (size_t row = 0; row < input_rows_count; ++row) {
            size_t off = offsets[row - 1];
            size_t len = offsets[row] - off;
            auto index = arguments[1].column->get_int(row);
            if (index > 0 && index <= len) {
                builder.update(row);
                dst_data[row] = nested_data[off + index - 1];
            } else if (index < 0 && -index <= len) {
                builder.update(row);
                dst_data[row] = nested_data[off + len + index];
            } else {
                builder.update();
                dst_data[row] = ElementDataType();
            }
        }

        VLOG_DEBUG << "function:" << get_name() << ", array type:" << arguments[0].type->get_name()
                   << ", index type:" << arguments[1].type->get_name() << ", result type:" << result_type->get_name();
        return dst;
    }
};

} // namespace doris::vectorized
