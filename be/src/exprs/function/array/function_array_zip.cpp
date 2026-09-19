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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayZip.cpp
// and modified by Doris

#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/types.h"
#include "exec/common/util.hpp"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

// Combines multiple arrays into a single array
// array_zip(['d', 'o', 'r', 'i', 's'], [1, 2, 3, 4, 5]) -> [('d', 1), ('o', 2), ('r', 3), ('i', 4), ('s', 5)]
class FunctionArrayZip : public IFunction {
public:
    static constexpr auto name = "array_zip";
    static FunctionPtr create() { return std::make_shared<FunctionArrayZip>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() > 0)
                << "function: " << get_name() << ", arguments should not be empty";

        if (std::ranges::any_of(arguments,
                                [](const auto& type) { return type->is_null_literal(); })) {
            return make_nullable(std::make_shared<DataTypeNothing>());
        }

        DataTypes res_data_types;
        size_t num_elements = arguments.size();
        bool result_is_nullable = false;
        for (size_t i = 0; i < num_elements; ++i) {
            const auto argument_type = remove_nullable(arguments[i]);
            result_is_nullable |= arguments[i]->is_nullable();
            const auto& array_type = assert_cast<const DataTypeArray&>(*argument_type);

            res_data_types.emplace_back(
                    make_nullable(remove_nullable(array_type.get_nested_type())));
        }

        auto res = std::make_shared<DataTypeArray>(
                make_nullable(std::make_shared<DataTypeStruct>(res_data_types)));
        return result_is_nullable ? make_nullable(res) : res;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        size_t num_element = arguments.size();

        ColumnUInt8::MutablePtr result_null_map;
        ColumnUInt8::Container* result_null_map_data = nullptr;
        if (block.get_by_position(result).type->is_nullable()) {
            result_null_map = ColumnUInt8::create(input_rows_count, 0);
            result_null_map_data = &result_null_map->get_data();
        }

        std::vector<const ColumnArray*> column_arrays(num_element);
        std::vector<bool> column_is_const(num_element, false);
        for (size_t i = 0; i < num_element; ++i) {
            const auto& input_column = block.get_by_position(arguments[i]).column;
            if (input_column->only_null()) {
                auto& result_column = block.get_by_position(result);
                result_column.column =
                        result_column.type->create_column_const(input_rows_count, Field());
                return Status::OK();
            }
            const auto& [unpacked_column, is_const] = unpack_if_const(input_column);
            column_is_const[i] = is_const;
            const IColumn* array_column = unpacked_column.get();
            if (const auto* nullable = check_and_get_column<ColumnNullable>(array_column)) {
                VectorizedUtils::update_null_map(*result_null_map_data,
                                                 nullable->get_null_map_data(), is_const);
                array_column = nullable->get_nested_column_ptr().get();
            }

            column_arrays[i] = &assert_cast<const ColumnArray&>(*array_column);
        }

        bool has_hidden_nested_data = false;
        if (result_null_map_data != nullptr) {
            for (size_t row = 0; row < input_rows_count && !has_hidden_nested_data; ++row) {
                if (!(*result_null_map_data)[row]) {
                    continue;
                }
                for (size_t i = 0; i < num_element; ++i) {
                    const size_t actual_row = index_check_const(row, column_is_const[i]);
                    const auto& offsets = column_arrays[i]->get_offsets();
                    if (offsets[actual_row] != offsets[actual_row - 1]) {
                        has_hidden_nested_data = true;
                        break;
                    }
                }
            }
        }

        Columns tuple_columns(num_element);
        ColumnPtr result_offsets;
        if (!has_hidden_nested_data) {
            Columns materialized_columns(num_element);
            // Const arrays must be expanded, but prefer offsets already owned by a non-const input.
            size_t offsets_source = 0;
            while (offsets_source < num_element && column_is_const[offsets_source]) {
                ++offsets_source;
            }
            if (offsets_source == num_element) {
                offsets_source = 0;
            }

            for (size_t i = 0; i < num_element; ++i) {
                if (!column_is_const[i]) {
                    continue;
                }

                auto column = block.get_by_position(arguments[i])
                                      .column->convert_to_full_column_if_const();
                if (const auto* nullable = check_and_get_column<ColumnNullable>(column.get())) {
                    column = nullable->get_nested_column_ptr();
                }
                materialized_columns[i] = std::move(column);
                column_arrays[i] = &assert_cast<const ColumnArray&>(*materialized_columns[i]);
            }

            for (size_t i = 1; i < num_element; ++i) {
                if (!column_arrays[i]->has_equal_offsets(*column_arrays[0])) {
                    return Status::RuntimeError(fmt::format(
                            "execute failed, function {}'s {}-th argument should have same "
                            "offsets with first argument",
                            get_name(), i + 1));
                }
            }

            for (size_t i = 0; i < num_element; ++i) {
                tuple_columns[i] = column_arrays[i]->get_data_ptr();
            }
            result_offsets = column_arrays[offsets_source]->get_offsets_ptr();
        } else {
            MutableColumns mutable_tuple_columns(num_element);
            for (size_t i = 0; i < num_element; ++i) {
                mutable_tuple_columns[i] = column_arrays[i]->get_data().clone_empty();
            }

            auto mutable_result_offsets = ColumnArray::ColumnOffsets::create();
            auto& result_offsets_data = mutable_result_offsets->get_data();
            result_offsets_data.reserve(input_rows_count);
            size_t result_offset = 0;
            for (size_t row = 0; row < input_rows_count; ++row) {
                if (result_null_map_data != nullptr && (*result_null_map_data)[row]) {
                    result_offsets_data.push_back(result_offset);
                    continue;
                }

                size_t row_size = 0;
                for (size_t i = 0; i < num_element; ++i) {
                    const size_t actual_row = index_check_const(row, column_is_const[i]);
                    const auto& offsets = column_arrays[i]->get_offsets();
                    const size_t row_begin = offsets[actual_row - 1];
                    const size_t current_size = offsets[actual_row] - row_begin;
                    if (i == 0) {
                        row_size = current_size;
                    } else if (current_size != row_size) {
                        return Status::RuntimeError(fmt::format(
                                "execute failed, function {}'s {}-th argument should have same "
                                "offsets with first argument",
                                get_name(), i + 1));
                    }
                    mutable_tuple_columns[i]->insert_range_from(column_arrays[i]->get_data(),
                                                                row_begin, row_size);
                }
                result_offset += row_size;
                result_offsets_data.push_back(result_offset);
            }

            for (size_t i = 0; i < num_element; ++i) {
                tuple_columns[i] = std::move(mutable_tuple_columns[i]);
            }
            result_offsets = std::move(mutable_result_offsets);
        }

        auto tuples = ColumnStruct::create(tuple_columns);
        const size_t tuple_size = tuples->size();
        auto nullable_tuples =
                ColumnNullable::create(std::move(tuples), ColumnUInt8::create(tuple_size, 0));
        ColumnPtr res_column =
                ColumnArray::create(std::move(nullable_tuples), std::move(result_offsets));
        if (block.get_by_position(result).type->is_nullable()) {
            res_column = ColumnNullable::create(std::move(res_column), std::move(result_null_map));
        }
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

void register_function_array_zip(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayZip>();
}

} // namespace doris
