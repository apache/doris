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
#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionArrayFilter : public IFunction {
public:
    static constexpr auto name = "array_filter";
    static FunctionPtr create() { return std::make_shared<FunctionArrayFilter>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "First argument for function: " << name
                << " should be DataTypeArray but it has type " << arguments[0]->get_name() << ".";
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        //TODO: maybe need optimize not convert
        auto first_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto second_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();

        const ColumnArray& first_col_array = assert_cast<const ColumnArray&>(*first_column);
        const auto& first_off_data =
                assert_cast<const ColumnArray::ColumnOffsets&>(first_col_array.get_offsets_column())
                        .get_data();
        const auto& first_nested_nullable_column =
                assert_cast<const ColumnNullable&>(*first_col_array.get_data_ptr());

        const ColumnArray& second_col_array = assert_cast<const ColumnArray&>(*second_column);
        const auto& second_off_data = assert_cast<const ColumnArray::ColumnOffsets&>(
                                              second_col_array.get_offsets_column())
                                              .get_data();
        const auto& second_nested_null_map_data =
                assert_cast<const ColumnNullable&>(*second_col_array.get_data_ptr())
                        .get_null_map_column()
                        .get_data();
        const auto& second_nested_column =
                assert_cast<const ColumnNullable&>(*second_col_array.get_data_ptr())
                        .get_nested_column();
        const auto& second_nested_data =
                assert_cast<const ColumnUInt8&>(second_nested_column).get_data();

        auto result_data_column = first_nested_nullable_column.clone_empty();
        auto result_offset_column = ColumnArray::ColumnOffsets::create();
        auto& result_offset_data = result_offset_column->get_data();
        vectorized::IColumn::Selector selector;
        selector.reserve(first_off_data.size());
        result_offset_data.reserve(input_rows_count);

        for (size_t row = 0; row < input_rows_count; ++row) {
            unsigned long count = 0;
            auto first_offset_start = first_off_data[row - 1];
            auto first_offset_end = first_off_data[row];
            auto second_offset_start = second_off_data[row - 1];
            auto second_offset_end = second_off_data[row];
            auto move_off = second_offset_start;
            for (auto off = first_offset_start;
                 off < first_offset_end && move_off < second_offset_end; // not out range
                 ++off) {
                if (second_nested_null_map_data[move_off] == 0 && // not null
                    second_nested_data[move_off] == 1) {          // not 0
                    count++;
                    selector.push_back(off);
                }
                move_off++;
            }
            result_offset_data.push_back(count + result_offset_data.back());
        }
        first_nested_nullable_column.append_data_by_selector(result_data_column, selector);

        auto res_column =
                ColumnArray::create(std::move(result_data_column), std::move(result_offset_column));
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

void register_function_array_filter_function(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayFilter>();
}

} // namespace doris::vectorized
