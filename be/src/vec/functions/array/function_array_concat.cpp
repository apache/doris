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

#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// array_concat([1, 2], [7, 8], [5, 6]) -> [1, 2, 7, 8, 5, 6]
class FunctionArrayConcat : public IFunction {
public:
    static constexpr auto name = "array_concat";

    static FunctionPtr create() { return std::make_shared<FunctionArrayConcat>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() > 0)
                << "function: " << get_name() << ", arguments should not be empty";
        for (const auto& arg : arguments) {
            DCHECK(is_array(arg)) << "argument for function array_concat should be DataTypeArray"
                                  << " and argument is " << arg->get_name();
        }
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        const size_t result, size_t input_rows_count) const override {
        DataTypePtr column_type = block.get_by_position(arguments[0]).type;
        auto nested_type = assert_cast<const DataTypeArray&>(*column_type).get_nested_type();
        auto result_column = ColumnArray::create(nested_type->create_column(),
                                                 ColumnArray::ColumnOffsets::create());
        IColumn& result_nested_col = result_column->get_data();
        ColumnArray::Offsets64& column_offsets = result_column->get_offsets();
        column_offsets.resize(input_rows_count);

        size_t total_size = 0;
        for (size_t col : arguments) {
            ColumnPtr src_column =
                    block.get_by_position(col).column->convert_to_full_column_if_const();
            const auto& src_column_array = check_and_get_column<ColumnArray>(*src_column);
            total_size += src_column_array->get_data().size();
        }
        result_nested_col.reserve(total_size);

        size_t off = 0;
        for (size_t row = 0; row < input_rows_count; ++row) {
            for (size_t col : arguments) {
                ColumnPtr src_column =
                        block.get_by_position(col).column->convert_to_full_column_if_const();
                const auto& src_column_array = check_and_get_column<ColumnArray>(*src_column);
                const auto& src_column_offsets = src_column_array->get_offsets();
                const size_t length = src_column_offsets[row] - src_column_offsets[row - 1];
                result_nested_col.insert_range_from(src_column_array->get_data(),
                                                    src_column_offsets[row - 1], length);
                off += length;
            }
            column_offsets[row] = off;
        }

        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }
};

void register_function_array_concat(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayConcat>();
}

} // namespace doris::vectorized