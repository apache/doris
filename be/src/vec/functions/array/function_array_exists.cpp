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
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// array_exists([1, 2, 3, 0]) -> [1, 1, 1, 0]
class FunctionArrayExists : public IFunction {
public:
    static constexpr auto name = "array_exists";

    static FunctionPtr create() { return std::make_shared<FunctionArrayExists>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        // 1. get first array column
        const auto first_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnArray& first_col_array = assert_cast<const ColumnArray&>(*first_column);
        const auto& first_off_data = assert_cast<const ColumnArray::ColumnOffsets&>(
                first_col_array.get_offsets_column());

        const auto& nested_nullable_column =
                assert_cast<const ColumnNullable&>(*first_col_array.get_data_ptr());
        const auto nested_column = nested_nullable_column.get_nested_column_ptr();
        const size_t nested_column_size = nested_column->size();
        MutableColumnPtr result_null_map =
                nested_nullable_column.get_null_map_column_ptr()->clone_resized(nested_column_size);

        // 2. compute result
        MutableColumnPtr result_column = ColumnUInt8::create(nested_column_size, 0);
        auto* __restrict result_column_data =
                assert_cast<ColumnUInt8&>(*result_column).get_data().data();
        MutableColumnPtr result_offset_column = first_off_data.clone_resized(first_off_data.size());
        const auto* __restrict nested_column_data =
                assert_cast<const ColumnUInt8&>(*nested_column).get_data().data();

        for (size_t row = 0; row < nested_column_size; ++row) {
            result_column_data[row] = nested_column_data[row] != 0;
        }

        ColumnPtr result_nullalble_column =
                ColumnNullable::create(std::move(result_column), std::move(result_null_map));
        ColumnPtr column_array =
                ColumnArray::create(result_nullalble_column, std::move(result_offset_column));
        block.replace_by_position(result, column_array);
        return Status::OK();
    }
};

void register_function_array_exists(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayExists>();
}

} // namespace doris::vectorized
