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

#include <vec/columns/column_array.h>
#include <vec/columns/column_nullable.h>
#include <vec/columns/columns_number.h>
#include <vec/data_types/data_type_array.h>
#include <vec/data_types/data_type_number.h>
#include <vec/functions/function.h>
#include <vec/functions/function_helpers.h>
#include <vec/functions/simple_function_factory.h>

#include <vec/utils/util.hpp>

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
                        size_t result, size_t input_rows_count) override {
        // 1. get first array column
        const auto first_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnArray& first_col_array = assert_cast<const ColumnArray&>(*first_column);
        const auto& first_off_data = assert_cast<const ColumnArray::ColumnOffsets&>(
                first_col_array.get_offsets_column());
        auto first_col = first_col_array.get_data_ptr();
        const auto nested_null_map = ColumnUInt8::create(size, 0);

        if (first_col_array.get_data_ptr()->is_nullable()) {
            first_nested_nullable_column = assert_cast<const ColumnNullable&>(*first_col_array.get_data_ptr());
            first_col = first_nested_nullable_column.get_nested_column_ptr();
            VectorizedUtils::update_null_map(nested_null_map->get_data(), 
                        first_nested_nullable_map_data.get_null_map_data().get_data());
        }
        // 2. compute result
        size_t size = first_col->size();
        const auto& result_column = ColumnUInt8::create(size, 0);
        auto& result_column_data = result_column->get_data();
        const auto& result_offset_column = first_off_data.clone_resized(first_off_data.size());
        const auto& first_col_data = check_and_get_column<ColumnUInt8>(*first_col)->get_data();

        for (size_t row = 0; row < size; ++row) {
            if (first_col_data[row] != 0) {
                result_column_data[row] = 1;
            }
        }

        const auto nested_null_column = ColumnNullable::create(result_column->assume_mutable(),
                                                               nested_null_map->assume_mutable());
        const auto column_array = ColumnArray::create(nested_null_column->assume_mutable(),
                                                      result_offset_column->assume_mutable());
        block.replace_by_position(result, column_array->assume_mutable());
        return Status::OK();
    }
};

void register_function_array_exists(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayExists>();
}

} // namespace doris::vectorized
