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
#include <string_view>

#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

// construct an array
// array(col1, col2, '22') -> [col1, col2, '22']
class FunctionArrayConstructor : public IFunction {
public:
    static constexpr auto name = "array";
    static FunctionPtr create() { return std::make_shared<FunctionArrayConstructor>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() > 0)
                << "function: " << get_name() << ", arguments should not be empty";
        return std::make_shared<DataTypeArray>(make_nullable(remove_nullable(arguments[0])));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        size_t num_element = arguments.size();
        auto result_col = block.get_by_position(result).type->create_column();
        auto result_array_col = static_cast<ColumnArray*>(result_col.get());
        IColumn& result_nested_col = result_array_col->get_data();
        ColumnArray::Offsets64& result_offset_col = result_array_col->get_offsets();
        result_nested_col.reserve(input_rows_count * num_element);
        result_offset_col.resize(input_rows_count);

        // convert to nullable column
        for (size_t i = 0; i < num_element; ++i) {
            auto& col = block.get_by_position(arguments[i]).column;
            col = col->convert_to_full_column_if_const();
            if (result_nested_col.is_nullable() && !col->is_nullable()) {
                col = ColumnNullable::create(col, ColumnUInt8::create(col->size(), 0));
            }
        }

        // insert value into array
        ColumnArray::Offset64 offset = 0;
        for (size_t row = 0; row < input_rows_count; ++row) {
            for (size_t idx = 0; idx < num_element; ++idx) {
                result_nested_col.insert_from(*block.get_by_position(arguments[idx]).column, row);
            }
            offset += num_element;
            result_offset_col[row] = offset;
        }
        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }
};

void register_function_array_constructor(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayConstructor>();
}

} // namespace doris::vectorized
