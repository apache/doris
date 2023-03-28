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

#include "vec/columns/column_const.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

// construct a struct
// struct(value1, value2, value3, value4) -> {value1, value2, value3, value4}
class FunctionStruct : public IFunction {
public:
    static constexpr auto name = "struct";
    static FunctionPtr create() { return std::make_shared<FunctionStruct>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    bool use_default_implementation_for_nulls() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() > 0)
                << "function: " << get_name() << ", arguments should not be empty.";
        return std::make_shared<DataTypeStruct>(make_nullable(arguments));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto result_col = block.get_by_position(result).type->create_column();
        auto struct_column = typeid_cast<ColumnStruct*>(result_col.get());
        if (!struct_column) {
            return Status::RuntimeError("unsupported types for function {} return {}", get_name(),
                                        block.get_by_position(result).type->get_name());
        }
        size_t num_element = struct_column->tuple_size();
        DCHECK(arguments.size() == num_element)
                << "function: " << get_name()
                << ", argument number should equal to return field number.";
        // convert to nullable column
        for (size_t i = 0; i < num_element; ++i) {
            auto& col = block.get_by_position(arguments[i]).column;
            col = col->convert_to_full_column_if_const();
            auto& nested_col = struct_column->get_column(i);
            nested_col.reserve(input_rows_count);
            bool is_nullable = nested_col.is_nullable();
            // for now, column in struct is always nullable
            if (is_nullable && !col->is_nullable()) {
                col = ColumnNullable::create(col, ColumnUInt8::create(col->size(), 0));
            }
        }

        // insert value into struct
        for (size_t i = 0; i < num_element; ++i) {
            struct_column->get_column(i).insert_range_from(
                    *block.get_by_position(arguments[i]).column, 0, input_rows_count);
        }
        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }
};

void register_function_struct(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionStruct>();
}

} // namespace doris::vectorized
