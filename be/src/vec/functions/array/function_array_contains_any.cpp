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
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

// array_contains_any([1, 2, 3, 10], [2,5]) -> true
class FunctionArrayContainsAny : public IFunction {
public:
    static constexpr auto name = "array_contains_any";
    static FunctionPtr create() { return std::make_shared<FunctionArrayContainsAny>(); }

    String get_name() const override { return name; }
    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() > 0)
                << "function: " << get_name() << ", arguments should not be empty";
        for (const auto& arg : arguments) {
            DCHECK(is_array(arg)) << "argument for function array_concat should be DataTypeArray"
                                  << " and argument is " << arg->get_name();
        }
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& argument,
                        size_t result, size_t input_rows_count) override {
        DataTypePtr column_type = block.get_by_position(argument[0]).type;

        return Status::OK();
    }
};

void register_function_array_contains_any(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayContainsAny>();
}

} // namespace doris::vectorized