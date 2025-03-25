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

#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionIgnore : public IFunction {
public:
    static constexpr auto name = "ignore";
    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionIgnore>(); }
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }
    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnPtr col = ColumnBool::create(1, false);
        block.replace_by_position(result, ColumnConst::create(col, input_rows_count));
        return Status::OK();
    }
};

void register_function_ignore(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionIgnore>();
}
} // namespace doris::vectorized
