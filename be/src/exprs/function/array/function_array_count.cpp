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

#include "core/column/column_array_view.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

// array_count([0, 1, 1, 1, 0, 0]) -> [3]
class FunctionArrayCount : public IFunction {
public:
    static constexpr auto name = "array_count";

    static FunctionPtr create() { return std::make_shared<FunctionArrayCount>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {1}; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto array_view =
                ColumnArrayView<TYPE_BOOLEAN>::create(block.get_by_position(arguments[0]).column);
        auto dst_column = ColumnInt64::create(array_view.size());
        auto& dst_data = dst_column->get_data();

        for (size_t row = 0; row < array_view.size(); ++row) {
            Int64 res = 0;
            if (array_view.is_null_at(row)) {
                dst_data[row] = res;
                continue;
            }
            auto array_data = array_view[row];
            const auto* data = array_data.get_data();
            const auto* null_map = array_data.get_null_map_data();
            for (size_t pos = 0; pos < array_data.size(); ++pos) {
                if (null_map[pos]) {
                    continue;
                }
                if (data[pos] != 0) {
                    ++res;
                }
            }
            dst_data[row] = res;
        }

        block.replace_by_position(result, std::move(dst_column));
        return Status::OK();
    }
};

void register_function_array_count(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayCount>();
}
} // namespace doris
