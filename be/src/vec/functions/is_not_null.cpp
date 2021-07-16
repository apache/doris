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

#include "vec/columns/column_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

/// Implements the function isNotNull which returns true if a value
/// is not null, false otherwise.
class FunctionIsNotNull : public IFunction {
public:
    static constexpr auto name = "is_not_null_pred";

    static FunctionPtr create() { return std::make_shared<FunctionIsNotNull>(); }

    std::string get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_nulls() const override { return false; }
    bool use_default_implementation_for_constants() const override { return true; }
    ColumnNumbers get_arguments_that_dont_imply_nullable_return_type(
            size_t /*number_of_arguments*/) const override {
        return {0};
    }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return std::make_shared<DataTypeUInt8>();
    }

    Status execute_impl(Block& block, const ColumnNumbers& arguments, size_t result,
                        size_t input_rows_count) override {
        const ColumnWithTypeAndName& elem = block.get_by_position(arguments[0]);
        if (auto* nullable = check_and_get_column<ColumnNullable>(*elem.column)) {
            /// Return the negated null map.
            auto res_column = ColumnUInt8::create(input_rows_count);
            const auto& src_data = nullable->get_null_map_data();
            auto& res_data = assert_cast<ColumnUInt8&>(*res_column).get_data();

            for (size_t i = 0; i < input_rows_count; ++i) {
                res_data[i] = !src_data[i];
            }

            block.replace_by_position(result, std::move(res_column));
        } else {
            /// Since no element is nullable, return a constant one.
            block.get_by_position(result).column =
                    DataTypeUInt8().create_column_const(elem.column->size(), 1u);
        }
        return Status::OK();
    }
};

void register_function_is_not_null(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionIsNotNull>();
}

} // namespace doris::vectorized