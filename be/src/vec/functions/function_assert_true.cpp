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

#include <memory>

#include "common/exception.h"
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

class FunctionAssertTrue : public IFunction {
public:
    static constexpr auto name = "assert_true";

    static FunctionPtr create() { return std::make_shared<FunctionAssertTrue>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (!context->is_col_constant(1)) [[unlikely]] {
            return Status::InvalidArgument("assert_true only accept constant for 2nd argument");
        }
        return Status::OK();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeBool>();
    }

    bool use_default_implementation_for_nulls() const final { return false; }
    ColumnNumbers get_arguments_that_are_always_constant() const final { return {1}; }

    // column2 is const, so in default logic column1 is no way const.
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        std::string errmsg =
                assert_cast<const ColumnConst&>(*block.get_by_position(arguments[1]).column)
                        .get_data_at(0)
                        .to_string();

        ColumnPtr col_ptr = block.get_by_position(arguments[0]).column;
        if (const auto* col_nullable = check_and_get_column<ColumnNullable>(col_ptr.get())) {
            if (col_nullable->has_null()) {
                throw doris::Exception(Status::InvalidArgument(errmsg));
            }
            col_ptr = col_nullable->get_nested_column_ptr();
        }
        const auto& data = assert_cast<const ColumnBool*>(col_ptr.get())->get_data();

        for (int i = 0; i < input_rows_count; i++) {
            if (!data[i]) [[unlikely]] {
                throw doris::Exception(Status::InvalidArgument(errmsg));
            }
        }

        block.replace_by_position(result, ColumnBool::create(input_rows_count, true));
        return Status::OK();
    }
};

void register_function_assert_true(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionAssertTrue>();
}
} // namespace doris::vectorized
