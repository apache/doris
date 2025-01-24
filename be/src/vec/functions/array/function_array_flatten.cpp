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

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class FunctionArrayFlatten : public IFunction {
public:
    static constexpr auto name = "array_flatten";
    static FunctionPtr create() { return std::make_shared<FunctionArrayFlatten>(); }

    /// Get function name.
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr arg_0 = arguments[0];
        DCHECK(is_array(arg_0));
        return remove_nullable(assert_cast<const DataTypeArray*>(arg_0.get())->get_nested_type());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src_column_array =
                assert_cast<const ColumnArray&>(*remove_nullable(src_column));
        const auto& nested_src_column_array =
                assert_cast<const ColumnArray&>(*remove_nullable(src_column_array.get_data_ptr()));

        DataTypePtr src_column_type = block.get_by_position(arguments[0]).type;
        auto nested_type = assert_cast<const DataTypeArray&>(*src_column_type).get_nested_type();

        auto result_column_offsets = ColumnArray::ColumnOffsets::create(input_rows_count);
        auto* offsets = result_column_offsets->get_data().data();

        for (size_t i = 0; i < input_rows_count; ++i) {
            offsets[i] =
                    nested_src_column_array.get_offsets()[src_column_array.get_offsets()[i] - 1];
        }

        block.replace_by_position(result,
                                  ColumnArray::create(assert_cast<const ColumnNullable&>(
                                                              nested_src_column_array.get_data())
                                                              .clone(),
                                                      std::move(result_column_offsets)));
        return Status::OK();
    }
};

void register_function_array_flatten(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayFlatten>();
}

} // namespace doris::vectorized