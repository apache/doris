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
        DataTypePtr arg = arguments[0];
        while (arg->get_primitive_type() == TYPE_ARRAY) {
            arg = remove_nullable(assert_cast<const DataTypeArray*>(arg.get())->get_nested_type());
        }
        return std::make_shared<DataTypeArray>(make_nullable(arg));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto* src_column_array_ptr =
                assert_cast<ColumnArray*>(remove_nullable(src_column)->assume_mutable().get());
        ColumnArray* nested_src_column_array_ptr = src_column_array_ptr;

        DataTypePtr src_data_type = block.get_by_position(arguments[0]).type;
        auto* src_data_type_array =
                assert_cast<const DataTypeArray*>(remove_nullable(src_data_type).get());

        auto result_column_offsets =
                assert_cast<ColumnArray::ColumnOffsets&>(src_column_array_ptr->get_offsets_column())
                        .clone();
        auto* offsets = assert_cast<ColumnArray::ColumnOffsets*>(result_column_offsets.get())
                                ->get_data()
                                .data();

        while (src_data_type_array->get_nested_type()->get_primitive_type() == TYPE_ARRAY) {
            nested_src_column_array_ptr = assert_cast<ColumnArray*>(
                    remove_nullable(src_column_array_ptr->get_data_ptr())->assume_mutable().get());

            for (size_t i = 0; i < input_rows_count; ++i) {
                offsets[i] = nested_src_column_array_ptr->get_offsets()[offsets[i] - 1];
            }
            src_column_array_ptr = nested_src_column_array_ptr;
            src_data_type_array = assert_cast<const DataTypeArray*>(
                    remove_nullable(src_data_type_array->get_nested_type()).get());
        }

        block.replace_by_position(
                result, ColumnArray::create(assert_cast<const ColumnNullable&>(
                                                    nested_src_column_array_ptr->get_data())
                                                    .clone(),
                                            std::move(result_column_offsets)));
        return Status::OK();
    }
};

void register_function_array_flatten(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayFlatten>();
}

} // namespace doris::vectorized
