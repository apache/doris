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

#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/types.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {

class FunctionArrayFlatten : public IFunction {
public:
    static constexpr auto name = "array_flatten";
    static FunctionPtr create() { return std::make_shared<FunctionArrayFlatten>(); }

    /// Get function name.
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr arg = remove_nullable(arguments[0]);
        const auto* array_type = check_and_get_data_type<DataTypeArray>(arg.get());
        if (!array_type) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Argument for function {} must be an array, but got {}",
                                   get_name(), arguments[0]->get_name());
        }
        do {
            arg = remove_nullable(array_type->get_nested_type());
            array_type = check_and_get_data_type<DataTypeArray>(arg.get());
        } while (array_type);
        return std::make_shared<DataTypeArray>(make_nullable(arg));
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto* src_column_array_ptr =
                check_and_get_column<ColumnArray>(remove_nullable(src_column).get());
        if (!src_column_array_ptr) {
            return Status::InvalidArgument("Argument for function {} must be an array, but got {}",
                                           get_name(), src_column->get_name());
        }
        const ColumnArray* nested_src_column_array_ptr = src_column_array_ptr;

        DataTypePtr src_data_type = remove_nullable(block.get_by_position(arguments[0]).type);
        const auto* src_data_type_array =
                check_and_get_data_type<DataTypeArray>(src_data_type.get());
        if (!src_data_type_array) {
            return Status::InvalidArgument(
                    "Argument type for function {} must be an array, but got {}", get_name(),
                    src_data_type->get_name());
        }

        auto result_column_offsets = src_column_array_ptr->get_offsets_column().clone();
        auto* offsets = assert_cast<ColumnArray::ColumnOffsets*>(result_column_offsets.get())
                                ->get_data()
                                .data();

        while (true) {
            const auto* nested_data_type_array = check_and_get_data_type<DataTypeArray>(
                    remove_nullable(src_data_type_array->get_nested_type()).get());
            if (!nested_data_type_array) {
                break;
            }
            nested_src_column_array_ptr = check_and_get_column<ColumnArray>(
                    remove_nullable(src_column_array_ptr->get_data_ptr()).get());
            if (!nested_src_column_array_ptr) {
                return Status::InvalidArgument(
                        "Nested argument column for function {} must be an array, but got {}",
                        get_name(), src_column_array_ptr->get_data().get_name());
            }

            for (size_t i = 0; i < input_rows_count; ++i) {
                if (offsets[i] != 0) {
                    offsets[i] = nested_src_column_array_ptr->get_offsets()[offsets[i] - 1];
                }
            }
            src_column_array_ptr = nested_src_column_array_ptr;
            src_data_type_array = nested_data_type_array;
        }

        const auto* nested_nullable_column =
                check_and_get_column<ColumnNullable>(&nested_src_column_array_ptr->get_data());
        if (!nested_nullable_column) {
            return Status::InvalidArgument(
                    "Nested argument column for function {} must be nullable, but got {}",
                    get_name(), nested_src_column_array_ptr->get_data().get_name());
        }

        block.replace_by_position(result, ColumnArray::create(nested_nullable_column->clone(),
                                                              std::move(result_column_offsets)));
        return Status::OK();
    }
};

void register_function_array_flatten(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayFlatten>();
}

} // namespace doris
