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
#pragma once

#include <string_view>

#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

class FunctionArraySlice : public IFunction {
public:
    static constexpr auto name = "array_slice";
    static FunctionPtr create() { return std::make_shared<FunctionArraySlice>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "First argument for function: " << name
                << " should be DataTypeArray but it has type " << arguments[0]->get_name() << ".";
        DCHECK(is_integer(arguments[1]))
                << "Second argument for function: " << name << " should be Integer but it has type "
                << arguments[1]->get_name() << ".";
        if (arguments.size() > 2) {
            DCHECK(is_integer(arguments[2]))
                    << "Third argument for function: " << name
                    << " should be Integer but it has type " << arguments[2]->get_name() << ".";
        }
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto array_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto offset_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        ColumnPtr length_column = nullptr;
        if (arguments.size() > 2) {
            length_column =
                    block.get_by_position(arguments[2]).column->convert_to_full_column_if_const();
        }
        // extract src array column
        ColumnArrayExecutionData src;
        if (!extract_column_array_info(*array_column, src)) {
            return Status::RuntimeError(
                    fmt::format("execute failed, unsupported types for function {}({}, {})",
                                get_name(), block.get_by_position(arguments[0]).type->get_name(),
                                block.get_by_position(arguments[1]).type->get_name()));
        }
        // prepare dst array column
        bool is_nullable = src.nested_nullmap_data ? true : false;
        ColumnArrayMutableData dst = create_mutable_data(src.nested_col, is_nullable);
        dst.offsets_ptr->reserve(input_rows_count);
        // execute
        slice_array(dst, src, *offset_column, length_column.get());
        ColumnPtr res_column = assemble_column_array(dst);
        block.replace_by_position(result, std::move(res_column));
        return Status::OK();
    }
};

} // namespace doris::vectorized
