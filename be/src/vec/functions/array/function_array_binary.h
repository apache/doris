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

#include "vec/columns/column_array.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/array/function_array_utils.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

// Functions with arguments is two arrays of the same element type.
template <typename Impl, typename Name>
class FunctionArrayBinary : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayBinary>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return false; }
    size_t get_number_of_arguments() const override { return 2; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0])) << arguments[0]->get_name();
        DCHECK(is_array(arguments[1])) << arguments[1]->get_name();
        DCHECK(arguments[0]->equals(*arguments[1]))
                << "data type " << arguments[0]->get_name() << " not equal with "
                << arguments[1]->get_name();
        return Impl::get_return_type(arguments);
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        auto right_column =
                block.get_by_position(arguments[1]).column->convert_to_full_column_if_const();
        Status ret = Status::RuntimeError(
                fmt::format("execute failed, unsupported types for function {}({}, {})", get_name(),
                            block.get_by_position(arguments[0]).type->get_name(),
                            block.get_by_position(arguments[1]).type->get_name()));
        // extract array column
        ColumnArrayExecutionData left_data;
        ColumnArrayExecutionData right_data;
        ColumnPtr res_ptr = nullptr;
        if (extract_column_array_info(*left_column, left_data) &&
            extract_column_array_info(*right_column, right_data)) {
            ret = Impl::execute(res_ptr, left_data, right_data);
        }
        if (ret == Status::OK()) {
            block.replace_by_position(result, std::move(res_ptr));
        }
        return ret;
    }
};

} // namespace doris::vectorized
