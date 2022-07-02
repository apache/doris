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
#include "vec/columns/column_const.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

class FunctionArraySize : public IFunction {
public:
    static constexpr auto name = "size";
    static FunctionPtr create() { return std::make_shared<FunctionArraySize>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeArray";
        return std::make_shared<DataTypeInt64>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto array_column = check_and_get_column<ColumnArray>(*left_column);
        if (!array_column) {
            return Status::RuntimeError("unsupported types for function {}({})", get_name(),
                                        block.get_by_position(arguments[0]).type->get_name());
        }
        const auto& offsets = array_column->get_offsets();

        auto dst_column = ColumnInt64::create(input_rows_count);
        auto& dst_data = dst_column->get_data();

        for (ssize_t i = 0; i < offsets.size(); ++i) {
            dst_data[i] = offsets[i] - offsets[i - 1];
        }

        block.replace_by_position(result, std::move(dst_column));
        return Status::OK();
    }
};

} // namespace doris::vectorized
