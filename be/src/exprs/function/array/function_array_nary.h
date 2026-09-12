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

#include "common/exception.h"
#include "core/column/column_array.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/array/function_array_utils.h"
#include "exprs/function/function.h"
#include "exprs/function/function_helpers.h"

namespace doris {

// Functions with more than two arrays of the same element type.
template <typename Impl, typename Name>
class FunctionArrayNary : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayNary>(); }
    String get_name() const override { return name; }
    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments.size() < 2) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Function {} requires at least two arguments", get_name());
        }
        const auto* left_array_type =
                check_and_get_data_type<DataTypeArray>(remove_nullable(arguments[0]).get());
        if (!left_array_type) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                   "Argument 0 for function {} must be an array, but got {}",
                                   get_name(), arguments[0]->get_name());
        }
        auto nested_type = remove_nullable(left_array_type->get_nested_type());
        for (size_t i = 1; i < arguments.size(); ++i) {
            const auto* right_array_type =
                    check_and_get_data_type<DataTypeArray>(remove_nullable(arguments[i]).get());
            if (!right_array_type) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       "Argument {} for function {} must be an array, but got {}",
                                       i, get_name(), arguments[i]->get_name());
            }
            auto right_nested_type = remove_nullable(right_array_type->get_nested_type());
            // do check array nested data type, now we just support same nested data type
            if (!nested_type->equals_ignore_precision(*right_nested_type)) {
                throw doris::Exception(
                        ErrorCode::INVALID_ARGUMENT,
                        "Function {} requires identical array element types, but got {} and {}",
                        get_name(), arguments[0]->get_name(), arguments[i]->get_name());
            }
        }
        DataTypePtr res_data_type = Impl::get_return_type(arguments);
        return res_data_type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        ColumnPtr res_ptr;
        ColumnArrayExecutionDatas datas(arguments.size());
        std::vector<bool> col_const(arguments.size());
        for (int i = 0; i < arguments.size(); ++i) {
            const auto& [col, is_const] =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
            col_const[i] = is_const;
            if (!extract_column_array_info(*col, datas[i])) {
                return Status::InvalidArgument(
                        "Argument {} for function {} must be an array column, but got {}", i,
                        get_name(), col->get_name());
            }
        }
        if (Status st = Impl::execute(res_ptr, datas, col_const, 0, input_rows_count); !st.ok()) {
            st.prepend(fmt::format("function {} execute failed: ", get_name()));
            return st;
        }
        block.replace_by_position(result, std::move(res_ptr));
        return Status::OK();
    }
};

} // namespace doris
