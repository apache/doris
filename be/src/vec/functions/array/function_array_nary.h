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
        DCHECK(arguments.size() >= 2)
                << "function: " << get_name() << ", arguments should large equals than 2";
        CHECK(is_array(remove_nullable(arguments[0])))
                << "0-th element is " << arguments[0]->get_name() << ",not array type";
        auto nested_type = remove_nullable(
                assert_cast<const DataTypeArray&>(*(arguments[0])).get_nested_type());
        for (size_t i = 1; i < arguments.size(); ++i) {
            CHECK(is_array(remove_nullable(arguments[i])))
                    << i << "-th element is " << arguments[i]->get_name() << ", not array type";
            auto right_nested_type = remove_nullable(
                    assert_cast<const DataTypeArray&>(*(remove_nullable(arguments[i])))
                            .get_nested_type());
            // do check array nested data type, now we just support same nested data type
            CHECK(nested_type->equals(*right_nested_type))
                    << "data type " << arguments[i]->get_name() << " not equal with "
                    << arguments[0]->get_name();
        }
        DataTypePtr res_data_type = Impl::get_return_type(arguments);
        return res_data_type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr res_ptr;
        ColumnArrayExecutionDatas datas(arguments.size());
        std::vector<bool> col_const(arguments.size());
        for (int i = 0; i < arguments.size(); ++i) {
            const auto& [col, is_const] =
                    unpack_if_const(block.get_by_position(arguments[i]).column);
            col_const[i] = is_const;
            extract_column_array_info(*col, datas[i]);
        }
        if (Status st = Impl::execute(res_ptr, datas, col_const, 0, input_rows_count); !st.ok()) {
            return Status::RuntimeError(
                    fmt::format("function {} execute failed {} ", get_name(), st.to_string()));
        }
        block.replace_by_position(result, std::move(res_ptr));
        return Status::OK();
    }
};

} // namespace doris::vectorized
