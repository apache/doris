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

#include "vec/functions/array/function_array_reverse.h"
#include "vec/functions/function_string.h"

namespace doris::vectorized {

class FunctionReverseCommon : public IFunction {
public:
    static constexpr auto name = "reverse";

    static FunctionPtr create() { return std::make_shared<FunctionReverseCommon>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    bool get_is_injective(const Block&) override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_string_or_fixed_string(arguments[0]) || is_array(arguments[0]))
                << fmt::format("Illegal type {} used for argument of function {}",
                               arguments[0]->get_name(), get_name());

        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr& src_column = block.get_by_position(arguments[0]).column;
        if (const ColumnString* col_string = check_and_get_column<ColumnString>(src_column.get())) {
            auto col_res = ColumnString::create();
            static_cast<void>(ReverseImpl::vector(col_string->get_chars(),
                                                  col_string->get_offsets(), col_res->get_chars(),
                                                  col_res->get_offsets()));
            block.replace_by_position(result, std::move(col_res));
        } else if (check_column<ColumnArray>(src_column.get())) {
            return ArrayReverseImpl::_execute(block, arguments, result, input_rows_count);
        } else {
            return Status::RuntimeError("Illegal column {} used for argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
