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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsConversion.h
// and modified by Doris

#pragma once

#include "vec/functions/function.h"
namespace doris::vectorized {

class FunctionToString : public IFunction {
public:
    static constexpr auto name = "to_string";

    String get_name() const final { return name; }

    bool is_variadic() const final { return true; }
    size_t get_number_of_arguments() const final { return 0; }

    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const final {
        return std::make_shared<DataTypeString>();
    }

    ColumnNumbers get_arguments_that_are_always_constant() const final { return {1}; }
    static FunctionPtr create() { return std::make_shared<FunctionToString>(); }
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& col_with_type_and_name = block.get_by_position(arguments[0]);
        const IDataType& type = *col_with_type_and_name.type;
        const IColumn& col_from = *col_with_type_and_name.column;

        auto col_to = ColumnString::create();
        type.to_string_batch(col_from, *col_to);

        block.replace_by_position(result, std::move(col_to));
        return Status::OK();
    }
};
} // namespace doris::vectorized