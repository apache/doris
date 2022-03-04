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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/Ifnull.h
// and modified by Doris

#pragma once

#include "vec/columns/column_nullable.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/function_string.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
class FunctionIfNull : public IFunction {
public:
    static constexpr auto name = "ifnull";

    static FunctionPtr create() { return std::make_shared<FunctionIfNull>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_constants() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (!arguments[0]->is_nullable() && arguments[1]->is_nullable()) {
            return reinterpret_cast<const DataTypeNullable*>(arguments[1].get())->get_nested_type();
        }
        return arguments[1];
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    // ifnull(col_left, col_right) == if(isnull(col_left), col_right, col_left)
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        const ColumnWithTypeAndName& col_left = block.get_by_position(arguments[0]);
        if (col_left.column->only_null()) {
            block.get_by_position(result).column = block.get_by_position(arguments[1]).column;
            return Status::OK();
        }

        ColumnWithTypeAndName null_column_arg0 {nullptr, std::make_shared<DataTypeUInt8>(), ""};
        ColumnWithTypeAndName nested_column_arg0 {nullptr, col_left.type, ""};

        /// implement isnull(col_left) logic
        if (auto* nullable = check_and_get_column<ColumnNullable>(*col_left.column)) {
            null_column_arg0.column = nullable->get_null_map_column_ptr();
            nested_column_arg0.column = nullable->get_nested_column_ptr();
            nested_column_arg0.type =
                    reinterpret_cast<const DataTypeNullable*>(nested_column_arg0.type.get())
                            ->get_nested_type();
        } else {
            block.get_by_position(result).column = col_left.column;
            return Status::OK();
        }
        const ColumnsWithTypeAndName if_columns {
                null_column_arg0, block.get_by_position(arguments[1]), nested_column_arg0};

        Block temporary_block({
                null_column_arg0,
                block.get_by_position(arguments[1]),
                nested_column_arg0,
                block.get_by_position(result),
        });

        auto func_if = SimpleFunctionFactory::instance().get_function(
                "if", if_columns, block.get_by_position(result).type);
        func_if->execute(context, temporary_block, {0, 1, 2}, 3, input_rows_count);
        block.get_by_position(result).column = temporary_block.get_by_position(3).column;
        return Status::OK();
    }
};
} // namespace doris::vectorized
