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

#include <stddef.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <memory>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
class FunctionIfNull : public IFunction {
public:
    static constexpr auto name = "ifnull";

    static FunctionPtr create() { return std::make_shared<FunctionIfNull>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 2; }

    // be compatible with fe code
    /* 
        if (fn.functionName().equalsIgnoreCase("ifnull") || fn.functionName().equalsIgnoreCase("nvl")) {
            Preconditions.checkState(children.size() == 2);
            if (children.get(0).isNullable()) {
                return children.get(1).isNullable();
            }
            return false;
        }
    */
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments[0]->is_nullable()) {
            return arguments[1];
        }
        return arguments[0];
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    // ifnull(col_left, col_right) == if(isnull(col_left), col_right, col_left)
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnWithTypeAndName& col_left = block.get_by_position(arguments[0]);
        if (col_left.column->only_null()) {
            block.get_by_position(result).column = block.get_by_position(arguments[1]).column;
            return Status::OK();
        }

        ColumnWithTypeAndName null_column_arg0 {nullptr, std::make_shared<DataTypeUInt8>(), ""};
        ColumnWithTypeAndName nested_column_arg0 {nullptr, col_left.type, ""};

        col_left.column = col_left.column->convert_to_full_column_if_const();

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

        // see get_return_type_impl
        // if result is nullable, means both then and else column are nullable, we use original col_left to keep nullable info
        // if result is not nullable, means both then and else column are not nullable, we use nested_column_arg0 to remove nullable info
        bool result_nullable = block.get_by_position(result).type->is_nullable();
        Block temporary_block({
                null_column_arg0,
                block.get_by_position(arguments[1]),
                result_nullable
                        ? col_left
                        : nested_column_arg0, // if result is nullable, we need pass the original col_left else pass nested_column_arg0
                block.get_by_position(result),
        });

        auto func_if = SimpleFunctionFactory::instance().get_function(
                "if", if_columns, block.get_by_position(result).type);
        RETURN_IF_ERROR(func_if->execute(context, temporary_block, {0, 1, 2}, 3, input_rows_count));
        block.get_by_position(result).column = temporary_block.get_by_position(3).column;
        return Status::OK();
    }
};
} // namespace doris::vectorized
