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

#include <fmt/format.h>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/arena.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_agg_state.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

class FunctionAggState : public IFunction {
public:
    FunctionAggState(const DataTypes& argument_types, const DataTypePtr& return_type,
                     AggregateFunctionPtr agg_function)
            : _argument_types(argument_types),
              _return_type(return_type),
              _agg_function(agg_function) {}

    static FunctionBasePtr create(const DataTypes& argument_types, const DataTypePtr& return_type,
                                  AggregateFunctionPtr agg_function) {
        if (agg_function == nullptr) {
            return nullptr;
        }
        return std::make_shared<DefaultFunction>(
                std::make_shared<FunctionAggState>(argument_types, return_type, agg_function),
                argument_types, return_type);
    }

    size_t get_number_of_arguments() const override { return _argument_types.size(); }

    bool use_default_implementation_for_constants() const override { return true; }
    bool use_default_implementation_for_nulls() const override { return false; }

    String get_name() const override { return fmt::format("{}_state", _agg_function->get_name()); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return _return_type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto col = _agg_function->create_serialize_column();
        std::vector<const IColumn*> agg_columns;
        std::vector<ColumnPtr> save_columns;

        for (size_t i = 0; i < arguments.size(); i++) {
            DataTypePtr signature =
                    assert_cast<const DataTypeAggState*>(_return_type.get())->get_sub_types()[i];
            ColumnPtr column =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            save_columns.push_back(column);

            if (!signature->is_nullable() && column->is_nullable()) {
                return Status::InternalError(
                        "State function meet input nullable column, but signature is not nullable");
            }
            if (!column->is_nullable() && signature->is_nullable()) {
                column = make_nullable(column);
                save_columns.push_back(column);
            }

            agg_columns.push_back(column);
        }
        _agg_function->streaming_agg_serialize_to_column(agg_columns.data(), col, input_rows_count,
                                                         &(context->get_arena()));
        block.replace_by_position(result, std::move(col));
        return Status::OK();
    }

private:
    DataTypes _argument_types;
    DataTypePtr _return_type;
    AggregateFunctionPtr _agg_function;
};

} // namespace doris::vectorized
