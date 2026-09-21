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

#include "core/arena.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "exprs/aggregate/aggregate_function.h"
#include "exprs/function/function.h"
#include "util/defer_op.h"

namespace doris {

class FunctionAggStateFinalize : public IFunction {
public:
    FunctionAggStateFinalize(DataTypePtr return_type, AggregateFunctionPtr agg_function)
            : _return_type(std::move(return_type)), _agg_function(std::move(agg_function)) {}

    static FunctionBasePtr create(const DataTypes& argument_types, const DataTypePtr& return_type,
                                  const AggregateFunctionPtr& agg_function) {
        return std::make_shared<DefaultFunction>(
                std::make_shared<FunctionAggStateFinalize>(return_type, agg_function),
                argument_types, return_type);
    }

    String get_name() const override { return _agg_function->get_name() + "_finalize"; }

    size_t get_number_of_arguments() const override { return 1; }

    // An outer NULL can have an empty, invalid serialized payload. Do not deserialize it.
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return _return_type;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto input =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto* nullable = check_and_get_column<ColumnNullable>(*input);
        const auto& states = nullable ? nullable->get_nested_column() : *input;
        auto output = _agg_function->get_return_type()->create_column();
        _agg_function->check_result_column_type(*output);
        Arena arena;
        const auto state_size = _agg_function->size_of_data();
        const auto state_alignment = _agg_function->align_of_data();
        for (size_t row = 0; row < input_rows_count; ++row) {
            if (nullable && nullable->is_null_at(row)) {
                output->insert_default();
                continue;
            }
            {
                auto* place = arena.aligned_alloc(state_size, state_alignment);
                _agg_function->create(place);
                DEFER(_agg_function->destroy(place));
                // The serialized column can be numeric, fixed-length, string or a complex column.
                _agg_function->deserialize_and_merge_from_column_range(place, states, row, row,
                                                                       arena);
                _agg_function->insert_result_into(place, *output);
            }
            // States may own variable-length data. Destroy them before reclaiming their arena.
            arena.clear();
        }
        ColumnPtr result_column = std::move(output);
        if (_return_type->is_nullable()) {
            result_column = wrap_in_nullable(result_column, block, arguments, input_rows_count);
        }
        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

private:
    DataTypePtr _return_type;
    AggregateFunctionPtr _agg_function;
};

} // namespace doris
