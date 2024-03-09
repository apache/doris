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

#pragma once

#include <glog/logging.h>
#include <stddef.h>

#include <algorithm>
#include <memory>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_struct_in.h"

namespace doris {
namespace vectorized {
struct ColumnRowRef;
}
} // namespace doris

namespace doris::vectorized {

struct ArrayInState {
    ENABLE_FACTORY_CREATOR(ArrayInState)
    std::unordered_set<ColumnRowRef, ColumnRowRef> args_set;
    bool null_in_set = false;
};

template <bool negative>
class FunctionArrayIn : public IFunction {
public:
    static constexpr auto name = negative ? "array_not_in" : "array_in";

    static FunctionPtr create() { return std::make_shared<FunctionArrayIn>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    DataTypePtr get_return_type_impl(const DataTypes& args) const override {
        for (const auto& arg : args) {
            if (arg->is_nullable()) {
                return make_nullable(std::make_shared<DataTypeUInt8>());
            }
        }
        return std::make_shared<DataTypeUInt8>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    // make data in context into a set
    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }

        int num_args = context->get_num_args();
        DCHECK(num_args >= 1);

        const TypeDescriptor* arg_type_desc = context->get_arg_type(0);
        DataTypePtr data_type_ptr =
                DataTypeFactory::instance().create_data_type(*arg_type_desc, false);
        MutableColumnPtr args_column_ptr = remove_nullable(data_type_ptr)->create_column();

        std::shared_ptr<ArrayInState> array_in_state = std::make_shared<ArrayInState>();
        context->set_function_state(FunctionContext::FRAGMENT_LOCAL, array_in_state);

        // put const column to args_column
        for (int i = 1; i < num_args; i++) {
            ColumnPtrWrapper* const_col_ptr = context->get_constant_col(i);
            if (const_col_ptr == nullptr) {
                break;
            }
            const auto& [col, _] = unpack_if_const(const_col_ptr->column_ptr);
            if (col->is_nullable()) {
                auto* null_col = vectorized::check_and_get_column<vectorized::ColumnNullable>(col);
                if (!array_in_state->null_in_set && null_col->has_null()) {
                    array_in_state->null_in_set = true;
                } else {
                    args_column_ptr->insert_from(null_col->get_nested_column(), 0);
                }
            } else {
                args_column_ptr->insert_from(*col, 0);
            }
        }
        ColumnPtr column_ptr = std::move(args_column_ptr);
        int col_size = column_ptr->size();
        for (size_t i = 0; i < col_size; i++) {
            array_in_state->args_set.insert({column_ptr, i});
        }

        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto in_state = reinterpret_cast<ArrayInState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!in_state) {
            return Status::RuntimeError("function context for function '{}' must have Set;",
                                        get_name());
        }
        auto args_set = in_state->args_set;
        bool null_in_set = in_state->null_in_set;

        auto res_col = ColumnUInt8::create();
        res_col->resize(input_rows_count);

        auto null_map = ColumnUInt8::create(input_rows_count, false);

        const ColumnWithTypeAndName& left_arg = block.get_by_position(arguments[0]);
        const auto& [materialized_column, col_const] = unpack_if_const(left_arg.column);
        auto materialized_column_not_null = materialized_column;
        if (materialized_column_not_null->is_nullable()) {
            materialized_column_not_null = assert_cast<ColumnPtr>(
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(
                            materialized_column_not_null)
                            ->get_nested_column_ptr());
        }
        for (size_t i = 0; i < input_rows_count; i++) {
            bool find = args_set.find({materialized_column_not_null, i}) != args_set.end();

            if constexpr (negative) {
                res_col->get_data()[i] = !find;
            } else {
                res_col->get_data()[i] = find;
            }

            if (!find && null_in_set) {
                null_map->get_data()[i] = true;
            }
        }

        if (block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(res_col), std::move(null_map)));
        } else {
            block.replace_by_position(result, std::move(res_col));
        }

        return Status::OK();
    }
};

} // namespace doris::vectorized
