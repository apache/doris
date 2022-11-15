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

#include <fmt/format.h>

#include "exprs/create_predicate_function.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

struct InState {
    bool use_set = true;

    // only use in null in set
    bool null_in_set = false;
    std::unique_ptr<HybridSetBase> hybrid_set;
};

template <bool negative>
class FunctionIn : public IFunction {
public:
    static constexpr auto name = negative ? "not_in" : "in";

    static FunctionPtr create() { return std::make_shared<FunctionIn>(); }

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

    Status prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        auto* state = new InState();
        context->set_function_state(scope, state);
        if (context->get_arg_type(0)->type == FunctionContext::Type::TYPE_CHAR ||
            context->get_arg_type(0)->type == FunctionContext::Type::TYPE_VARCHAR ||
            context->get_arg_type(0)->type == FunctionContext::Type::TYPE_STRING) {
            // the StringValue's memory is held by FunctionContext, so we can use StringValueSet here directly
            state->hybrid_set.reset(new StringValueSet());
        } else {
            state->hybrid_set.reset(
                    create_set(convert_type_to_primitive(context->get_arg_type(0)->type), true));
        }

        DCHECK(context->get_num_args() >= 1);
        for (int i = 1; i < context->get_num_args(); ++i) {
            const auto& const_column_ptr = context->get_constant_col(i);
            if (const_column_ptr != nullptr) {
                auto const_data = const_column_ptr->column_ptr->get_data_at(0);
                if (const_data.data == nullptr) {
                    state->null_in_set = true;
                } else {
                    state->hybrid_set->insert((void*)const_data.data, const_data.size);
                }
            } else {
                state->use_set = false;
                break;
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override {
        auto in_state = reinterpret_cast<InState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!in_state) {
            return Status::RuntimeError("funciton context for function '{}' must have Set;",
                                        get_name());
        }
        auto res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = res->get_data();
        vec_res.resize(input_rows_count);

        ColumnUInt8::MutablePtr col_null_map_to;
        col_null_map_to = ColumnUInt8::create(input_rows_count);
        auto& vec_null_map_to = col_null_map_to->get_data();

        /// First argument may be a single column.
        const ColumnWithTypeAndName& left_arg = block.get_by_position(arguments[0]);
        auto materialized_column = left_arg.column->convert_to_full_column_if_const();

        if (in_state->use_set) {
            if (materialized_column->is_nullable()) {
                auto* null_col_ptr = vectorized::check_and_get_column<vectorized::ColumnNullable>(
                        materialized_column);
                auto& null_bitmap = reinterpret_cast<const vectorized::ColumnUInt8&>(
                                            null_col_ptr->get_null_map_column())
                                            .get_data();
                auto* nested_col_ptr = null_col_ptr->get_nested_column_ptr().get();
                auto search_hash_set = [&](auto* col_ptr) {
                    for (size_t i = 0; i < input_rows_count; ++i) {
                        const auto& ref_data = col_ptr->get_data_at(i);
                        vec_res[i] =
                                !null_bitmap[i] &&
                                in_state->hybrid_set->find((void*)ref_data.data, ref_data.size);
                        if constexpr (negative) {
                            vec_res[i] = !vec_res[i];
                        }
                    }
                };

                if (nested_col_ptr->is_column_string()) {
                    const auto* column_string_ptr =
                            reinterpret_cast<const vectorized::ColumnString*>(nested_col_ptr);
                    search_hash_set(column_string_ptr);
                } else {
                    // todo support other column type
                    search_hash_set(nested_col_ptr);
                }

                if (!in_state->null_in_set) {
                    for (size_t i = 0; i < input_rows_count; ++i) {
                        vec_null_map_to[i] = null_bitmap[i];
                    }
                } else {
                    for (size_t i = 0; i < input_rows_count; ++i) {
                        vec_null_map_to[i] = null_bitmap[i] || (negative == vec_res[i]);
                    }
                }

            } else { // non-nullable

                auto search_hash_set = [&](auto* col_ptr) {
                    for (size_t i = 0; i < input_rows_count; ++i) {
                        const auto& ref_data = col_ptr->get_data_at(i);
                        vec_res[i] =
                                in_state->hybrid_set->find((void*)ref_data.data, ref_data.size);
                        if constexpr (negative) {
                            vec_res[i] = !vec_res[i];
                        }
                    }
                };

                if (materialized_column->is_column_string()) {
                    const auto* column_string_ptr =
                            reinterpret_cast<const vectorized::ColumnString*>(
                                    materialized_column.get());
                    search_hash_set(column_string_ptr);
                } else {
                    search_hash_set(materialized_column.get());
                }

                if (in_state->null_in_set) {
                    for (size_t i = 0; i < input_rows_count; ++i) {
                        vec_null_map_to[i] = negative == vec_res[i];
                    }
                } else {
                    for (size_t i = 0; i < input_rows_count; ++i) {
                        vec_null_map_to[i] = false;
                    }
                }
            }
        } else {
            std::vector<ColumnPtr> set_columns;
            for (int i = 1; i < arguments.size(); ++i) {
                set_columns.emplace_back(block.get_by_position(arguments[i]).column);
            }

            for (size_t i = 0; i < input_rows_count; ++i) {
                const auto& ref_data = materialized_column->get_data_at(i);
                if (ref_data.data == nullptr) {
                    vec_null_map_to[i] = true;
                    continue;
                }

                std::unique_ptr<HybridSetBase> hybrid_set(create_set(
                        convert_type_to_primitive(context->get_arg_type(0)->type), true));
                bool null_in_set = false;

                for (const auto& set_column : set_columns) {
                    auto set_data = set_column->get_data_at(i);
                    if (set_data.data == nullptr) {
                        null_in_set = true;
                    } else {
                        hybrid_set->insert((void*)(set_data.data), set_data.size);
                    }
                }
                vec_res[i] = negative ^ hybrid_set->find((void*)ref_data.data, ref_data.size);
                if (null_in_set) {
                    vec_null_map_to[i] = negative == vec_res[i];
                } else {
                    vec_null_map_to[i] = false;
                }
            }
        }

        if (block.get_by_position(result).type->is_nullable()) {
            block.replace_by_position(
                    result, ColumnNullable::create(std::move(res), std::move(col_null_map_to)));
        } else {
            block.replace_by_position(result, std::move(res));
        }

        return Status::OK();
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::FRAGMENT_LOCAL) {
            delete reinterpret_cast<InState*>(
                    context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        }
        return Status::OK();
    }
};

} // namespace doris::vectorized
