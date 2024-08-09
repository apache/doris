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

#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "exprs/create_predicate_function.h"
#include "exprs/hybrid_set.h"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "udf/udf.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"

namespace doris::vectorized {

template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;

struct InState {
    bool use_set = true;
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

    // size of [ in ( 1 , 2  , 3 , null) ]  is 3
    size_t get_size_with_out_null(FunctionContext* context) {
        if ((context->get_num_args() - 1) > FIXED_CONTAINER_MAX_SIZE) {
            return context->get_num_args() - 1;
        }
        size_t sz = 0;
        for (int i = 1; i < context->get_num_args(); ++i) {
            const auto& const_column_ptr = context->get_constant_col(i);
            if (const_column_ptr != nullptr) {
                auto const_data = const_column_ptr->column_ptr->get_data_at(0);
                if (const_data.data != nullptr) {
                    sz++;
                }
            }
        }
        return sz;
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }
        std::shared_ptr<InState> state = std::make_shared<InState>();
        context->set_function_state(scope, state);
        DCHECK(context->get_num_args() >= 1);
        if (context->get_arg_type(0)->type == PrimitiveType::TYPE_NULL) {
            state->hybrid_set.reset(create_set(TYPE_BOOLEAN, 0));
        } else if (context->get_arg_type(0)->type == PrimitiveType::TYPE_CHAR ||
                   context->get_arg_type(0)->type == PrimitiveType::TYPE_VARCHAR ||
                   context->get_arg_type(0)->type == PrimitiveType::TYPE_STRING) {
            // the StringValue's memory is held by FunctionContext, so we can use StringValueSet here directly
            state->hybrid_set.reset(create_string_value_set((size_t)(context->get_num_args() - 1)));
        } else {
            state->hybrid_set.reset(
                    create_set(context->get_arg_type(0)->type, get_size_with_out_null(context)));
        }
        state->hybrid_set->set_null_aware(true);

        for (int i = 1; i < context->get_num_args(); ++i) {
            const auto& const_column_ptr = context->get_constant_col(i);
            if (const_column_ptr != nullptr) {
                auto const_data = const_column_ptr->column_ptr->get_data_at(0);
                state->hybrid_set->insert((void*)const_data.data, const_data.size);
            } else {
                state->use_set = false;
                state->hybrid_set.reset();
                break;
            }
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto* in_state = reinterpret_cast<InState*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (!in_state) {
            return Status::RuntimeError("funciton context for function '{}' must have Set;",
                                        get_name());
        }
        auto res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = res->get_data();
        vec_res.resize(input_rows_count);

        ColumnUInt8::MutablePtr col_null_map_to;
        col_null_map_to = ColumnUInt8::create(input_rows_count, false);
        auto& vec_null_map_to = col_null_map_to->get_data();

        const ColumnWithTypeAndName& left_arg = block.get_by_position(arguments[0]);
        const auto& [materialized_column, col_const] = unpack_if_const(left_arg.column);

        if (in_state->use_set) {
            if (materialized_column->is_nullable()) {
                const auto* null_col_ptr =
                        vectorized::check_and_get_column<vectorized::ColumnNullable>(
                                materialized_column);
                const auto& null_map = assert_cast<const vectorized::ColumnUInt8&>(
                                               null_col_ptr->get_null_map_column())
                                               .get_data();
                const auto* nested_col_ptr = null_col_ptr->get_nested_column_ptr().get();

                if (nested_col_ptr->is_column_string()) {
                    const auto* column_string_ptr =
                            assert_cast<const vectorized::ColumnString*>(nested_col_ptr);
                    search_hash_set_check_null(in_state, input_rows_count, vec_res, null_map,
                                               column_string_ptr);
                } else {
                    //TODO: support other column type
                    search_hash_set_check_null(in_state, input_rows_count, vec_res, null_map,
                                               nested_col_ptr);
                }

                if (!in_state->hybrid_set->contain_null()) {
                    for (size_t i = 0; i < input_rows_count; ++i) {
                        vec_null_map_to[i] = null_map[i];
                    }
                } else {
                    for (size_t i = 0; i < input_rows_count; ++i) {
                        vec_null_map_to[i] = null_map[i] || negative == vec_res[i];
                    }
                }

            } else { // non-nullable
                if (WhichDataType(left_arg.type).is_string()) {
                    const auto* column_string_ptr =
                            assert_cast<const vectorized::ColumnString*>(materialized_column.get());
                    search_hash_set(in_state, input_rows_count, vec_res, column_string_ptr);
                } else {
                    search_hash_set(in_state, input_rows_count, vec_res, materialized_column.get());
                }

                if (in_state->hybrid_set->contain_null()) {
                    for (size_t i = 0; i < input_rows_count; ++i) {
                        vec_null_map_to[i] = negative == vec_res[i];
                    }
                }
            }
        } else { //!in_state->use_set
            std::vector<ColumnPtr> set_columns;
            for (int i = 1; i < arguments.size(); ++i) {
                set_columns.emplace_back(block.get_by_position(arguments[i]).column);
            }
            if (col_const) {
                impl_without_set<true>(context, set_columns, input_rows_count, vec_res,
                                       vec_null_map_to, materialized_column);
            } else {
                impl_without_set<false>(context, set_columns, input_rows_count, vec_res,
                                        vec_null_map_to, materialized_column);
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
        return Status::OK();
    }

private:
    template <typename T>
    static void search_hash_set_check_null(InState* in_state, size_t input_rows_count,
                                           ColumnUInt8::Container& vec_res,
                                           const ColumnUInt8::Container& null_map, T* col_ptr) {
        if constexpr (!negative) {
            in_state->hybrid_set->find_batch_nullable(*col_ptr, input_rows_count, null_map,
                                                      vec_res);
        } else {
            in_state->hybrid_set->find_batch_nullable_negative(*col_ptr, input_rows_count, null_map,
                                                               vec_res);
        }
    }

    template <typename T>
    static void search_hash_set(InState* in_state, size_t input_rows_count,
                                ColumnUInt8::Container& vec_res, T* col_ptr) {
        if constexpr (!negative) {
            in_state->hybrid_set->find_batch(*col_ptr, input_rows_count, vec_res);
        } else {
            in_state->hybrid_set->find_batch_negative(*col_ptr, input_rows_count, vec_res);
        }
    }

    template <bool Const>
    static void impl_without_set(FunctionContext* context,
                                 const std::vector<ColumnPtr>& set_columns, size_t input_rows_count,
                                 ColumnUInt8::Container& vec_res,
                                 ColumnUInt8::Container& vec_null_map_to,
                                 const ColumnPtr& materialized_column) {
        for (size_t i = 0; i < input_rows_count; ++i) {
            const auto& ref_data = materialized_column->get_data_at(index_check_const(i, Const));
            if (ref_data.data == nullptr) {
                vec_null_map_to[i] = true;
                continue;
            }

            std::vector<StringRef> set_datas;
            // To comply with the SQL standard, IN() returns NULL not only if the expression on the left hand side is NULL,
            // but also if no match is found in the list and one of the expressions in the list is NULL.
            bool null_in_set = false;

            for (const auto& set_column : set_columns) {
                auto set_data = set_column->get_data_at(i);
                if (set_data.data == nullptr) {
                    null_in_set = true;
                } else {
                    set_datas.push_back(set_data);
                }
            }
            std::unique_ptr<HybridSetBase> hybrid_set(
                    create_set(context->get_arg_type(0)->type, set_datas.size()));
            for (auto& set_data : set_datas) {
                hybrid_set->insert((void*)(set_data.data), set_data.size);
            }

            vec_res[i] = negative ^ hybrid_set->find((void*)ref_data.data, ref_data.size);
            if (null_in_set) {
                vec_null_map_to[i] = negative == vec_res[i];
            } else {
                vec_null_map_to[i] = false;
            }
        }
    }
};

} // namespace doris::vectorized
