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

namespace doris::vectorized {
struct ColumnRowRef {
    ColumnPtr column;
    size_t row_idx;

    // equals when call set insert, this operator will be used
    bool operator==(const ColumnRowRef& other) const {
        return column->compare_at(row_idx, other.row_idx, *column, 0) == 0;
    }
    // compare
    bool operator<(const ColumnRowRef& other) const {
        return column->compare_at(row_idx, other.row_idx, *column, 0) < 0;
    }

    // when call set find, will use hash to find
    size_t operator()(const ColumnRowRef& a) const {
        uint32_t hash_val = 0;
        a.column->update_crc_with_value(a.row_idx, a.row_idx + 1, hash_val, nullptr);
        return hash_val;
    }
};

template <bool negative>
class FunctionStructIn : public IFunction {
public:
    static constexpr auto name = negative ? "struct_not_in" : "struct_in";

    static FunctionPtr create() { return std::make_shared<FunctionStructIn>(); }

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
        DCHECK(context->get_num_args() >= 1);
        auto* col_desc = context->get_arg_type(0);
        DataTypePtr args_type = DataTypeFactory::instance().create_data_type(*col_desc);
        MutableColumnPtr column_struct_ptr_args = remove_nullable(args_type)->create_column();
        NullMap null_map(context->get_num_args(), false);
        for (int i = 1; i < context->get_num_args(); ++i) {
            // FE should make element type consistent and
            // equalize the length of the elements in struct
            const auto& const_column_ptr = context->get_constant_col(i);
            if (const_column_ptr == nullptr) {
                break;
            }
            const auto& [col, _] = unpack_if_const(const_column_ptr->column_ptr);
            if (col->is_nullable()) {
                auto* null_col = vectorized::check_and_get_column<vectorized::ColumnNullable>(col);
                if (null_col->has_null()) {
                    null_in_set = true;
                    null_map[i - 1] = true;
                } else {
                    column_struct_ptr_args->insert_from(null_col->get_nested_column(), 0);
                }
            } else {
                column_struct_ptr_args->insert_from(*col, 0);
            }
        }
        ColumnPtr column_ptr = std::move(column_struct_ptr_args);
        // make StructRef into set
        for (size_t i = 1; i < context->get_num_args(); ++i) {
            if (null_in_set && null_map[i - 1]) {
                continue;
            }
            args_set.insert({column_ptr, i - 1});
        }
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto res = ColumnUInt8::create();
        ColumnUInt8::Container& vec_res = res->get_data();
        vec_res.resize(input_rows_count);

        ColumnUInt8::MutablePtr col_null_map_to;
        col_null_map_to = ColumnUInt8::create(input_rows_count, false);
        auto& vec_null_map_to = col_null_map_to->get_data();

        const ColumnWithTypeAndName& left_arg = block.get_by_position(arguments[0]);
        const auto& [materialized_column, col_const] = unpack_if_const(left_arg.column);

        for (size_t i = 0; i < input_rows_count; ++i) {
            ColumnRowRef ref({materialized_column, i});
            bool find = args_set.find({materialized_column, i}) != args_set.end();
            if constexpr (negative) {
                vec_res[i] = !find;
            } else {
                vec_res[i] = find;
            }
            if (null_in_set) {
                vec_null_map_to[i] = negative == vec_res[i];
            } else {
                vec_null_map_to[i] = false;
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

private:
    std::unordered_set<ColumnRowRef, ColumnRowRef> args_set;
    bool null_in_set = false;
};

} // namespace doris::vectorized
