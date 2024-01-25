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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arraySort.cpp
// and modified by Doris
#pragma once

#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>

#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

template <typename Name, bool Positive>
class FunctionArraySort : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionArraySort<Name, Positive>>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(is_array(arguments[0]))
                << "first argument for function: " << name << " should be DataTypeArray"
                << " and arguments[0] is " << arguments[0]->get_name();
        return arguments[0];
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        ColumnPtr src_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const auto& src_column_array = check_and_get_column<ColumnArray>(*src_column);
        if (!src_column_array) {
            return Status::RuntimeError(
                    fmt::format("unsupported types for function {}({})", get_name(),
                                block.get_by_position(arguments[0]).type->get_name()));
        }

        auto dest_column_ptr = _execute(*src_column_array);

        block.replace_by_position(result, std::move(dest_column_ptr));
        return Status::OK();
    }

private:
    ColumnPtr _execute(const ColumnArray& src_column_array) const {
        const auto& src_offsets = src_column_array.get_offsets();
        const auto& src_nested_column = src_column_array.get_data();

        size_t size = src_offsets.size();
        size_t nested_size = src_nested_column.size();
        IColumn::Permutation permutation(nested_size);
        for (size_t i = 0; i < nested_size; ++i) {
            permutation[i] = i;
        }

        for (size_t i = 0; i < size; ++i) {
            auto current_offset = src_offsets[i - 1];
            auto next_offset = src_offsets[i];

            std::sort(&permutation[current_offset], &permutation[next_offset],
                      Less(src_nested_column));
        }

        return ColumnArray::create(src_column_array.get_data().permute(permutation, 0),
                                   src_column_array.get_offsets_ptr());
    }

    struct Less {
        const IColumn& column;

        explicit Less(const IColumn& column_) : column(column_) {}

        bool operator()(size_t lhs, size_t rhs) const {
            if (Positive) {
                return column.compare_at(lhs, rhs, column, -1) < 0;
            } else {
                return column.compare_at(lhs, rhs, column, -1) > 0;
            }
        }
    };
};

} // namespace doris::vectorized
