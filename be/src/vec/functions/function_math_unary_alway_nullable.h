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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionMathUnary.h
// and modified by Doris

#pragma once

#include "vec/columns/column.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/core/call_on_type_index.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {

template <typename Impl>
class FunctionMathUnaryAlwayNullable : public IFunction {
public:
    using IFunction::execute;

    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionMathUnaryAlwayNullable>(); }

private:
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<typename Impl::Type>());
    }

    static void execute_in_iterations(const double* src_data, double* dst_data, size_t size) {
        for (size_t i = 0; i < size; i++) {
            Impl::execute(&src_data[i], &dst_data[i]);
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const ColumnFloat64* col = nullptr;

        if (is_column_nullable(*block.get_by_position(arguments[0]).column)) {
            const auto* col_null = static_cast<const ColumnNullable*>(
                    block.get_by_position(arguments[0]).column.get());
            col = assert_cast<const ColumnFloat64*>(col_null->get_nested_column_ptr().get());
        } else {
            col = assert_cast<const ColumnFloat64*>(
                    block.get_by_position(arguments[0]).column.get());
        }

        const auto& src_data = col->get_data();
        const size_t size = src_data.size();

        auto dst = ColumnFloat64::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(size);

        execute_in_iterations(col->get_data().data(), dst_data.data(), size);

        auto result_null_map = ColumnUInt8::create(size, 0);

        for (size_t i = 0; i < size; i++) {
            if (Impl::is_invalid_input(col->get_data()[i])) {
                result_null_map->get_data().data()[i] = 1;
            }
        }

        if (is_column_nullable(*block.get_by_position(arguments[0]).column)) {
            const auto* col_null = static_cast<const ColumnNullable*>(
                    block.get_by_position(arguments[0]).column.get());
            vectorized::VectorizedUtils::update_null_map(result_null_map->get_data(),
                                                         col_null->get_null_map_data());
        }

        block.replace_by_position(
                result, ColumnNullable::create(std::move(dst), std::move(result_null_map)));
        return Status::OK();
    }
};

} // namespace doris::vectorized
