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

#include "vec/columns/column_decimal.h"
#include "vec/columns/columns_number.h"
#include "vec/core/call_on_type_index.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

template <typename Impl>
class FunctionMathUnary : public IFunction {
public:
    using IFunction::execute;

    static constexpr auto name = Impl::name;
    static FunctionPtr create() { return std::make_shared<FunctionMathUnary>(); }

private:
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<typename Impl::Type>();
    }

    static void execute_in_iterations(const double* src_data, double* dst_data, size_t size) {
        for (size_t i = 0; i < size; i++) {
            Impl::execute(&src_data[i], &dst_data[i]);
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto* col =
                assert_cast<const ColumnFloat64*>(block.get_by_position(arguments[0]).column.get());

        const auto& src_data = col->get_data();
        const size_t size = src_data.size();

        auto dst = ColumnFloat64::create();
        auto& dst_data = dst->get_data();
        dst_data.resize(size);

        execute_in_iterations(col->get_data().data(), dst_data.data(), size);

        block.replace_by_position(result, std::move(dst));
        return Status::OK();
    }
};

template <typename Name, Float64(Function)(Float64)>
struct UnaryFunctionPlain {
    using Type = DataTypeFloat64;
    static constexpr auto name = Name::name;

    template <typename T, typename U>
    static void execute(const T* src, U* dst) {
        *dst = static_cast<Float64>(Function(*src));
    }
};

} // namespace doris::vectorized
