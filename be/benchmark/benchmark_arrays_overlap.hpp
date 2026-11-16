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

#include <benchmark/benchmark.h>

#include <cstddef>
#include <memory>

#include "core/column/column_array.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"

namespace doris {

// Both variants execute the same constant-column input. Registering the constant
// with FunctionContext enables preparation; omitting it measures the per-row path.
// Time execute() only, including result allocation, but excluding input creation/open().
template <bool Prepare>
static void BM_ArraysOverlap(benchmark::State& state) {
    constexpr size_t rows = 4096;
    const size_t const_size = state.range(0);
    const size_t array_size = state.range(1);
    const bool match = state.range(2);
    const auto type =
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeInt32>()));
    const auto result_type = make_nullable(std::make_shared<DataTypeUInt8>());
    Block block;
    for (size_t arg = 0; arg < 2; ++arg) {
        const size_t count = arg == 0 ? rows : 1;
        const size_t length = arg == 0 ? array_size : const_size;
        auto values = ColumnInt32::create();
        auto offsets = ColumnArray::ColumnOffsets::create();
        for (size_t row = 0; row < count; ++row) {
            for (size_t i = 0; i < length; ++i) {
                if (arg == 1) {
                    values->insert_value(static_cast<Int32>(i));
                } else {
                    const auto value = match && i + 1 == length ? const_size - 1 : 1000 + row + i;
                    values->insert_value(static_cast<Int32>(value));
                }
            }
            offsets->insert_value(values->size());
        }
        auto nulls = ColumnUInt8::create(values->size(), 0);
        ColumnPtr column = ColumnArray::create(
                ColumnNullable::create(std::move(values), std::move(nulls)), std::move(offsets));
        if (arg == 1) {
            column = ColumnConst::create(std::move(column), rows);
        }
        block.insert({std::move(column), type, arg == 0 ? "array" : "constant"});
    }
    auto function = SimpleFunctionFactory::instance().get_function(
            "arrays_overlap", block.get_columns_with_type_and_name(), result_type);
    auto context = FunctionContext::create_context(nullptr, result_type, {type, type});
    if constexpr (Prepare) {
        context->set_constant_cols(
                {nullptr, std::make_shared<ConstantExprResult>(block.get_by_position(1).column)});
    }
    auto status =
            function->open(context.get(), FunctionContext::FunctionStateScope::FRAGMENT_LOCAL);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
        return;
    }
    block.insert({nullptr, result_type, "result"});
    for (auto _ : state) {
        status = function->execute(context.get(), block, {0, 1}, 2, rows);
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            break;
        }
        benchmark::DoNotOptimize(block.get_by_position(2).column);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * rows);
}

BENCHMARK_TEMPLATE(BM_ArraysOverlap, false)->ArgsProduct({{3, 64}, {8, 64}, {0, 1}});
BENCHMARK_TEMPLATE(BM_ArraysOverlap, true)->ArgsProduct({{3, 64}, {8, 64}, {0, 1}});

} // namespace doris
