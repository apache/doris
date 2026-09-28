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

#include <benchmark/benchmark.h>

#include <memory>

#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"

namespace doris {

static void BM_ArrayRange(benchmark::State& state) {
    const size_t rows = state.range(0);
    const auto length = static_cast<Int32>(state.range(1));
    const auto step = static_cast<Int32>(state.range(2));
    auto int_type = std::make_shared<DataTypeInt32>();
    auto result_type = make_nullable(std::make_shared<DataTypeArray>(make_nullable(int_type)));
    Block block;
    auto starts = ColumnInt32::create();
    auto ends = ColumnInt32::create();
    auto steps = ColumnInt32::create();
    for (size_t row = 0; row < rows; ++row) {
        const auto start = static_cast<Int32>(row % 17);
        starts->insert_value(start);
        ends->insert_value(start + length * step);
        steps->insert_value(step);
    }
    block.insert({std::move(starts), int_type, "start"});
    block.insert({std::move(ends), int_type, "end"});
    block.insert({std::move(steps), int_type, "step"});
    auto function = SimpleFunctionFactory::instance().get_function(
            "array_range", block.get_columns_with_type_and_name(), result_type);
    auto context =
            FunctionContext::create_context(nullptr, result_type, {int_type, int_type, int_type});
    auto status = function->open(context.get(), FunctionContext::FRAGMENT_LOCAL);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
        return;
    }
    status = function->open(context.get(), FunctionContext::THREAD_LOCAL);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
        return;
    }
    block.insert({nullptr, result_type, "result"});
    for (auto _ : state) {
        status = function->execute(context.get(), block, {0, 1, 2}, 3, rows);
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            break;
        }
        benchmark::DoNotOptimize(block.get_by_position(3).column);
        benchmark::ClobberMemory();
    }
    status = function->close(context.get(), FunctionContext::THREAD_LOCAL);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
    }
    status = function->close(context.get(), FunctionContext::FRAGMENT_LOCAL);
    if (!status.ok()) {
        state.SkipWithError(status.to_string().c_str());
    }
    state.SetItemsProcessed(state.iterations() * rows * length);
}

BENCHMARK(BM_ArrayRange)
        ->Args({4096, 0, 1})
        ->Args({4096, 1, 1})
        ->Args({4096, 16, 1})
        ->Args({4096, 256, 1})
        ->Args({4096, 1024, 1})
        ->Args({4096, 256, 7})
        ->Args({1, 1000000, 1});

} // namespace doris
