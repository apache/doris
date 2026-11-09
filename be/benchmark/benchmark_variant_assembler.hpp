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

#include <algorithm>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <utility>

#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "storage/segment/variant/v2/variant_assembler.h"

namespace doris::variant_assembler_benchmark {

static void BM_VariantAssembler(benchmark::State& state, size_t slots, size_t visible) {
    constexpr size_t ROWS = 4096;
    auto type = make_nullable(std::make_shared<DataTypeInt64>());
    segment_v2::variant_v2::VariantAssemblerOptions options;
    DorisVector<ColumnNullable::MutablePtr> columns;
    DorisVector<const IColumn*> materialized;
    for (size_t slot = 0; slot < slots; ++slot) {
        options.materialized_paths.push_back(
                {.path = PathInData("k" + std::to_string(slot)), .type = type});
        columns.push_back(ColumnNullable::create(
                ColumnInt64::create(ROWS, 1), ColumnUInt8::create(ROWS, visible == slots ? 0 : 1)));
        materialized.push_back(columns.back().get());
    }
    if (visible != slots) {
        std::mt19937 random(20261031);
        DorisVector<size_t> slot_order(slots);
        std::iota(slot_order.begin(), slot_order.end(), 0);
        for (size_t row = 0; row < ROWS; ++row) {
            std::shuffle(slot_order.begin(), slot_order.end(), random);
            for (size_t index = 0; index < visible; ++index) {
                columns[slot_order[index]]->get_null_map_data()[row] = 0;
            }
        }
    }
    auto result = segment_v2::variant_v2::VariantAssembler::create(std::move(options));
    if (!result.has_value()) {
        state.SkipWithError(result.error().to_string().c_str());
        return;
    }
    auto assembler = std::move(result).value();
    segment_v2::variant_v2::VariantAssemblerBatchView batch;
    batch.num_rows = ROWS;
    batch.materialized_columns = materialized;
    for (auto _ : state) {
        ColumnNullable::MutablePtr output;
        const Status status = assembler->assemble(batch, &output);
        if (!status.ok()) {
            state.SkipWithError(status.to_string().c_str());
            return;
        }
        benchmark::DoNotOptimize(output);
    }
    state.SetItemsProcessed(state.iterations() * ROWS);
    state.counters["rows/s"] = benchmark::Counter(static_cast<double>(state.iterations()) * ROWS,
                                                  benchmark::Counter::kIsRate);
}

BENCHMARK_CAPTURE(BM_VariantAssembler, WideSparse, 2048, 20);
BENCHMARK_CAPTURE(BM_VariantAssembler, Dense, 200, 200);
BENCHMARK_CAPTURE(BM_VariantAssembler, Narrow, 10, 10);
BENCHMARK_CAPTURE(BM_VariantAssembler, MediumSparse, 200, 75);

} // namespace doris::variant_assembler_benchmark
