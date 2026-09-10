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

#include <cstdint>
#include <memory>
#include <numeric>

#include "load/memtable/memtable.h"

namespace doris {

static void BM_DuplicateKeyRowIndexSharedPtr(benchmark::State& state) {
    const auto rows = static_cast<size_t>(state.range(0));
    for (auto _ : state) {
        DorisVector<std::shared_ptr<RowInBlock>> row_index;
        row_index.reserve(rows);
        for (size_t i = 0; i < rows; ++i) {
            row_index.emplace_back(std::make_shared<RowInBlock>(i));
        }
        benchmark::DoNotOptimize(row_index);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
    state.counters["estimated_bytes_per_row"] =
            benchmark::Counter(sizeof(std::shared_ptr<RowInBlock>) + sizeof(RowInBlock));
}

static void BM_DuplicateKeyRowIndexCompact(benchmark::State& state) {
    const auto rows = static_cast<size_t>(state.range(0));
    for (auto _ : state) {
        DorisVector<uint32_t> row_index;
        row_index.resize(rows);
        std::iota(row_index.begin(), row_index.end(), uint32_t {0});
        benchmark::DoNotOptimize(row_index);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows));
    state.counters["estimated_bytes_per_row"] = benchmark::Counter(sizeof(uint32_t));
}

BENCHMARK(BM_DuplicateKeyRowIndexSharedPtr)
        ->Arg(4096)
        ->Arg(65536)
        ->Arg(1048576)
        ->Unit(benchmark::kMicrosecond);
BENCHMARK(BM_DuplicateKeyRowIndexCompact)
        ->Arg(4096)
        ->Arg(65536)
        ->Arg(1048576)
        ->Unit(benchmark::kMicrosecond);

} // namespace doris
