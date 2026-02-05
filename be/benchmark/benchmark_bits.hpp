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

#include "util/simd/bits.h"

namespace doris {} // namespace doris

static void BM_Bits_CountZeroNum(benchmark::State& state) {
    const auto n = static_cast<size_t>(state.range(0));
    std::vector<int8_t> data(n, 0);

    for (auto _ : state) {
        auto r = doris::simd::count_zero_num<size_t>(data.data(), data.size());
        benchmark::DoNotOptimize(r);
    }

    state.SetBytesProcessed(state.iterations() * n);
}

static void BM_Bits_CountZeroNumNullMap(benchmark::State& state) {
    const auto n = static_cast<size_t>(state.range(0));
    std::vector<int8_t> data(n, 0);
    std::vector<uint8_t> null_map(n, 0);

    for (auto _ : state) {
        auto r = doris::simd::count_zero_num<size_t>(data.data(), null_map.data(), data.size());
        benchmark::DoNotOptimize(r);
    }

    state.SetBytesProcessed(state.iterations() * n);
}

BENCHMARK(BM_Bits_CountZeroNum)
        ->Unit(benchmark::kNanosecond)
        ->Arg(16)   // 16 bytes
        ->Arg(32)   // 32 bytes
        ->Arg(64)   // 64 bytes
        ->Arg(256)  // 256 bytes
        ->Arg(1024) // 1KB
        ->Repetitions(5)
        ->DisplayAggregatesOnly();

BENCHMARK(BM_Bits_CountZeroNumNullMap)
        ->Unit(benchmark::kNanosecond)
        ->Arg(16)   // 16 bytes
        ->Arg(32)   // 32 bytes
        ->Arg(64)   // 64 bytes
        ->Arg(256)  // 256 bytes
        ->Arg(1024) // 1KB
        ->Repetitions(5)
        ->DisplayAggregatesOnly();
