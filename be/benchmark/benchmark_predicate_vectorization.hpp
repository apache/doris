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

#include <cstdint>

#include "common/compiler_util.h"
#include "core/pod_array.h"

namespace doris {
namespace {

using ByteVector = PaddedPODArray<uint8_t>;

NO_INLINE void raw_or_current(ByteVector& combined, const ByteVector& child) {
    for (size_t row = 0; row < combined.size(); ++row) {
        combined[row] |= child[row];
    }
}

NO_INLINE void raw_or_optimized(ByteVector& combined, const ByteVector& child) {
    auto* combined_data = combined.data();
    const auto* child_data = child.data();
    const size_t rows = combined.size();
    for (size_t row = 0; row < rows; ++row) {
        combined_data[row] |= child_data[row];
    }
}

NO_INLINE void topn_current(ByteVector& values, const ByteVector& null_map, uint8_t nulls_first) {
    for (size_t row = 0; row < values.size(); ++row) {
        values[row] = null_map[row] ? nulls_first : values[row];
    }
}

NO_INLINE void topn_optimized(ByteVector& values, const ByteVector& null_map, uint8_t nulls_first) {
    auto* value_data = values.data();
    const auto* null_data = null_map.data();
    const size_t rows = values.size();
    for (size_t row = 0; row < rows; ++row) {
        value_data[row] = null_data[row] ? nulls_first : value_data[row];
    }
}

template <typename Func>
void run_kernel(benchmark::State& state, Func&& func) {
    const auto rows = static_cast<size_t>(state.range(0));
    ByteVector values(rows, 1);
    ByteVector null_map(rows, 0);
    for (size_t row = 0; row < rows; row += 17) {
        null_map[row] = 1;
    }
    for (auto _ : state) {
        func(values, null_map);
        auto* values_data = values.data();
        benchmark::DoNotOptimize(values_data);
        benchmark::ClobberMemory();
    }
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * rows));
}

void BM_PredicateRawMaskCurrent(benchmark::State& state) {
    run_kernel(state, [](ByteVector& values, const ByteVector& null_map) {
        raw_or_current(values, null_map);
    });
}

void BM_PredicateRawMaskOptimized(benchmark::State& state) {
    run_kernel(state, [](ByteVector& values, const ByteVector& null_map) {
        raw_or_optimized(values, null_map);
    });
}

void BM_PredicateTopNCurrent(benchmark::State& state) {
    run_kernel(state, [](ByteVector& values, const ByteVector& null_map) {
        topn_current(values, null_map, 1);
    });
}

void BM_PredicateTopNOptimized(benchmark::State& state) {
    run_kernel(state, [](ByteVector& values, const ByteVector& null_map) {
        topn_optimized(values, null_map, 1);
    });
}

#define REGISTER_PREDICATE_BENCHMARK(name) \
    BENCHMARK(name)                        \
            ->Unit(benchmark::kNanosecond) \
            ->Args({4096})                 \
            ->Args({8192})                 \
            ->Repetitions(5)               \
            ->DisplayAggregatesOnly()

REGISTER_PREDICATE_BENCHMARK(BM_PredicateRawMaskCurrent);
REGISTER_PREDICATE_BENCHMARK(BM_PredicateRawMaskOptimized);
REGISTER_PREDICATE_BENCHMARK(BM_PredicateTopNCurrent);
REGISTER_PREDICATE_BENCHMARK(BM_PredicateTopNOptimized);

#undef REGISTER_PREDICATE_BENCHMARK

} // namespace
} // namespace doris
