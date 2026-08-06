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

// ============================================================
// Benchmark: FixedContainer vs DynamicContainer (from hybrid_set.h)
//
// Measures find() performance for different container sizes (1-8)
// and element types (int32_t, int64_t, std::string).
// ============================================================

#pragma once

#include <benchmark/benchmark.h>

#include <cstdint>
#include <string>
#include <vector>

#include "exprs/hybrid_set.h"

namespace doris {

// ============================================================
// Test data generators
// ============================================================

template <typename T>
static std::vector<T> generate_values(size_t n);

template <>
std::vector<int32_t> generate_values<int32_t>(size_t n) {
    std::vector<int32_t> vals;
    vals.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        vals.push_back(static_cast<int32_t>(i * 7 + 13));
    }
    return vals;
}

template <>
std::vector<int64_t> generate_values<int64_t>(size_t n) {
    std::vector<int64_t> vals;
    vals.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        vals.push_back(static_cast<int64_t>(i * 7 + 13));
    }
    return vals;
}

template <>
std::vector<std::string> generate_values<std::string>(size_t n) {
    std::vector<std::string> vals;
    vals.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        vals.push_back("key_" + std::to_string(i * 7 + 13));
    }
    return vals;
}

// Number of find() calls per iteration to amortize loop overhead.
static constexpr size_t FIND_ITERS = 10000;

// ============================================================
// FixedContainer benchmark: insert N values, then find them
// ============================================================

template <typename T, size_t N>
static void BM_FixedContainer_Find(benchmark::State& state) {
    auto values = generate_values<T>(N);

    // Also prepare a "miss" value for interleaving hit/miss lookups.
    auto miss_values = generate_values<T>(N + 4);

    for (auto _ : state) {
        FixedContainer<T, N> container;
        for (size_t i = 0; i < N; ++i) {
            container.insert(values[i]);
        }

        int64_t found = 0;
        for (size_t iter = 0; iter < FIND_ITERS; ++iter) {
            // Hit: search for existing value
            found += container.find(values[iter % N]);
            // Miss: search for non-existing value
            found += container.find(miss_values[N + (iter % 4)]);
        }
        benchmark::DoNotOptimize(found);
    }
}

// ============================================================
// DynamicContainer benchmark: insert N values, then find them
// ============================================================

template <typename T, size_t N>
static void BM_DynamicContainer_Find(benchmark::State& state) {
    auto values = generate_values<T>(N);
    auto miss_values = generate_values<T>(N + 4);

    for (auto _ : state) {
        DynamicContainer<T> container;
        for (size_t i = 0; i < N; ++i) {
            container.insert(values[i]);
        }

        int64_t found = 0;
        for (size_t iter = 0; iter < FIND_ITERS; ++iter) {
            found += container.find(values[iter % N]);
            found += container.find(miss_values[N + (iter % 4)]);
        }
        benchmark::DoNotOptimize(found);
    }
}

// ============================================================
// Register benchmarks for int32_t
// ============================================================

#define REGISTER_FIXED_INT32(N)                                                            \
    BENCHMARK(BM_FixedContainer_Find<int32_t, N>)->Name("Fixed_Int32_N" #N)->Unit(         \
            benchmark::kMicrosecond);                                                      \
    BENCHMARK(BM_DynamicContainer_Find<int32_t, N>)->Name("Dynamic_Int32_N" #N)->Unit(     \
            benchmark::kMicrosecond);

REGISTER_FIXED_INT32(1)
REGISTER_FIXED_INT32(2)
REGISTER_FIXED_INT32(3)
REGISTER_FIXED_INT32(4)
REGISTER_FIXED_INT32(5)
REGISTER_FIXED_INT32(6)
REGISTER_FIXED_INT32(7)
REGISTER_FIXED_INT32(8)

// ============================================================
// Register benchmarks for int64_t
// ============================================================

#define REGISTER_FIXED_INT64(N)                                                            \
    BENCHMARK(BM_FixedContainer_Find<int64_t, N>)->Name("Fixed_Int64_N" #N)->Unit(         \
            benchmark::kMicrosecond);                                                      \
    BENCHMARK(BM_DynamicContainer_Find<int64_t, N>)->Name("Dynamic_Int64_N" #N)->Unit(     \
            benchmark::kMicrosecond);

REGISTER_FIXED_INT64(1)
REGISTER_FIXED_INT64(2)
REGISTER_FIXED_INT64(3)
REGISTER_FIXED_INT64(4)
REGISTER_FIXED_INT64(5)
REGISTER_FIXED_INT64(6)
REGISTER_FIXED_INT64(7)
REGISTER_FIXED_INT64(8)

// ============================================================
// Register benchmarks for std::string
// ============================================================

#define REGISTER_FIXED_STRING(N)                                                               \
    BENCHMARK(BM_FixedContainer_Find<std::string, N>)->Name("Fixed_String_N" #N)->Unit(        \
            benchmark::kMicrosecond);                                                          \
    BENCHMARK(BM_DynamicContainer_Find<std::string, N>)->Name("Dynamic_String_N" #N)->Unit(    \
            benchmark::kMicrosecond);

REGISTER_FIXED_STRING(1)
REGISTER_FIXED_STRING(2)
REGISTER_FIXED_STRING(3)
REGISTER_FIXED_STRING(4)
REGISTER_FIXED_STRING(5)
REGISTER_FIXED_STRING(6)
REGISTER_FIXED_STRING(7)
REGISTER_FIXED_STRING(8)

#undef REGISTER_FIXED_INT32
#undef REGISTER_FIXED_INT64
#undef REGISTER_FIXED_STRING

} // namespace doris
