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
// Benchmark: int_divide vector-constant path
//
// Compares the current scalar libdivide loop with an AVX2-SIMD
// libdivide loop for the vector_constant case.
//
// Types benchmarked: Int32, UInt32, Int64, UInt64
// ============================================================

#pragma once

#include <benchmark/benchmark.h>
#include <libdivide.h>

#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace doris {

static constexpr size_t INT_DIVIDE_N = 4096;

// ---------- helpers --------------------------------------------------

template <typename T>
static std::vector<T> make_random_data(size_t n, T lo, T hi) {
    std::mt19937_64 rng(42);
    std::uniform_int_distribution<T> dist(lo, hi);
    std::vector<T> v(n);
    for (auto& x : v) {
        x = dist(rng);
    }
    return v;
}

// ---------- Old: scalar libdivide loop (current Doris) ---------------

template <typename T>
static void int_divide_scalar_old(const T* __restrict a, T b, T* __restrict c, size_t size) {
    const libdivide::divider<T> divider(b);
    for (size_t i = 0; i < size; i++) {
        c[i] = a[i] / divider;
    }
}

// ---------- New: AVX2 SIMD libdivide loop ----------------------------

#ifdef LIBDIVIDE_AVX2
template <typename T>
static void int_divide_simd_new(const T* __restrict a, T b, T* __restrict c, size_t size) {
    const libdivide::divider<T> divider(b);

    constexpr size_t values_per_reg = sizeof(__m256i) / sizeof(T);
    const size_t simd_size = size / values_per_reg * values_per_reg;

    size_t i = 0;
    for (; i < simd_size; i += values_per_reg) {
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(c + i),
                            _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i)) / divider);
    }
    // tail
    for (; i < size; i++) {
        c[i] = a[i] / divider;
    }
}
#else
template <typename T>
static void int_divide_simd_new(const T* __restrict a, T b, T* __restrict c, size_t size) {
    // fallback to scalar when AVX2 not available
    int_divide_scalar_old(a, b, c, size);
}
#endif

// ---------- Benchmark definitions ------------------------------------

#define DEFINE_INT_DIVIDE_BENCH(TNAME, T, DIVISOR)                                            \
    static void BM_IntDiv_##TNAME##_Old(benchmark::State& state) {                            \
        auto a = make_random_data<T>(INT_DIVIDE_N, 1, std::numeric_limits<T>::max() / 2);     \
        std::vector<T> c(INT_DIVIDE_N);                                                        \
        for (auto _ : state) {                                                                 \
            int_divide_scalar_old<T>(a.data(), static_cast<T>(DIVISOR), c.data(),             \
                                     INT_DIVIDE_N);                                            \
            benchmark::DoNotOptimize(c.data()[0]);                                               \
        }                                                                                      \
        state.SetItemsProcessed(state.iterations() * INT_DIVIDE_N);                           \
    }                                                                                          \
    BENCHMARK(BM_IntDiv_##TNAME##_Old);                                                        \
                                                                                               \
    static void BM_IntDiv_##TNAME##_New(benchmark::State& state) {                            \
        auto a = make_random_data<T>(INT_DIVIDE_N, 1, std::numeric_limits<T>::max() / 2);     \
        std::vector<T> c(INT_DIVIDE_N);                                                        \
        for (auto _ : state) {                                                                 \
            int_divide_simd_new<T>(a.data(), static_cast<T>(DIVISOR), c.data(),               \
                                   INT_DIVIDE_N);                                              \
            benchmark::DoNotOptimize(c.data()[0]);                                               \
        }                                                                                      \
        state.SetItemsProcessed(state.iterations() * INT_DIVIDE_N);                           \
    }                                                                                          \
    BENCHMARK(BM_IntDiv_##TNAME##_New)

DEFINE_INT_DIVIDE_BENCH(Int32, int32_t, 7);
DEFINE_INT_DIVIDE_BENCH(UInt32, uint32_t, 7);
DEFINE_INT_DIVIDE_BENCH(Int64, int64_t, 7);
DEFINE_INT_DIVIDE_BENCH(UInt64, uint64_t, 7);

#undef DEFINE_INT_DIVIDE_BENCH

} // namespace doris
