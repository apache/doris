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

#include <random>
#include <vector>

#include "util/simd/expand.h"

namespace doris {

// Benchmarks comparing SIMD (via Expand::expand_load) vs branchless implementation
// (Expand::expand_load_branchless) for various data types.

static void BM_Expand_Int32_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int32_t> src(n);
    std::vector<int32_t> dst(n);
    std::vector<uint8_t> nulls(n);

    for (int i = 0; i < n; ++i) src[i] = i;
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (int i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Int32_Branchless(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int32_t> src(n);
    std::vector<int32_t> dst(n);
    std::vector<uint8_t> nulls(n);

    for (int i = 0; i < n; ++i) src[i] = i;
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (int i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load_branchless(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Int64_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int64_t> src(n);
    std::vector<int64_t> dst(n);
    std::vector<uint8_t> nulls(n);

    for (int i = 0; i < n; ++i) src[i] = i;
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (int i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Int64_Branchless(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int64_t> src(n);
    std::vector<int64_t> dst(n);
    std::vector<uint8_t> nulls(n);

    for (int i = 0; i < n; ++i) src[i] = i;
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (int i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load_branchless(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Float_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<float> src(n);
    std::vector<float> dst(n);
    std::vector<uint8_t> nulls(n);

    for (int i = 0; i < n; ++i) src[i] = static_cast<float>(i);
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (int i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Float_Branchless(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<float> src(n);
    std::vector<float> dst(n);
    std::vector<uint8_t> nulls(n);

    for (int i = 0; i < n; ++i) src[i] = static_cast<float>(i);
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (int i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load_branchless(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Double_SIMD(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<double> src(n);
    std::vector<double> dst(n);
    std::vector<uint8_t> nulls(n);

    for (int i = 0; i < n; ++i) src[i] = static_cast<double>(i);
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (int i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Double_Branchless(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<double> src(n);
    std::vector<double> dst(n);
    std::vector<uint8_t> nulls(n);

    for (int i = 0; i < n; ++i) src[i] = static_cast<double>(i);
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (int i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load_branchless(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

// Branch-based benchmarks
static void BM_Expand_Int32_Branch(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int32_t> src(n);
    std::vector<int32_t> dst(n);
    std::vector<uint8_t> nulls(n);

    for (size_t i = 0; i < n; ++i) src[i] = static_cast<int32_t>(i);
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (size_t i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load_branch(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Int64_Branch(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<int64_t> src(n);
    std::vector<int64_t> dst(n);
    std::vector<uint8_t> nulls(n);

    for (size_t i = 0; i < n; ++i) src[i] = static_cast<int64_t>(i);
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (size_t i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load_branch(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Float_Branch(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<float> src(n);
    std::vector<float> dst(n);
    std::vector<uint8_t> nulls(n);

    for (size_t i = 0; i < n; ++i) src[i] = static_cast<float>(i);
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (size_t i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load_branch(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

static void BM_Expand_Double_Branch(benchmark::State& state) {
    size_t n = static_cast<size_t>(state.range(0));
    std::vector<double> src(n);
    std::vector<double> dst(n);
    std::vector<uint8_t> nulls(n);

    for (size_t i = 0; i < n; ++i) src[i] = static_cast<double>(i);
    std::mt19937 gen(42);
    std::bernoulli_distribution d(0.5);
    for (size_t i = 0; i < n; ++i) nulls[i] = d(gen);

    for (auto _ : state) {
        benchmark::DoNotOptimize(src.data());
        benchmark::DoNotOptimize(nulls.data());
        doris::simd::Expand::expand_load_branch(dst.data(), src.data(), nulls.data(), n);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * static_cast<int64_t>(n));
}

// Register benchmarks with a few sizes
BENCHMARK(BM_Expand_Int32_SIMD)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Int32_Branchless)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Int32_Branch)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Int64_SIMD)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Int64_Branchless)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Int64_Branch)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Float_SIMD)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Float_Branchless)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Float_Branch)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Double_SIMD)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Double_Branchless)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);
BENCHMARK(BM_Expand_Double_Branch)->Arg(1 << 12)->Arg(1 << 16)->Arg(1 << 20);

} // namespace doris
