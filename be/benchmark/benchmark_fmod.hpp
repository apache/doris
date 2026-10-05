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
#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>
#include <vector>

#include "exprs/function/fmod_fast.h"

namespace doris {
namespace {

enum FmodBenchCase {
    DB_DB = 0,
    IN_ONE_DB = 1,
    DB_IN_ONE = 2,
    DB_IN_TEN = 3,
    MIXED_SIGNS_AND_ZEROS = 4,
};

void fill_actual_load_sample(int case_id, size_t size, std::vector<double>* lhs,
                             std::vector<double>* rhs) {
    lhs->resize(size);
    rhs->resize(size);
    constexpr double db_scales[] = {1234.4500, 1876.2222, 8945.7353, 5612.6245, 4646.7853,
                                    6523.5285, 1000.1575, 6555.5678, 2587.8535, 3754.2575};
    constexpr uint64_t rows_per_load = 5'000'000;
    constexpr uint64_t total_rows = 50'000'000;

    std::mt19937_64 rng(0xadc83b19ULL);
    std::uniform_real_distribution<double> signed_large(-4.5e10, 4.5e10);
    std::uniform_real_distribution<double> signed_small(-10.0, 10.0);

    for (size_t i = 0; i < size; ++i) {
        uint64_t virtual_row = (static_cast<uint64_t>(i) * total_rows) / size;
        uint64_t block = std::min<uint64_t>(virtual_row / rows_per_load, 9);
        double row_num = static_cast<double>((virtual_row % rows_per_load) + 1);
        double db = row_num * db_scales[block];
        double in_one = row_num * 2e-7;
        double in_ten = row_num * 2e-6;

        switch (case_id) {
        case DB_DB:
            (*lhs)[i] = db;
            (*rhs)[i] = db;
            break;
        case IN_ONE_DB:
            (*lhs)[i] = in_one;
            (*rhs)[i] = db;
            break;
        case DB_IN_ONE:
            (*lhs)[i] = db;
            (*rhs)[i] = in_one;
            break;
        case DB_IN_TEN:
            (*lhs)[i] = db;
            (*rhs)[i] = in_ten;
            break;
        case MIXED_SIGNS_AND_ZEROS:
            (*lhs)[i] = signed_large(rng);
            (*rhs)[i] = i % 97 == 0 ? 0.0 : signed_small(rng);
            break;
        default:
            (*lhs)[i] = db;
            (*rhs)[i] = in_one;
            break;
        }
    }
}

void fill_actual_load_sample_float(int case_id, size_t size, std::vector<float>* lhs,
                                   std::vector<float>* rhs) {
    std::vector<double> lhs_double;
    std::vector<double> rhs_double;
    fill_actual_load_sample(case_id, size, &lhs_double, &rhs_double);

    lhs->resize(size);
    rhs->resize(size);
    for (size_t i = 0; i < size; ++i) {
        (*lhs)[i] = static_cast<float>(lhs_double[i]);
        (*rhs)[i] = static_cast<float>(rhs_double[i]);
    }
}

void std_vector_vector(const double* lhs, const double* rhs, double* result, uint8_t* null_map,
                       size_t size) {
    for (size_t i = 0; i < size; ++i) {
        uint8_t is_null = rhs[i] == 0.0;
        null_map[i] = is_null;
        result[i] = std::fmod(lhs[i], rhs[i] + static_cast<double>(is_null));
    }
}

void std_vector_vector(const float* lhs, const float* rhs, float* result, uint8_t* null_map,
                       size_t size) {
    for (size_t i = 0; i < size; ++i) {
        uint8_t is_null = rhs[i] == 0.0F;
        null_map[i] = is_null;
        float adjusted_rhs = rhs[i] + static_cast<float>(is_null);
        result[i] = static_cast<float>(
                std::fmod(static_cast<double>(lhs[i]), static_cast<double>(adjusted_rhs)));
    }
}

void std_vector_constant(const double* lhs, double rhs, double* result, uint8_t* null_map,
                         size_t size) {
    uint8_t is_null = rhs == 0.0;
    memset(null_map, is_null, size);
    if (is_null) {
        return;
    }
    for (size_t i = 0; i < size; ++i) {
        result[i] = std::fmod(lhs[i], rhs);
    }
}

void std_vector_constant(const float* lhs, float rhs, float* result, uint8_t* null_map,
                         size_t size) {
    uint8_t is_null = rhs == 0.0F;
    memset(null_map, is_null, size);
    if (is_null) {
        return;
    }
    for (size_t i = 0; i < size; ++i) {
        result[i] = static_cast<float>(
                std::fmod(static_cast<double>(lhs[i]), static_cast<double>(rhs)));
    }
}

void std_constant_vector(double lhs, const double* rhs, double* result, uint8_t* null_map,
                         size_t size) {
    for (size_t i = 0; i < size; ++i) {
        uint8_t is_null = rhs[i] == 0.0;
        null_map[i] = is_null;
        result[i] = std::fmod(lhs, rhs[i] + static_cast<double>(is_null));
    }
}

void std_constant_vector(float lhs, const float* rhs, float* result, uint8_t* null_map,
                         size_t size) {
    for (size_t i = 0; i < size; ++i) {
        uint8_t is_null = rhs[i] == 0.0F;
        null_map[i] = is_null;
        float adjusted_rhs = rhs[i] + static_cast<float>(is_null);
        result[i] = static_cast<float>(
                std::fmod(static_cast<double>(lhs), static_cast<double>(adjusted_rhs)));
    }
}

void benchmark_args(benchmark::internal::Benchmark* b) {
    constexpr int64_t rows = 1 << 20;
    b->Args({DB_DB, rows})
            ->Args({IN_ONE_DB, rows})
            ->Args({DB_IN_ONE, rows})
            ->Args({DB_IN_TEN, rows})
            ->Args({MIXED_SIGNS_AND_ZEROS, rows})
            ->Unit(benchmark::kMillisecond)
            ->UseRealTime()
            ->Repetitions(5)
            ->DisplayAggregatesOnly();
}

void benchmark_const_args(benchmark::internal::Benchmark* b) {
    constexpr int64_t rows = 1 << 20;
    b->Arg(rows)
            ->Unit(benchmark::kMillisecond)
            ->UseRealTime()
            ->Repetitions(5)
            ->DisplayAggregatesOnly();
}

static void BM_FmodDoubleVectorVectorStd(benchmark::State& state) {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_actual_load_sample(static_cast<int>(state.range(0)), state.range(1), &lhs, &rhs);
    std::vector<double> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        std_vector_vector(lhs.data(), rhs.data(), result.data(), null_map.data(), lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodDoubleVectorVectorFast(benchmark::State& state) {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_actual_load_sample(static_cast<int>(state.range(0)), state.range(1), &lhs, &rhs);
    std::vector<double> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        fmod_fast::vector_vector(lhs.data(), rhs.data(), result.data(), null_map.data(),
                                 lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodDoubleVectorConstantStd(benchmark::State& state) {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_actual_load_sample(DB_IN_ONE, state.range(0), &lhs, &rhs);
    std::vector<double> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        std_vector_constant(lhs.data(), 0.9999998, result.data(), null_map.data(), lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodDoubleVectorConstantFast(benchmark::State& state) {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_actual_load_sample(DB_IN_ONE, state.range(0), &lhs, &rhs);
    std::vector<double> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        fmod_fast::vector_constant(lhs.data(), 0.9999998, result.data(), null_map.data(),
                                   lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodDoubleConstZeroStd(benchmark::State& state) {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_actual_load_sample(DB_IN_ONE, state.range(0), &lhs, &rhs);
    std::vector<double> result(lhs.size(), -777.0);
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        std_vector_constant(lhs.data(), 0.0, result.data(), null_map.data(), lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodDoubleConstZeroFast(benchmark::State& state) {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_actual_load_sample(DB_IN_ONE, state.range(0), &lhs, &rhs);
    std::vector<double> result(lhs.size(), -777.0);
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        fmod_fast::vector_constant(lhs.data(), 0.0, result.data(), null_map.data(), lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodDoubleConstantVectorStd(benchmark::State& state) {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_actual_load_sample(IN_ONE_DB, state.range(0), &lhs, &rhs);
    std::vector<double> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        std_constant_vector(12345.678, rhs.data(), result.data(), null_map.data(), rhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rhs.size()));
}

static void BM_FmodDoubleConstantVectorFast(benchmark::State& state) {
    std::vector<double> lhs;
    std::vector<double> rhs;
    fill_actual_load_sample(IN_ONE_DB, state.range(0), &lhs, &rhs);
    std::vector<double> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        fmod_fast::constant_vector(12345.678, rhs.data(), result.data(), null_map.data(),
                                   rhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rhs.size()));
}

static void BM_FmodFloatVectorVectorStd(benchmark::State& state) {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_actual_load_sample_float(static_cast<int>(state.range(0)), state.range(1), &lhs, &rhs);
    std::vector<float> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        std_vector_vector(lhs.data(), rhs.data(), result.data(), null_map.data(), lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodFloatVectorVectorFast(benchmark::State& state) {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_actual_load_sample_float(static_cast<int>(state.range(0)), state.range(1), &lhs, &rhs);
    std::vector<float> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        fmod_fast::vector_vector(lhs.data(), rhs.data(), result.data(), null_map.data(),
                                 lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodFloatVectorConstantStd(benchmark::State& state) {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_actual_load_sample_float(DB_IN_ONE, state.range(0), &lhs, &rhs);
    std::vector<float> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        std_vector_constant(lhs.data(), 0.9999998F, result.data(), null_map.data(), lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodFloatVectorConstantFast(benchmark::State& state) {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_actual_load_sample_float(DB_IN_ONE, state.range(0), &lhs, &rhs);
    std::vector<float> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        fmod_fast::vector_constant(lhs.data(), 0.9999998F, result.data(), null_map.data(),
                                   lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodFloatConstZeroStd(benchmark::State& state) {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_actual_load_sample_float(DB_IN_ONE, state.range(0), &lhs, &rhs);
    std::vector<float> result(lhs.size(), -777.0F);
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        std_vector_constant(lhs.data(), 0.0F, result.data(), null_map.data(), lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodFloatConstZeroFast(benchmark::State& state) {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_actual_load_sample_float(DB_IN_ONE, state.range(0), &lhs, &rhs);
    std::vector<float> result(lhs.size(), -777.0F);
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        fmod_fast::vector_constant(lhs.data(), 0.0F, result.data(), null_map.data(), lhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(lhs.size()));
}

static void BM_FmodFloatConstantVectorStd(benchmark::State& state) {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_actual_load_sample_float(IN_ONE_DB, state.range(0), &lhs, &rhs);
    std::vector<float> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        std_constant_vector(12345.678F, rhs.data(), result.data(), null_map.data(), rhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rhs.size()));
}

static void BM_FmodFloatConstantVectorFast(benchmark::State& state) {
    std::vector<float> lhs;
    std::vector<float> rhs;
    fill_actual_load_sample_float(IN_ONE_DB, state.range(0), &lhs, &rhs);
    std::vector<float> result(lhs.size());
    std::vector<uint8_t> null_map(lhs.size());

    for (auto _ : state) {
        fmod_fast::constant_vector(12345.678F, rhs.data(), result.data(), null_map.data(),
                                   rhs.size());
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rhs.size()));
}

} // namespace

BENCHMARK(BM_FmodDoubleVectorVectorStd)->Apply(benchmark_args);
BENCHMARK(BM_FmodDoubleVectorVectorFast)->Apply(benchmark_args);
BENCHMARK(BM_FmodDoubleVectorConstantStd)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodDoubleVectorConstantFast)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodDoubleConstZeroStd)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodDoubleConstZeroFast)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodDoubleConstantVectorStd)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodDoubleConstantVectorFast)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodFloatVectorVectorStd)->Apply(benchmark_args);
BENCHMARK(BM_FmodFloatVectorVectorFast)->Apply(benchmark_args);
BENCHMARK(BM_FmodFloatVectorConstantStd)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodFloatVectorConstantFast)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodFloatConstZeroStd)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodFloatConstZeroFast)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodFloatConstantVectorStd)->Apply(benchmark_const_args);
BENCHMARK(BM_FmodFloatConstantVectorFast)->Apply(benchmark_const_args);

} // namespace doris
