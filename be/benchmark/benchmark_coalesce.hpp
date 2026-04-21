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
// Benchmark: coalesce inner-loop kernel for numeric (Int64) columns
//
// Old: two-array approach (null_map scan loop + separate fill loop with
//      filled_flag guard, plus unnecessary record_idx update).
// New: single-pass per column (fill + null_map update in one loop,
//      no filled_flag, no record_idx update for non-string types).
//
// Scenarios:
//   Mixed50   – each column ~50% null
//   FirstNull – col0 all null, col1 all not-null  (fills in 2nd pass)
//   FirstFull – col0 all not-null                  (fills in 1st pass)
// ============================================================

#pragma once

#include <benchmark/benchmark.h>

#include <cstdint>
#include <random>
#include <vector>

namespace doris {

static constexpr size_t COALESCE_N = 4096;
static constexpr size_t COALESCE_NCOLS = 3;

// -------------------------------------------------------
// Data helpers
// -------------------------------------------------------

struct CoalesceTestData {
    // null_maps[i][j] == 1 means column i, row j is NULL
    std::vector<std::vector<uint8_t>> null_maps;
    std::vector<std::vector<int64_t>> col_data;
    size_t n;
    size_t ncols;
};

// null_prob = probability that a cell is NULL
static CoalesceTestData make_test_data(size_t n, size_t ncols, double null_prob) {
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> prob(0.0, 1.0);
    std::uniform_int_distribution<int64_t> val(1, 1000000);

    CoalesceTestData d;
    d.n = n;
    d.ncols = ncols;
    d.null_maps.resize(ncols, std::vector<uint8_t>(n));
    d.col_data.resize(ncols, std::vector<int64_t>(n));
    for (size_t c = 0; c < ncols; ++c) {
        for (size_t r = 0; r < n; ++r) {
            d.null_maps[c][r] = (prob(rng) < null_prob) ? 1 : 0;
            d.col_data[c][r] = val(rng);
        }
    }
    return d;
}

// -------------------------------------------------------
// Old kernel: mirrors current Doris execute_column logic for numeric types
// Two arrays: null_map_data (scan) + filled_flag (fill guard)
// -------------------------------------------------------
static void coalesce_old(const CoalesceTestData& d, std::vector<int64_t>& result,
                         std::vector<uint8_t>& result_null_map) {
    const size_t n = d.n;
    const size_t ncols = d.ncols;

    // Initialize null_map (1 = not yet filled / row is null)
    std::vector<uint8_t> null_map_data(n, 1);
    // Track which rows have been filled in result
    std::vector<uint8_t> filled_flags(n, 0);

    result.assign(n, 0);
    size_t remaining_rows = n;

    for (size_t i = 0; i < ncols && remaining_rows; ++i) {
        const uint8_t* __restrict col_null = d.null_maps[i].data();
        const int64_t* __restrict col_vals = d.col_data[i].data();
        auto* __restrict nm = null_map_data.data();
        auto* __restrict ff = filled_flags.data();
        auto* __restrict res = result.data();

        // Phase 1: scan loop – compute is_not_null and update null_map_data
        // (mirrors the existing scan + record_idx loop, without record_idx for simplicity)
        for (size_t j = 0; j < n; ++j) {
            uint8_t is_not_null = !col_null[j];
            uint8_t mark = is_not_null & nm[j];
            remaining_rows -= mark;
            nm[j] -= mark; // 0 for committed rows
        }

        // Phase 2: fill loop – insert_result_data (current implementation)
        // condition: null_map==0 && filled_flag==0  → this row is being filled by col i
        for (size_t j = 0; j < n; ++j) {
            uint8_t should_fill = !(nm[j] | ff[j]);
            res[j] += col_vals[j] * static_cast<int64_t>(should_fill);
            ff[j] += should_fill;
        }
    }

    result_null_map = null_map_data; // remaining 1s mean the row is NULL in result
}

// -------------------------------------------------------
// New kernel: single-pass per column – no filled_flag, no record_idx
// -------------------------------------------------------
static void coalesce_new(const CoalesceTestData& d, std::vector<int64_t>& result,
                         std::vector<uint8_t>& result_null_map) {
    const size_t n = d.n;
    const size_t ncols = d.ncols;

    // null_map_data[j] == 1 means "row j not yet filled"
    std::vector<uint8_t> null_map_data(n, 1);
    result.assign(n, 0);
    size_t remaining_rows = n;

    for (size_t i = 0; i < ncols && remaining_rows; ++i) {
        const uint8_t* __restrict col_null = d.null_maps[i].data();
        const int64_t* __restrict col_vals = d.col_data[i].data();
        auto* __restrict nm = null_map_data.data();
        auto* __restrict res = result.data();

        // Single pass: fill result and update null_map_data in one loop
        for (size_t j = 0; j < n; ++j) {
            uint8_t should_fill = (!col_null[j]) & nm[j];
            res[j] += col_vals[j] * static_cast<int64_t>(should_fill);
            nm[j] -= should_fill;
            remaining_rows -= should_fill;
        }
    }

    result_null_map = null_map_data;
}

// -------------------------------------------------------
// Scenarios
// -------------------------------------------------------

// Scenario A: ~50% null per column
static const CoalesceTestData kMixed50 = make_test_data(COALESCE_N, COALESCE_NCOLS, 0.5);
// Scenario B: first column all null, second not null
static const CoalesceTestData kFirstNull = [] {
    CoalesceTestData d = make_test_data(COALESCE_N, COALESCE_NCOLS, 0.0);
    // make col0 all null
    for (size_t r = 0; r < COALESCE_N; ++r) {
        d.null_maps[0][r] = 1;
    }
    return d;
}();
// Scenario C: first column all not-null
static const CoalesceTestData kFirstFull = make_test_data(COALESCE_N, COALESCE_NCOLS, 0.0);

// -------------------------------------------------------
// Benchmark macros
// -------------------------------------------------------

#define DEFINE_COALESCE_BENCH(SCENARIO, SCENARIO_DATA)                                          \
    static void BM_Coalesce_##SCENARIO##_Old(benchmark::State& state) {                        \
        std::vector<int64_t> result;                                                            \
        std::vector<uint8_t> null_map;                                                          \
        for (auto _ : state) {                                                                  \
            coalesce_old(SCENARIO_DATA, result, null_map);                                      \
            benchmark::DoNotOptimize(result.data()[0]);                                         \
        }                                                                                       \
        state.SetItemsProcessed(state.iterations() * COALESCE_N);                              \
    }                                                                                           \
    BENCHMARK(BM_Coalesce_##SCENARIO##_Old);                                                   \
                                                                                                \
    static void BM_Coalesce_##SCENARIO##_New(benchmark::State& state) {                        \
        std::vector<int64_t> result;                                                            \
        std::vector<uint8_t> null_map;                                                          \
        for (auto _ : state) {                                                                  \
            coalesce_new(SCENARIO_DATA, result, null_map);                                      \
            benchmark::DoNotOptimize(result.data()[0]);                                         \
        }                                                                                       \
        state.SetItemsProcessed(state.iterations() * COALESCE_N);                              \
    }                                                                                           \
    BENCHMARK(BM_Coalesce_##SCENARIO##_New)

DEFINE_COALESCE_BENCH(Mixed50, kMixed50);
DEFINE_COALESCE_BENCH(FirstNull, kFirstNull);
DEFINE_COALESCE_BENCH(FirstFull, kFirstFull);

#undef DEFINE_COALESCE_BENCH

} // namespace doris
