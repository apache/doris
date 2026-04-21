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
// Old kernel: faithful reproduction of the pre-change Doris path:
//   - per-column heap allocation of a temporary is_not_null buffer
//   - scan loop updates null_map_data + record_idx (uint32_t per row)
//   - same-column early-return check via record_idx
//   - separate fill loop guarded by filled_flag (uint8_t per row)
//
// New kernel: single-pass per column, col_null_map pointer taken
//   directly from ColumnNullable (nullptr for non-nullable columns).
//
// Scenarios:
//   Mixed50     – all cols nullable, ~50% null each
//   FirstNull   – col0 all-null nullable, col1/col2 non-nullable
//                 (exercises col_null_map == nullptr in the new path,
//                  constant-1 alloc in the old path)
//   FirstFull   – col0 all-not-null nullable (same-column early exit)
//   AllNonNull  – all cols non-nullable (nullptr branch throughout)
//
// Correctness: each BM_* function verifies old == new before
//   starting the timing loop, so any divergence aborts with
//   a state.SkipWithError() message.
// ============================================================

#pragma once

#include <benchmark/benchmark.h>

#include <cassert>
#include <cstdint>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

namespace doris {

static constexpr size_t COALESCE_N = 4096;
static constexpr size_t COALESCE_NCOLS = 3;

// -------------------------------------------------------
// Data helpers
// -------------------------------------------------------

struct CoalesceTestData {
    // null_maps[i] is EMPTY when column i is NOT nullable.
    // null_maps[i][j] == 1 means column i, row j is NULL.
    std::vector<std::vector<uint8_t>> null_maps;
    std::vector<std::vector<int64_t>> col_data;
    size_t n;
    size_t ncols;
};

// null_prob = probability that a cell is NULL (ignored when nullable==false)
static CoalesceTestData make_test_data(size_t n, size_t ncols, double null_prob,
                                       bool nullable = true) {
    std::mt19937 rng(42);
    std::uniform_real_distribution<double> prob(0.0, 1.0);
    std::uniform_int_distribution<int64_t> val(1, 1000000);

    CoalesceTestData d;
    d.n = n;
    d.ncols = ncols;
    d.null_maps.resize(ncols);
    d.col_data.resize(ncols, std::vector<int64_t>(n));
    for (size_t c = 0; c < ncols; ++c) {
        if (nullable) {
            d.null_maps[c].resize(n);
            for (size_t r = 0; r < n; ++r) {
                d.null_maps[c][r] = (prob(rng) < null_prob) ? 1 : 0;
            }
        }
        // else null_maps[c] stays empty → non-nullable column
        for (size_t r = 0; r < n; ++r) {
            d.col_data[c][r] = val(rng);
        }
    }
    return d;
}

// -------------------------------------------------------
// Old kernel: faithful reproduction of pre-change Doris path.
//
// Key overheads modelled:
//   1. record_idx[]  – uint32_t[n], heap allocated, updated in scan loop
//   2. filled_flags[] – uint8_t[n], heap allocated, guards fill loop
//   3. is_not_null_buf[] – uint8_t[n] allocated per-column iteration
//      (non-nullable → constant-1 vector; nullable → negated null map)
//   4. Two separate full-array loops per column (scan + fill)
//   5. Same-column early-return check via O(N) scan of record_idx
// -------------------------------------------------------
static void coalesce_old(const CoalesceTestData& d, std::vector<int64_t>& result,
                         std::vector<uint8_t>& result_null_map) {
    const size_t n = d.n;
    const size_t ncols = d.ncols;

    std::vector<uint8_t> null_map_data(n, 1);
    std::vector<uint32_t> record_idx(n, 0);
    std::vector<uint8_t> filled_flags(n, 0);

    result.assign(n, 0);
    size_t remaining_rows = n;

    for (size_t i = 0; i < ncols && remaining_rows; ++i) {
        const bool col_is_nullable = !d.null_maps[i].empty();

        // Model old is_not_null() helper: always allocates a ColumnUInt8 of size n.
        std::vector<uint8_t> is_not_null_buf(n);
        if (col_is_nullable) {
            const uint8_t* __restrict src = d.null_maps[i].data();
            for (size_t j = 0; j < n; ++j) {
                is_not_null_buf[j] = !src[j];
            }
        } else {
            // ColumnUInt8::create(size, 1) – constant 1, still allocated
            for (size_t j = 0; j < n; ++j) {
                is_not_null_buf[j] = 1;
            }
        }

        const uint8_t* __restrict res = is_not_null_buf.data();
        auto* __restrict nm = null_map_data.data();
        auto* __restrict ri = record_idx.data();

        // Phase 1: scan loop (mirrors old execute_column scan)
        for (size_t j = 0; j < n; ++j) {
            uint8_t mark = res[j] & nm[j];
            remaining_rows -= mark;
            ri[j] += mark * static_cast<uint32_t>(i);
            nm[j] -= mark;
        }

        // Same-column early-return check (O(N) record_idx scan)
        if (remaining_rows == 0) {
            size_t same = 0;
            const auto first = ri[0];
            for (size_t row = 0; row < n; ++row) {
                same += (ri[row] == first);
            }
            if (same == n) {
                // Return nested column directly (here: copy col_data[i] as proxy)
                result = d.col_data[i];
                result_null_map.assign(n, 0);
                return;
            }
        }

        // Phase 2: fill loop (mirrors old insert_result_data with filled_flag)
        const int64_t* __restrict col_vals = d.col_data[i].data();
        auto* __restrict ff = filled_flags.data();
        auto* __restrict out = result.data();
        for (size_t j = 0; j < n; ++j) {
            uint8_t should_fill = !(nm[j] | ff[j]);
            out[j] += col_vals[j] * static_cast<int64_t>(should_fill);
            ff[j] += should_fill;
        }
    }

    result_null_map = null_map_data;
}

// -------------------------------------------------------
// New kernel: single-pass per column.
// col_null_map == nullptr for non-nullable columns.
// -------------------------------------------------------
static void coalesce_new(const CoalesceTestData& d, std::vector<int64_t>& result,
                         std::vector<uint8_t>& result_null_map) {
    const size_t n = d.n;
    const size_t ncols = d.ncols;

    std::vector<uint8_t> null_map_data(n, 1);
    result.assign(n, 0);
    size_t remaining_rows = n;

    for (size_t i = 0; i < ncols && remaining_rows; ++i) {
        const bool col_is_nullable = !d.null_maps[i].empty();
        const uint8_t* __restrict col_null = col_is_nullable ? d.null_maps[i].data() : nullptr;
        const int64_t* __restrict col_vals = d.col_data[i].data();
        auto* __restrict nm = null_map_data.data();
        auto* __restrict out = result.data();

        size_t prev_remaining = remaining_rows;
        // Single pass: fill result and advance null_map_data
        for (size_t j = 0; j < n; ++j) {
            uint8_t is_not_null = col_null ? (!col_null[j]) : uint8_t(1);
            uint8_t should_fill = is_not_null & nm[j];
            out[j] += col_vals[j] * static_cast<int64_t>(should_fill);
            nm[j] -= should_fill;
            remaining_rows -= should_fill;
        }

        // All-same-column early exit: if all rows were committed on this pass
        if (remaining_rows == 0 && prev_remaining == n) {
            result = d.col_data[i];
            result_null_map.assign(n, 0);
            return;
        }
    }

    result_null_map = null_map_data;
}

// -------------------------------------------------------
// Correctness cross-check (called once before timing loop)
// -------------------------------------------------------
static void verify_old_new_equal(const CoalesceTestData& d) {
    std::vector<int64_t> r_old, r_new;
    std::vector<uint8_t> nm_old, nm_new;
    coalesce_old(d, r_old, nm_old);
    coalesce_new(d, r_new, nm_new);
    for (size_t j = 0; j < d.n; ++j) {
        // Both null-maps must agree on NULL-ness
        bool null_old = (nm_old[j] != 0);
        bool null_new = (nm_new[j] != 0);
        if (null_old != null_new) {
            throw std::logic_error("coalesce old/new null_map mismatch at row " +
                                   std::to_string(j));
        }
        // If the row is not null, values must match
        if (!null_old && r_old[j] != r_new[j]) {
            throw std::logic_error("coalesce old/new value mismatch at row " +
                                   std::to_string(j) + ": old=" + std::to_string(r_old[j]) +
                                   " new=" + std::to_string(r_new[j]));
        }
    }
}

// -------------------------------------------------------
// Scenarios
// -------------------------------------------------------

// A: ~50% null, all nullable
static const CoalesceTestData kMixed50 = make_test_data(COALESCE_N, COALESCE_NCOLS, 0.5, true);

// B: col0 all-null nullable, col1/col2 non-nullable
//    → old path: allocates constant-1 is_not_null buf for col1/col2
//    → new path: col_null_map == nullptr for col1/col2
static const CoalesceTestData kFirstNull = [] {
    CoalesceTestData d = make_test_data(COALESCE_N, COALESCE_NCOLS, 0.0, false);
    // col0: nullable, all-null
    d.null_maps[0].assign(COALESCE_N, 1);
    // col1/col2 remain non-nullable (empty null_maps)
    return d;
}();

// C: col0 all-not-null nullable → same-column early exit on col0
static const CoalesceTestData kFirstFull = [] {
    CoalesceTestData d = make_test_data(COALESCE_N, COALESCE_NCOLS, 0.5, true);
    d.null_maps[0].assign(COALESCE_N, 0); // col0: nullable, no nulls
    return d;
}();

// D: all columns non-nullable → exercises nullptr branch throughout
static const CoalesceTestData kAllNonNull =
        make_test_data(COALESCE_N, COALESCE_NCOLS, 0.0, false);

// -------------------------------------------------------
// Benchmark macros
// -------------------------------------------------------

#define DEFINE_COALESCE_BENCH(SCENARIO, SCENARIO_DATA)                                           \
    static void BM_Coalesce_##SCENARIO##_Old(benchmark::State& state) {                         \
        try {                                                                                     \
            verify_old_new_equal(SCENARIO_DATA);                                                 \
        } catch (const std::exception& e) {                                                      \
            state.SkipWithError(e.what());                                                       \
            return;                                                                               \
        }                                                                                        \
        std::vector<int64_t> result;                                                             \
        std::vector<uint8_t> null_map;                                                           \
        for (auto _ : state) {                                                                   \
            coalesce_old(SCENARIO_DATA, result, null_map);                                       \
            benchmark::DoNotOptimize(result.data()[0]);                                          \
        }                                                                                        \
        state.SetItemsProcessed(state.iterations() * COALESCE_N);                               \
    }                                                                                            \
    BENCHMARK(BM_Coalesce_##SCENARIO##_Old);                                                    \
                                                                                                 \
    static void BM_Coalesce_##SCENARIO##_New(benchmark::State& state) {                         \
        try {                                                                                     \
            verify_old_new_equal(SCENARIO_DATA);                                                 \
        } catch (const std::exception& e) {                                                      \
            state.SkipWithError(e.what());                                                       \
            return;                                                                               \
        }                                                                                        \
        std::vector<int64_t> result;                                                             \
        std::vector<uint8_t> null_map;                                                           \
        for (auto _ : state) {                                                                   \
            coalesce_new(SCENARIO_DATA, result, null_map);                                       \
            benchmark::DoNotOptimize(result.data()[0]);                                          \
        }                                                                                        \
        state.SetItemsProcessed(state.iterations() * COALESCE_N);                               \
    }                                                                                            \
    BENCHMARK(BM_Coalesce_##SCENARIO##_New)

DEFINE_COALESCE_BENCH(Mixed50, kMixed50);
DEFINE_COALESCE_BENCH(FirstNull, kFirstNull);
DEFINE_COALESCE_BENCH(FirstFull, kFirstFull);
DEFINE_COALESCE_BENCH(AllNonNull, kAllNonNull);

#undef DEFINE_COALESCE_BENCH

} // namespace doris
