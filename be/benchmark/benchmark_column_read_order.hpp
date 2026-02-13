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
#include <cstdint>
#include <cstring>
#include <numeric>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "vec/exec/format/parquet/column_read_order_ctx.h"

namespace doris::vectorized {

// ============================================================================
// P0-2 Benchmark: Predicate Column Read Order Optimization
//
// This benchmark compares three strategies:
//
//  1. AllAtOnce (baseline)
//     Read ALL predicate columns fully (all rows), then evaluate filters.
//     This is the original _do_lazy_read() path with no P0-1 or P0-2.
//
//  2. PerCol_NoPushdown (P0-2 only, no P0-1)
//     Read columns one-by-one with intermediate filtering. However, the
//     decoder does NOT receive the filter bitmap — it still decodes ALL
//     rows (num_rows). The benefit comes only from being able to skip
//     evaluating conjuncts on already-filtered rows and potentially
//     short-circuiting. In practice this means: decode cost is the same
//     as AllAtOnce per column, but we evaluate filters earlier.
//
//  3. PerCol_WithPushdown (P0-2 + P0-1)
//     Read columns one-by-one with intermediate filtering AND filter
//     bitmap pushdown. The decoder only decodes surviving rows (via P0-1).
//     This is the full optimized path.
//
// For each strategy we test BestOrder and WorstOrder column orderings.
//
// We also include Adaptive (ColumnReadOrderCtx) and overhead benchmarks.
// ============================================================================

// ---- Helper: generate a random filter with given selectivity ----
static std::vector<uint8_t> p02_gen_column_filter(int num_rows, double selectivity, unsigned seed) {
    std::mt19937 rng(seed);
    std::uniform_real_distribution<double> dist(0.0, 1.0);
    std::vector<uint8_t> filter(num_rows);
    for (int i = 0; i < num_rows; ++i) {
        filter[i] = dist(rng) < selectivity ? 1 : 0;
    }
    return filter;
}

// ---- Helper: combine (AND) two filters ----
static void p02_combine_filters(std::vector<uint8_t>& combined,
                                const std::vector<uint8_t>& col_filter, int num_rows) {
    for (int i = 0; i < num_rows; ++i) {
        combined[i] &= col_filter[i];
    }
}

// ---- Helper: count surviving rows ----
static int p02_count_survivors(const std::vector<uint8_t>& filter, int num_rows) {
    int count = 0;
    for (int i = 0; i < num_rows; ++i) {
        count += filter[i];
    }
    return count;
}

// Simulated decode WITH P0-1 pushdown:
// Only touches surviving rows. Cost = survivors * per_row_cost.
static void p02_decode_with_pushdown(const std::vector<uint8_t>& surviving_filter, int num_rows,
                                     int per_row_cost, std::vector<uint8_t>& scratch) {
    if (static_cast<int>(scratch.size()) < num_rows * per_row_cost) {
        scratch.resize(num_rows * per_row_cost);
    }
    int offset = 0;
    for (int i = 0; i < num_rows; ++i) {
        if (surviving_filter[i]) {
            memset(scratch.data() + offset, static_cast<int>(i & 0xFF), per_row_cost);
            offset += per_row_cost;
        }
    }
    benchmark::DoNotOptimize(scratch.data());
    benchmark::ClobberMemory();
}

// Simulated decode WITHOUT P0-1 pushdown:
// Decodes ALL rows regardless of filter. Cost = num_rows * per_row_cost.
// This is what the decoder does when it doesn't receive filter_data.
static void p02_decode_no_pushdown(int num_rows, int per_row_cost, std::vector<uint8_t>& scratch) {
    int total = num_rows * per_row_cost;
    if (static_cast<int>(scratch.size()) < total) {
        scratch.resize(total);
    }
    memset(scratch.data(), 0x42, total);
    benchmark::DoNotOptimize(scratch.data());
    benchmark::ClobberMemory();
}

// ---- Column config for simulation ----
struct P02SimColumn {
    int cost;                    // per-row decode cost in bytes
    double selectivity;          // fraction of rows passing this column's filter
    std::vector<uint8_t> filter; // pre-generated filter
};

static std::vector<P02SimColumn> p02_build_sim_columns(int num_rows, int num_cols,
                                                       const std::vector<int>& costs,
                                                       const std::vector<double>& selectivities) {
    std::vector<P02SimColumn> cols(num_cols);
    for (int i = 0; i < num_cols; ++i) {
        cols[i].cost = costs[i];
        cols[i].selectivity = selectivities[i];
        cols[i].filter = p02_gen_column_filter(num_rows, selectivities[i], 1000 + i);
    }
    return cols;
}

// ---- Scenario setup helper ----
static void p02_setup_scenario(int num_cols, int scenario, std::vector<int>& costs,
                               std::vector<double>& selectivities) {
    costs.resize(num_cols);
    selectivities.resize(num_cols);
    for (int i = 0; i < num_cols; ++i) {
        costs[i] = 32;
    }
    switch (scenario) {
    case 0: // skewed: one column 1%, rest 90%
        for (int i = 0; i < num_cols; ++i) {
            selectivities[i] = (i == num_cols - 1) ? 0.01 : 0.90;
        }
        break;
    case 1: // uniform: all 50%
        for (int i = 0; i < num_cols; ++i) {
            selectivities[i] = 0.50;
        }
        break;
    case 2: // cascading: 80% -> 20%
        for (int i = 0; i < num_cols; ++i) {
            selectivities[i] = 0.80 - i * (0.60 / std::max(num_cols - 1, 1));
            if (selectivities[i] < 0.05) selectivities[i] = 0.05;
        }
        break;
    default:
        break;
    }
}

static std::string p02_scenario_name(int scenario) {
    switch (scenario) {
    case 0:
        return "skewed";
    case 1:
        return "uniform";
    case 2:
        return "cascading";
    default:
        return "unknown";
    }
}

// Sort order helpers
static std::vector<int> p02_best_order(const std::vector<P02SimColumn>& cols, int num_cols) {
    std::vector<int> order(num_cols);
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(),
              [&](int a, int b) { return cols[a].selectivity < cols[b].selectivity; });
    return order;
}

static std::vector<int> p02_worst_order(const std::vector<P02SimColumn>& cols, int num_cols) {
    std::vector<int> order(num_cols);
    std::iota(order.begin(), order.end(), 0);
    std::sort(order.begin(), order.end(),
              [&](int a, int b) { return cols[a].selectivity > cols[b].selectivity; });
    return order;
}

// ============================================================================
// Benchmark 1: AllAtOnce — Baseline (no P0-1, no P0-2)
//
// Read ALL predicate columns fully (all rows decoded), then filter.
// Total decode work = num_cols * num_rows * per_row_cost
// ============================================================================
static void BM_P02_AllAtOnce(benchmark::State& state) {
    int num_cols = static_cast<int>(state.range(0));
    int num_rows = static_cast<int>(state.range(1)) * 1000;
    int scenario = static_cast<int>(state.range(2));

    std::vector<int> costs;
    std::vector<double> selectivities;
    p02_setup_scenario(num_cols, scenario, costs, selectivities);
    auto columns = p02_build_sim_columns(num_rows, num_cols, costs, selectivities);
    std::vector<uint8_t> scratch;

    for (auto _ : state) {
        // Phase 1: decode ALL columns, ALL rows (no filter pushdown)
        for (int c = 0; c < num_cols; ++c) {
            p02_decode_no_pushdown(num_rows, columns[c].cost, scratch);
        }
        // Phase 2: evaluate all filters at once
        std::vector<uint8_t> combined(num_rows, 1);
        for (int c = 0; c < num_cols; ++c) {
            p02_combine_filters(combined, columns[c].filter, num_rows);
        }
        benchmark::DoNotOptimize(combined.data());
    }

    state.SetLabel("cols=" + std::to_string(num_cols) + " rows=" + std::to_string(num_rows) + " " +
                   p02_scenario_name(scenario));
    state.SetItemsProcessed(state.iterations() * num_rows * num_cols);
}

// ============================================================================
// Benchmark 2: PerCol_NoPushdown — P0-2 only (no P0-1)
//
// Read columns one-by-one, evaluate per-col filter after each.
// BUT decoder still decodes ALL rows (no filter bitmap pushdown).
// Benefit: can skip conjunct evaluation for filtered rows, and if a
// column filters everything, remaining columns don't need to be read.
// Cost: same decode work per column as AllAtOnce.
// ============================================================================
static void BM_P02_PerCol_NoPushdown_Best(benchmark::State& state) {
    int num_cols = static_cast<int>(state.range(0));
    int num_rows = static_cast<int>(state.range(1)) * 1000;
    int scenario = static_cast<int>(state.range(2));

    std::vector<int> costs;
    std::vector<double> selectivities;
    p02_setup_scenario(num_cols, scenario, costs, selectivities);
    auto columns = p02_build_sim_columns(num_rows, num_cols, costs, selectivities);
    auto order = p02_best_order(columns, num_cols);
    std::vector<uint8_t> scratch;

    for (auto _ : state) {
        std::vector<uint8_t> combined(num_rows, 1);
        for (int idx = 0; idx < num_cols; ++idx) {
            int c = order[idx];
            // Decoder decodes ALL rows (no pushdown)
            p02_decode_no_pushdown(num_rows, columns[c].cost, scratch);
            // Evaluate per-col filter
            p02_combine_filters(combined, columns[c].filter, num_rows);
        }
        benchmark::DoNotOptimize(combined.data());
    }

    state.SetLabel("cols=" + std::to_string(num_cols) + " rows=" + std::to_string(num_rows) + " " +
                   p02_scenario_name(scenario));
    state.SetItemsProcessed(state.iterations() * num_rows * num_cols);
}

static void BM_P02_PerCol_NoPushdown_Worst(benchmark::State& state) {
    int num_cols = static_cast<int>(state.range(0));
    int num_rows = static_cast<int>(state.range(1)) * 1000;
    int scenario = static_cast<int>(state.range(2));

    std::vector<int> costs;
    std::vector<double> selectivities;
    p02_setup_scenario(num_cols, scenario, costs, selectivities);
    auto columns = p02_build_sim_columns(num_rows, num_cols, costs, selectivities);
    auto order = p02_worst_order(columns, num_cols);
    std::vector<uint8_t> scratch;

    for (auto _ : state) {
        std::vector<uint8_t> combined(num_rows, 1);
        for (int idx = 0; idx < num_cols; ++idx) {
            int c = order[idx];
            p02_decode_no_pushdown(num_rows, columns[c].cost, scratch);
            p02_combine_filters(combined, columns[c].filter, num_rows);
        }
        benchmark::DoNotOptimize(combined.data());
    }

    state.SetLabel("cols=" + std::to_string(num_cols) + " rows=" + std::to_string(num_rows) + " " +
                   p02_scenario_name(scenario));
    state.SetItemsProcessed(state.iterations() * num_rows * num_cols);
}

// ============================================================================
// Benchmark 3: PerCol_WithPushdown — P0-2 + P0-1 (full optimization)
//
// Read columns one-by-one, evaluate per-col filter after each.
// Decoder receives accumulated filter bitmap and ONLY decodes surviving rows.
// This is the full P0-2 + P0-1 path.
// ============================================================================
static void BM_P02_PerCol_WithPushdown_Best(benchmark::State& state) {
    int num_cols = static_cast<int>(state.range(0));
    int num_rows = static_cast<int>(state.range(1)) * 1000;
    int scenario = static_cast<int>(state.range(2));

    std::vector<int> costs;
    std::vector<double> selectivities;
    p02_setup_scenario(num_cols, scenario, costs, selectivities);
    auto columns = p02_build_sim_columns(num_rows, num_cols, costs, selectivities);
    auto order = p02_best_order(columns, num_cols);
    std::vector<uint8_t> scratch;

    for (auto _ : state) {
        std::vector<uint8_t> combined(num_rows, 1);
        for (int idx = 0; idx < num_cols; ++idx) {
            int c = order[idx];
            // Decoder only decodes surviving rows (P0-1 pushdown)
            p02_decode_with_pushdown(combined, num_rows, columns[c].cost, scratch);
            // Evaluate per-col filter
            p02_combine_filters(combined, columns[c].filter, num_rows);
        }
        benchmark::DoNotOptimize(combined.data());
    }

    state.SetLabel("cols=" + std::to_string(num_cols) + " rows=" + std::to_string(num_rows) + " " +
                   p02_scenario_name(scenario));
    state.SetItemsProcessed(state.iterations() * num_rows * num_cols);
}

static void BM_P02_PerCol_WithPushdown_Worst(benchmark::State& state) {
    int num_cols = static_cast<int>(state.range(0));
    int num_rows = static_cast<int>(state.range(1)) * 1000;
    int scenario = static_cast<int>(state.range(2));

    std::vector<int> costs;
    std::vector<double> selectivities;
    p02_setup_scenario(num_cols, scenario, costs, selectivities);
    auto columns = p02_build_sim_columns(num_rows, num_cols, costs, selectivities);
    auto order = p02_worst_order(columns, num_cols);
    std::vector<uint8_t> scratch;

    for (auto _ : state) {
        std::vector<uint8_t> combined(num_rows, 1);
        for (int idx = 0; idx < num_cols; ++idx) {
            int c = order[idx];
            p02_decode_with_pushdown(combined, num_rows, columns[c].cost, scratch);
            p02_combine_filters(combined, columns[c].filter, num_rows);
        }
        benchmark::DoNotOptimize(combined.data());
    }

    state.SetLabel("cols=" + std::to_string(num_cols) + " rows=" + std::to_string(num_rows) + " " +
                   p02_scenario_name(scenario));
    state.SetItemsProcessed(state.iterations() * num_rows * num_cols);
}

// ============================================================================
// Benchmark 4: PerCol_WithPushdown_Adaptive — P0-2 + P0-1 with Ctx
//
// Full path with ColumnReadOrderCtx adaptive ordering.
// Runs 20 batches (10 exploration + 10 exploitation).
// ============================================================================
static void BM_P02_PerCol_Adaptive(benchmark::State& state) {
    int num_cols = static_cast<int>(state.range(0));
    int num_rows = static_cast<int>(state.range(1)) * 1000;
    int scenario = static_cast<int>(state.range(2));

    std::vector<int> costs;
    std::vector<double> selectivities;
    p02_setup_scenario(num_cols, scenario, costs, selectivities);
    auto columns = p02_build_sim_columns(num_rows, num_cols, costs, selectivities);
    std::vector<uint8_t> scratch;

    for (auto _ : state) {
        std::vector<size_t> col_indices(num_cols);
        std::iota(col_indices.begin(), col_indices.end(), 0);
        std::unordered_map<size_t, size_t> cost_map;
        size_t total_cost = 0;
        for (int i = 0; i < num_cols; ++i) {
            cost_map[i] = columns[i].cost;
            total_cost += columns[i].cost;
        }
        ColumnReadOrderCtx ctx(col_indices, cost_map, total_cost * num_rows);

        for (int batch = 0; batch < 20; ++batch) {
            const auto& read_order = ctx.get_column_read_order();

            std::vector<uint8_t> combined(num_rows, 1);
            size_t round_cost = 0;
            double first_selectivity = 1.0;

            for (size_t idx = 0; idx < read_order.size(); ++idx) {
                size_t c = read_order[idx];
                int survivors = p02_count_survivors(combined, num_rows);
                round_cost += survivors * columns[c].cost;
                // P0-1 pushdown: only decode surviving rows
                p02_decode_with_pushdown(combined, num_rows, columns[c].cost, scratch);
                p02_combine_filters(combined, columns[c].filter, num_rows);

                if (idx == 0) {
                    int new_survivors = p02_count_survivors(combined, num_rows);
                    first_selectivity =
                            survivors > 0 ? static_cast<double>(new_survivors) / survivors : 0.0;
                }
            }

            ctx.update(round_cost, first_selectivity);
            benchmark::DoNotOptimize(combined.data());
        }
    }

    state.SetLabel("cols=" + std::to_string(num_cols) + " rows=" + std::to_string(num_rows) + " " +
                   p02_scenario_name(scenario) + " 20batches");
    state.SetItemsProcessed(state.iterations() * num_rows * num_cols * 20);
}

// ============================================================================
// Benchmark 5: Filter Accumulation (bitwise AND) overhead
// ============================================================================
static void BM_P02_FilterAccumulation(benchmark::State& state) {
    int num_cols = static_cast<int>(state.range(0));
    int num_rows = static_cast<int>(state.range(1)) * 1000;

    std::vector<std::vector<uint8_t>> filters(num_cols);
    for (int i = 0; i < num_cols; ++i) {
        filters[i] = p02_gen_column_filter(num_rows, 0.5, 2000 + i);
    }

    for (auto _ : state) {
        std::vector<uint8_t> combined(num_rows, 1);
        for (int c = 0; c < num_cols; ++c) {
            p02_combine_filters(combined, filters[c], num_rows);
        }
        benchmark::DoNotOptimize(combined.data());
        benchmark::ClobberMemory();
    }

    state.SetLabel("cols=" + std::to_string(num_cols) + " rows=" + std::to_string(num_rows));
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(num_rows) * num_cols);
}

// ============================================================================
// Benchmark 6: ColumnReadOrderCtx overhead
// ============================================================================
static void BM_P02_CtxOverhead(benchmark::State& state) {
    int num_cols = static_cast<int>(state.range(0));

    for (auto _ : state) {
        std::vector<size_t> col_indices(num_cols);
        std::iota(col_indices.begin(), col_indices.end(), 0);
        std::unordered_map<size_t, size_t> cost_map;
        size_t total_cost = 0;
        for (int i = 0; i < num_cols; ++i) {
            cost_map[i] = 32;
            total_cost += 32;
        }
        ColumnReadOrderCtx ctx(col_indices, cost_map, total_cost * 4096);

        for (int batch = 0; batch < 20; ++batch) {
            const auto& order = ctx.get_column_read_order();
            benchmark::DoNotOptimize(order.data());
            size_t fake_cost = 1000 - batch * 30;
            double fake_sel = 0.5 - batch * 0.02;
            ctx.update(fake_cost, fake_sel);
        }
        benchmark::ClobberMemory();
    }

    state.SetLabel("cols=" + std::to_string(num_cols));
    state.SetItemsProcessed(state.iterations() * 20);
}

// ============================================================================
// Registrations
// ============================================================================
// Args: (num_cols, num_rows_in_thousands, scenario)
// Scenario: 0=skewed, 1=uniform, 2=cascading

#define P02_COMMON_ARGS         \
    ->Args({4, 100, 0})         \
            ->Args({4, 100, 1}) \
            ->Args({4, 100, 2}) \
            ->Args({8, 100, 0}) \
            ->Args({8, 100, 1}) \
            ->Args({8, 100, 2}) \
            ->Args({2, 100, 0}) \
            ->Unit(benchmark::kMicrosecond)

// --- Baseline: AllAtOnce (no P0-1, no P0-2) ---
BENCHMARK(BM_P02_AllAtOnce) P02_COMMON_ARGS;

// --- P0-2 only (no P0-1): PerCol with no decoder pushdown ---
BENCHMARK(BM_P02_PerCol_NoPushdown_Best) P02_COMMON_ARGS;
BENCHMARK(BM_P02_PerCol_NoPushdown_Worst) P02_COMMON_ARGS;

// --- P0-2 + P0-1: PerCol with decoder pushdown ---
BENCHMARK(BM_P02_PerCol_WithPushdown_Best) P02_COMMON_ARGS;
BENCHMARK(BM_P02_PerCol_WithPushdown_Worst) P02_COMMON_ARGS;

// --- P0-2 + P0-1 Adaptive ---
BENCHMARK(BM_P02_PerCol_Adaptive) P02_COMMON_ARGS;

// --- Filter Accumulation overhead ---
BENCHMARK(BM_P02_FilterAccumulation)
        ->Args({2, 100})
        ->Args({4, 100})
        ->Args({8, 100})
        ->Args({4, 1000})
        ->Unit(benchmark::kMicrosecond);

// --- Ctx overhead ---
BENCHMARK(BM_P02_CtxOverhead)
        ->Args({2})
        ->Args({4})
        ->Args({8})
        ->Args({16})
        ->Unit(benchmark::kNanosecond);

} // namespace doris::vectorized
