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
#include <cstddef>
#include <cstdint>
#include <numeric>
#include <string>
#include <vector>

#include "format_v2/parquet/selection_vector.h"
#include "parquet_benchmark_scenarios.h"

namespace doris::parquet_benchmark::selection_detail {

constexpr size_t SELECTION_ROWS = 1UL << 12;
constexpr int CASCADE_FIRST_SELECTIVITY = 90;

inline std::vector<uint8_t> make_filter(size_t rows, int selectivity_percent, Pattern pattern) {
    std::vector<uint8_t> filter(rows, 0);
    const size_t selected_rows =
            rows * static_cast<size_t>(std::clamp(selectivity_percent, 0, 100)) / 100;
    if (pattern == Pattern::CLUSTERED) {
        std::fill_n(filter.begin(), selected_rows, uint8_t {1});
    } else if (selected_rows != 0) {
        for (size_t selected = 0; selected < selected_rows; ++selected) {
            filter[selected * rows / selected_rows] = 1;
        }
    }
    return filter;
}

// Keep the benchmark source buildable on revisions before the bulk compaction helpers. The
// fallback mirrors the former Parquet scan loops so one named matrix can compare both revisions.
template <typename Selection>
size_t compact_with_row_filter(Selection* selection, const uint8_t* filter, size_t rows) {
    if constexpr (requires { selection->compact_with_row_filter(filter, rows); }) {
        return selection->compact_with_row_filter(filter, rows);
    } else {
        size_t output = 0;
        for (size_t position = 0; position < rows; ++position) {
            const auto row = selection->get_index(position);
            if (filter[row] != 0) {
                selection->set_index(output++, row);
            }
        }
        return output;
    }
}

template <typename Selection>
size_t compact_with_selection_filter(Selection* selection, const uint8_t* filter, size_t rows) {
    if constexpr (requires { selection->compact_with_selection_filter(filter, rows); }) {
        return selection->compact_with_selection_filter(filter, rows);
    } else {
        size_t output = 0;
        for (size_t position = 0; position < rows; ++position) {
            if (filter[position] != 0) {
                selection->set_index(output++, selection->get_index(position));
            }
        }
        return output;
    }
}

inline std::vector<format::parquet::SelectionVector::Index> expected_selection_rows(
        const SelectionScenario& scenario, const std::vector<uint8_t>& row_filter,
        const std::vector<uint8_t>& first_filter, const std::vector<uint8_t>& selection_filter) {
    std::vector<format::parquet::SelectionVector::Index> rows(SELECTION_ROWS);
    std::iota(rows.begin(), rows.end(), 0);
    if (scenario.operation == SelectionOperation::RESIZE_IDENTITY) {
        return rows;
    }
    if (scenario.operation == SelectionOperation::ROW_FILTER) {
        std::erase_if(rows, [&](const auto row) { return row_filter[row] == 0; });
        return rows;
    }
    std::erase_if(rows, [&](const auto row) { return first_filter[row] == 0; });
    std::vector<format::parquet::SelectionVector::Index> cascaded;
    cascaded.reserve(rows.size());
    for (size_t position = 0; position < rows.size(); ++position) {
        if (selection_filter[position] != 0) {
            cascaded.push_back(rows[position]);
        }
    }
    return cascaded;
}

inline void run_selection(benchmark::State& state, const SelectionScenario& scenario) {
    format::parquet::SelectionVector selection;
    const auto row_filter =
            make_filter(SELECTION_ROWS, scenario.selectivity_percent, scenario.pattern);
    const auto first_filter =
            make_filter(SELECTION_ROWS, CASCADE_FIRST_SELECTIVITY, Pattern::ALTERNATING);
    const size_t first_selected =
            static_cast<size_t>(std::count(first_filter.begin(), first_filter.end(), uint8_t {1}));
    const auto selection_filter =
            make_filter(first_selected, scenario.selectivity_percent, scenario.pattern);
    const auto expected_rows =
            expected_selection_rows(scenario, row_filter, first_filter, selection_filter);

    size_t selected_rows = 0;
    for (auto _ : state) {
        selection.resize(SELECTION_ROWS);
        switch (scenario.operation) {
        case SelectionOperation::RESIZE_IDENTITY:
            selected_rows = SELECTION_ROWS;
            break;
        case SelectionOperation::ROW_FILTER:
            selected_rows = compact_with_row_filter(&selection, row_filter.data(), SELECTION_ROWS);
            break;
        case SelectionOperation::CASCADE_FILTER:
            selected_rows =
                    compact_with_row_filter(&selection, first_filter.data(), SELECTION_ROWS);
            selected_rows = compact_with_selection_filter(&selection, selection_filter.data(),
                                                          selected_rows);
            break;
        }
        benchmark::DoNotOptimize(selected_rows);
        benchmark::ClobberMemory();
    }
    bool selection_matches = selected_rows == expected_rows.size();
    for (size_t position = 0; selection_matches && position < selected_rows; ++position) {
        selection_matches = selection.get_index(position) == expected_rows[position];
    }
    if (!selection_matches) {
        state.SkipWithError("selection compaction produced unexpected row indices");
        return;
    }

    state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * SELECTION_ROWS));
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * SELECTION_ROWS));
    state.counters["raw_rows"] = static_cast<double>(SELECTION_ROWS);
    state.counters["selected_rows"] = static_cast<double>(selected_rows);
    state.counters["ns/raw_row"] = benchmark::Counter(
            static_cast<double>(SELECTION_ROWS),
            benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
}

inline bool register_selection_benchmarks() {
    for (const auto& scenario : selection_scenarios()) {
        const std::string name = "ParquetSelection/" + to_string(scenario.operation) + "/sel_" +
                                 std::to_string(scenario.selectivity_percent) + "/" +
                                 to_string(scenario.pattern);
        benchmark::RegisterBenchmark(name.c_str(), [=](benchmark::State& state) {
            run_selection(state, scenario);
        })->Unit(benchmark::kNanosecond);
    }
    return true;
}

inline const bool SELECTION_BENCHMARKS_REGISTERED = register_selection_benchmarks();

} // namespace doris::parquet_benchmark::selection_detail
