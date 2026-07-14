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

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "core/column/column_string.h"
#include "exec/common/multi_string_searcher.h"
#include "exec/common/string_searcher.h"

namespace doris {

namespace {

struct MultiSearchDataset {
    ColumnString::Chars haystack_data;
    ColumnString::Offsets haystack_offsets;
    std::vector<std::string> needle_storage;
    std::vector<std::string_view> needles;
};

enum class HitMode {
    NO_HIT,
    RARE_HIT,
    LAST_NEEDLE_HIT,
};

static std::string make_needle(size_t index, size_t needle_size = 8) {
    std::string needle(needle_size, 'a');
    for (size_t i = 0; i < needle_size; ++i) {
        needle[i] = static_cast<char>('a' + ((index * 7 + i * 11) % 26));
    }
    return needle;
}

static MultiSearchDataset build_multi_search_dataset(size_t rows, size_t row_len,
                                                     size_t needle_count, HitMode hit_mode) {
    MultiSearchDataset dataset;
    dataset.haystack_offsets.resize(rows);
    dataset.haystack_data.reserve(rows * row_len);
    dataset.needle_storage.reserve(needle_count);
    dataset.needles.reserve(needle_count);

    for (size_t i = 0; i < needle_count; ++i) {
        dataset.needle_storage.emplace_back(make_needle(i));
    }
    for (const auto& needle : dataset.needle_storage) {
        dataset.needles.emplace_back(needle);
    }

    std::mt19937 rng(20260602);
    std::uniform_int_distribution<int> char_dist(0, 25);
    std::uniform_int_distribution<size_t> needle_dist(0, needle_count - 1);

    size_t offset = 0;
    for (size_t row = 0; row < rows; ++row) {
        std::string value(row_len, 'a');
        for (size_t i = 0; i < row_len; ++i) {
            value[i] = static_cast<char>('A' + char_dist(rng));
        }

        if (hit_mode == HitMode::RARE_HIT && row % 16 == 0 && row_len >= 16) {
            const size_t needle_index = needle_dist(rng);
            const auto& needle = dataset.needle_storage[needle_index];
            const size_t pos = (row * 131) % (row_len - needle.size() + 1);
            std::memcpy(value.data() + pos, needle.data(), needle.size());
        } else if (hit_mode == HitMode::LAST_NEEDLE_HIT && row_len >= 16) {
            const auto& needle = dataset.needle_storage.back();
            const size_t pos = row_len - needle.size();
            std::memcpy(value.data() + pos, needle.data(), needle.size());
        }

        dataset.haystack_data.insert(value.data(), value.data() + value.size());
        offset += value.size();
        dataset.haystack_offsets[row] = static_cast<ColumnString::Offset>(offset);
    }

    return dataset;
}

static void multi_search_all_positions_old(const ColumnString::Chars& haystack_data,
                                           const ColumnString::Offsets& haystack_offsets,
                                           const std::vector<std::string_view>& needles,
                                           std::vector<Int32>& result) {
    std::vector<ASCIICaseSensitiveStringSearcher> searchers;
    searchers.reserve(needles.size());
    for (const auto needle : needles) {
        searchers.emplace_back(needle.data(), needle.size());
    }

    const size_t rows = haystack_offsets.size();
    const size_t needle_count = needles.size();
    result.assign(rows * needle_count, 0);

    for (size_t needle_index = 0; needle_index < searchers.size(); ++needle_index) {
        const auto& searcher = searchers[needle_index];
        size_t prev_haystack_offset = 0;

        for (size_t row = 0, result_index = needle_index; row < rows;
             ++row, result_index += needle_count) {
            const auto* haystack = haystack_data.data() + prev_haystack_offset;
            const auto* haystack_end = haystack_data.data() + haystack_offsets[row];
            const auto* match = searcher.search(haystack, haystack_end);
            result[result_index] =
                    match >= haystack_end ? 0 : static_cast<Int32>(match - haystack + 1);
            prev_haystack_offset = haystack_offsets[row];
        }
    }
}

static void multi_search_all_positions_new(const ColumnString::Chars& haystack_data,
                                           const ColumnString::Offsets& haystack_offsets,
                                           const std::vector<std::string_view>& needles,
                                           std::vector<Int32>& result) {
    const size_t rows = haystack_offsets.size();
    const size_t needle_count = needles.size();
    result.assign(rows * needle_count, 0);

    std::vector<StringRef> needle_refs;
    needle_refs.reserve(needles.size());
    for (const auto needle : needles) {
        needle_refs.emplace_back(needle.data(), needle.size());
    }

    MultiStringSearcher searcher(needle_refs);
    while (searcher.has_more_to_search()) {
        size_t prev_haystack_offset = 0;
        for (size_t row = 0; row < rows; ++row) {
            const auto* haystack = haystack_data.data() + prev_haystack_offset;
            const auto* haystack_end = haystack_data.data() + haystack_offsets[row];
            searcher.search_one_all(reinterpret_cast<const uint8_t*>(haystack),
                                    reinterpret_cast<const uint8_t*>(haystack_end),
                                    result.data() + row * needle_count);
            prev_haystack_offset = haystack_offsets[row];
        }
    }
}

static void verify_multi_search_results(const MultiSearchDataset& dataset) {
    std::vector<Int32> old_result;
    std::vector<Int32> new_result;
    multi_search_all_positions_old(dataset.haystack_data, dataset.haystack_offsets, dataset.needles,
                                   old_result);
    multi_search_all_positions_new(dataset.haystack_data, dataset.haystack_offsets, dataset.needles,
                                   new_result);
    CHECK_EQ(old_result.size(), new_result.size());
    for (size_t i = 0; i < old_result.size(); ++i) {
        CHECK_EQ(old_result[i], new_result[i]) << "mismatch at result index " << i;
    }
}

static void run_multi_search_old(benchmark::State& state, HitMode hit_mode) {
    const size_t rows = 4096;
    const size_t row_len = static_cast<size_t>(state.range(0));
    const size_t needle_count = static_cast<size_t>(state.range(1));
    const auto dataset = build_multi_search_dataset(rows, row_len, needle_count, hit_mode);
    verify_multi_search_results(dataset);

    std::vector<Int32> result;
    for (auto _ : state) {
        result.clear();
        multi_search_all_positions_old(dataset.haystack_data, dataset.haystack_offsets,
                                       dataset.needles, result);
        benchmark::DoNotOptimize(result);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * rows * needle_count);
}

static void run_multi_search_new(benchmark::State& state, HitMode hit_mode) {
    const size_t rows = 4096;
    const size_t row_len = static_cast<size_t>(state.range(0));
    const size_t needle_count = static_cast<size_t>(state.range(1));
    const auto dataset = build_multi_search_dataset(rows, row_len, needle_count, hit_mode);
    verify_multi_search_results(dataset);

    std::vector<Int32> result;
    for (auto _ : state) {
        result.clear();
        multi_search_all_positions_new(dataset.haystack_data, dataset.haystack_offsets,
                                       dataset.needles, result);
        benchmark::DoNotOptimize(result);
        benchmark::ClobberMemory();
    }
    state.SetItemsProcessed(state.iterations() * rows * needle_count);
}

static void BM_MultiSearchAllPositions_Old_NoHit(benchmark::State& state) {
    run_multi_search_old(state, HitMode::NO_HIT);
}

static void BM_MultiSearchAllPositions_New_NoHit(benchmark::State& state) {
    run_multi_search_new(state, HitMode::NO_HIT);
}

static void BM_MultiSearchAllPositions_Old_RareHit(benchmark::State& state) {
    run_multi_search_old(state, HitMode::RARE_HIT);
}

static void BM_MultiSearchAllPositions_New_RareHit(benchmark::State& state) {
    run_multi_search_new(state, HitMode::RARE_HIT);
}

static void BM_MultiSearchAllPositions_Old_LastNeedleHit(benchmark::State& state) {
    run_multi_search_old(state, HitMode::LAST_NEEDLE_HIT);
}

static void BM_MultiSearchAllPositions_New_LastNeedleHit(benchmark::State& state) {
    run_multi_search_new(state, HitMode::LAST_NEEDLE_HIT);
}

} // namespace

BENCHMARK(BM_MultiSearchAllPositions_Old_NoHit)
        ->Args({64, 4})
        ->Args({64, 16})
        ->Args({1024, 16})
        ->Args({32768, 16})
        ->Args({1024, 64});
BENCHMARK(BM_MultiSearchAllPositions_New_NoHit)
        ->Args({64, 4})
        ->Args({64, 16})
        ->Args({1024, 16})
        ->Args({32768, 16})
        ->Args({1024, 64});

BENCHMARK(BM_MultiSearchAllPositions_Old_RareHit)
        ->Args({64, 4})
        ->Args({64, 16})
        ->Args({1024, 16})
        ->Args({32768, 16})
        ->Args({1024, 64});
BENCHMARK(BM_MultiSearchAllPositions_New_RareHit)
        ->Args({64, 4})
        ->Args({64, 16})
        ->Args({1024, 16})
        ->Args({32768, 16})
        ->Args({1024, 64});

BENCHMARK(BM_MultiSearchAllPositions_Old_LastNeedleHit)
        ->Args({64, 4})
        ->Args({64, 16})
        ->Args({1024, 16})
        ->Args({32768, 16})
        ->Args({1024, 64});
BENCHMARK(BM_MultiSearchAllPositions_New_LastNeedleHit)
        ->Args({64, 4})
        ->Args({64, 16})
        ->Args({1024, 16})
        ->Args({32768, 16})
        ->Args({1024, 64});

} // namespace doris
