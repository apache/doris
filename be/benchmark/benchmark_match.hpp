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
#include <string>
#include <vector>

#include "vec/columns/column_string.h"
#include "vec/common/string_searcher.h"

namespace doris {

// Generate realistic English-like text data for benchmarking
static void generate_match_test_data(ColumnString& col, size_t num_rows, size_t avg_words) {
    static const std::vector<std::string> words = {
            "the",      "quick",  "brown",     "fox",       "jumps",   "over",
            "lazy",     "dog",    "apple",     "banana",    "cherry",  "database",
            "query",    "index",  "search",    "engine",    "table",   "column",
            "record",   "filter", "aggregate", "partition", "cluster", "replica",
            "optimize", "cache",  "memory",    "storage",   "network", "compute"};
    std::mt19937 rng(42);
    std::uniform_int_distribution<size_t> word_dist(0, words.size() - 1);
    std::uniform_int_distribution<size_t> len_dist(avg_words / 2, avg_words * 3 / 2);

    for (size_t i = 0; i < num_rows; ++i) {
        std::string text;
        size_t num_words = len_dist(rng);
        for (size_t j = 0; j < num_words; ++j) {
            if (j > 0) text += ' ';
            text += words[word_dist(rng)];
        }
        col.insert_data(text.data(), text.size());
    }
}

// Benchmark: TokenSearcher fast path (new) - match_any single token
static void BM_MatchAny_TokenSearcher(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t words_per_row = state.range(1);

    auto col = ColumnString::create();
    generate_match_test_data(*col, rows, words_per_row);

    ASCIICaseSensitiveTokenSearcher searcher("optimize", 8);

    for (auto _ : state) {
        size_t match_count = 0;
        for (size_t i = 0; i < rows; ++i) {
            const auto str_ref = col->get_data_at(i);
            const auto* haystack = reinterpret_cast<const uint8_t*>(str_ref.data);
            const auto* haystack_end = haystack + str_ref.size;
            if (searcher.search(haystack, haystack_end) < haystack_end) {
                match_count++;
            }
        }
        benchmark::DoNotOptimize(match_count);
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * rows);
}

// Benchmark: Original slow path simulation - tokenize then search
static void BM_MatchAny_Tokenize(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t words_per_row = state.range(1);

    auto col = ColumnString::create();
    generate_match_test_data(*col, rows, words_per_row);

    const std::string query_token = "optimize";

    for (auto _ : state) {
        size_t match_count = 0;
        for (size_t i = 0; i < rows; ++i) {
            const auto str_ref = col->get_data_at(i);
            // Simulate tokenization: split by non-alphanumeric and compare
            std::string_view sv(str_ref.data, str_ref.size);
            size_t start = 0;
            bool found = false;
            for (size_t j = 0; j <= sv.size() && !found; ++j) {
                bool is_sep =
                        (j == sv.size()) || (static_cast<uint8_t>(sv[j]) < 128 &&
                                             !std::isalnum(static_cast<unsigned char>(sv[j])));
                if (is_sep && j > start) {
                    if (sv.substr(start, j - start) == query_token) {
                        found = true;
                    }
                    start = j + 1;
                } else if (is_sep) {
                    start = j + 1;
                }
            }
            if (found) match_count++;
        }
        benchmark::DoNotOptimize(match_count);
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * rows);
}

// Benchmark: TokenSearcher with multiple tokens (match_all semantics)
static void BM_MatchAll_TokenSearcher(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t words_per_row = state.range(1);

    auto col = ColumnString::create();
    generate_match_test_data(*col, rows, words_per_row);

    std::vector<ASCIICaseSensitiveTokenSearcher> searchers;
    searchers.emplace_back("quick", 5);
    searchers.emplace_back("fox", 3);
    searchers.emplace_back("dog", 3);

    for (auto _ : state) {
        size_t match_count = 0;
        for (size_t i = 0; i < rows; ++i) {
            const auto str_ref = col->get_data_at(i);
            const auto* haystack = reinterpret_cast<const uint8_t*>(str_ref.data);
            const auto* haystack_end = haystack + str_ref.size;
            bool all = true;
            for (const auto& s : searchers) {
                if (s.search(haystack, haystack_end) >= haystack_end) {
                    all = false;
                    break;
                }
            }
            if (all) match_count++;
        }
        benchmark::DoNotOptimize(match_count);
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * rows);
}

// Benchmark: Case-insensitive TokenSearcher
static void BM_MatchAny_TokenSearcher_CaseInsensitive(benchmark::State& state) {
    size_t rows = state.range(0);
    size_t words_per_row = state.range(1);

    auto col = ColumnString::create();
    generate_match_test_data(*col, rows, words_per_row);

    ASCIICaseInsensitiveTokenSearcher searcher("OPTIMIZE", 8);

    for (auto _ : state) {
        size_t match_count = 0;
        for (size_t i = 0; i < rows; ++i) {
            const auto str_ref = col->get_data_at(i);
            const auto* haystack = reinterpret_cast<const uint8_t*>(str_ref.data);
            const auto* haystack_end = haystack + str_ref.size;
            if (searcher.search(haystack, haystack_end) < haystack_end) {
                match_count++;
            }
        }
        benchmark::DoNotOptimize(match_count);
    }
    state.SetItemsProcessed(int64_t(state.iterations()) * rows);
}

//                              rows  words/row
BENCHMARK(BM_MatchAny_TokenSearcher)
        ->Args({10000, 10})
        ->Args({10000, 50})
        ->Args({10000, 200})
        ->Args({100000, 10})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_MatchAny_Tokenize)
        ->Args({10000, 10})
        ->Args({10000, 50})
        ->Args({10000, 200})
        ->Args({100000, 10})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_MatchAll_TokenSearcher)
        ->Args({10000, 10})
        ->Args({10000, 50})
        ->Args({10000, 200})
        ->Unit(benchmark::kMicrosecond);

BENCHMARK(BM_MatchAny_TokenSearcher_CaseInsensitive)
        ->Args({10000, 10})
        ->Args({10000, 50})
        ->Args({10000, 200})
        ->Unit(benchmark::kMicrosecond);

} // namespace doris
