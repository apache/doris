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
#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <array>
#include <cctype>
#include <random>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "benchmark_relative_day.hpp"
#include "util/weekday_parser.h"

namespace doris {
namespace {

// Preserve the original copy and uppercase work to isolate the container's contribution.
template <typename Map>
int parse_weekday_with_map(std::string_view weekday) {
    static const Map names = {
            {"MO", 1}, {"MON", 1}, {"MONDAY", 1},    {"TU", 2}, {"TUE", 2}, {"TUESDAY", 2},
            {"WE", 3}, {"WED", 3}, {"WEDNESDAY", 3}, {"TH", 4}, {"THU", 4}, {"THURSDAY", 4},
            {"FR", 5}, {"FRI", 5}, {"FRIDAY", 5},    {"SA", 6}, {"SAT", 6}, {"SATURDAY", 6},
            {"SU", 7}, {"SUN", 7}, {"SUNDAY", 7}};
    auto uppercase = std::string(weekday);
    // Keep the original normalization algorithm in both container baselines.
    // NOLINTNEXTLINE(modernize-use-ranges)
    std::transform(uppercase.begin(), uppercase.end(), uppercase.begin(), ::toupper);
    const auto found = names.find(uppercase);
    return found == names.end() ? 0 : found->second;
}

template <int Method>
int parse_weekday_for_benchmark(std::string_view weekday) {
    if constexpr (Method == 0) {
        return parse_weekday_with_map<std::unordered_map<std::string, int>>(weekday);
    } else if constexpr (Method == 1) {
        return parse_weekday_with_map<phmap::flat_hash_map<std::string, int>>(weekday);
    } else {
        return parse_weekday(weekday);
    }
}

template <int Method, bool Misses>
void BM_weekday_lookup(benchmark::State& state) {
    constexpr std::array<std::string_view, 21> names = {
            "MO",     "mon",       "MoNdAy", "TU",       "tue",      "TuEsDaY", "WE",
            "wed",    "WeDnEsDaY", "TH",     "thu",      "ThUrSdAy", "FR",      "fri",
            "FrIdAy", "SA",        "sat",    "SaTuRdAy", "SU",       "sun",     "SuNdAy"};
    constexpr std::array<std::string_view, 12> misses = {
            "",           "M",    "MOND", "MONDDY", "SUNDAX", "SATURDAX",
            "WEDNESDAYX", " MON", "MO ",  "TUX",    "THX",    "WEX"};
    std::vector<std::string_view> inputs;
    inputs.reserve(4096);
    for (size_t i = 0; i < 4096; ++i) {
        if constexpr (Misses) {
            inputs.push_back(misses[i % misses.size()]);
            DORIS_CHECK_EQ(parse_weekday_for_benchmark<Method>(inputs.back()), 0);
        } else {
            inputs.push_back(names[i % names.size()]);
            DORIS_CHECK_EQ(parse_weekday_for_benchmark<Method>(inputs.back()),
                           (i % names.size()) / 3 + 1);
        }
    }
    std::mt19937 random(42);
    std::shuffle(inputs.begin(), inputs.end(), random);
    for (auto _ : state) {
        for (auto input : inputs) {
            benchmark::DoNotOptimize(input);
            int result = parse_weekday_for_benchmark<Method>(input);
            benchmark::DoNotOptimize(result);
        }
    }
    state.SetItemsProcessed(state.iterations() * inputs.size());
}

BENCHMARK_TEMPLATE(BM_weekday_lookup, 0, false)->Name("weekday/unordered/hits");
BENCHMARK_TEMPLATE(BM_weekday_lookup, 1, false)->Name("weekday/flat/hits");
BENCHMARK_TEMPLATE(BM_weekday_lookup, 2, false)->Name("weekday/perfect/hits");
BENCHMARK_TEMPLATE(BM_weekday_lookup, 0, true)->Name("weekday/unordered/misses");
BENCHMARK_TEMPLATE(BM_weekday_lookup, 1, true)->Name("weekday/flat/misses");
BENCHMARK_TEMPLATE(BM_weekday_lookup, 2, true)->Name("weekday/perfect/misses");

} // namespace
} // namespace doris
