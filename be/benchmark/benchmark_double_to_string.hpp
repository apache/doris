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
#include <fmt/compile.h>
#include <fmt/format.h>

#include <cstdint>
#include <limits>
#include <vector>

namespace doris {
namespace {

const std::vector<double>& double_to_string_values() {
    static const std::vector<double> values = [] {
        constexpr size_t num_values = 4096;
        std::vector<double> result;
        result.reserve(num_values);
        for (size_t i = 0; i < num_values; ++i) {
            // Match representative two-decimal DOUBLE values used by CSV OUTFILE workloads.
            uint64_t raw_value = (i * 104729 + 582) % 1000000;
            result.push_back(static_cast<double>(raw_value) / 100.0);
        }
        return result;
    }();
    return values;
}

template <bool shortest>
void BM_FmtDoubleToString(benchmark::State& state) {
    const auto& values = double_to_string_values();
    uint64_t total_bytes = 0;

    for (auto _ : state) {
        for (double value : values) {
            char buffer[64];
            char* end;
            if constexpr (shortest) {
                end = fmt::format_to(buffer, FMT_COMPILE("{}"), value);
            } else {
                end = fmt::format_to(buffer, FMT_COMPILE("{:.{}g}"), value,
                                     std::numeric_limits<double>::digits10 + 1);
            }
            total_bytes += end - buffer;
            benchmark::DoNotOptimize(buffer[0]);
        }
    }

    const auto num_values = static_cast<int64_t>(values.size());
    state.SetItemsProcessed(state.iterations() * num_values);
    state.counters["bytes_per_value"] =
            static_cast<double>(total_bytes) / state.iterations() / num_values;
}

BENCHMARK_TEMPLATE(BM_FmtDoubleToString, false)->Name("FmtDoubleToString_Significant16");
BENCHMARK_TEMPLATE(BM_FmtDoubleToString, true)->Name("FmtDoubleToString_Shortest");

} // namespace
} // namespace doris
