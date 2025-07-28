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

#include <string>

#include "util/bitmap_value.h"

using Roaring64Map = doris::detail::Roaring64Map;

namespace doris::vectorized {

const uint32_t num_bitmaps = 100;
const uint32_t num_outer_slots = 1000;
const uint32_t num_inner_values = 2000;
const uint32_t overlap_percent = 30; // 重叠比例

std::vector<Roaring64Map> makeMaps() {
    std::vector<Roaring64Map> result;
    const uint32_t shared_slots = num_outer_slots * overlap_percent / 100;

    Roaring64Map shared;
    for (uint32_t slot = 0; slot < shared_slots; ++slot) {
        auto value = (uint64_t(slot) << 32);
        for (uint32_t i = 0; i < num_inner_values; ++i) {
            shared.add(value + i);
        }
    }

    for (uint32_t bm_index = 0; bm_index < num_bitmaps; ++bm_index) {
        Roaring64Map roaring = shared;

        for (uint32_t slot = shared_slots; slot < num_outer_slots; ++slot) {
            auto value = (uint64_t(slot) << 32) + bm_index;
            for (uint32_t i = 0; i < num_inner_values; ++i) {
                roaring.add(value + i * num_bitmaps);
            }
        }
        result.push_back(std::move(roaring));
    }
    return result;
}

Roaring64Map legacy_fastunion(size_t n, const Roaring64Map** inputs) {
    Roaring64Map ans;
    for (size_t lcv = 0; lcv < n; ++lcv) {
        ans |= *(inputs[lcv]);
    }
    return ans;
}

static std::vector<Roaring64Map> test_maps = makeMaps();
static std::vector<const Roaring64Map*> test_map_ptrs = []() {
    std::vector<const Roaring64Map*> ptrs;
    ptrs.reserve(test_maps.size());
    for (const auto& map : test_maps) {
        ptrs.push_back(&map);
    }
    return ptrs;
}();

static void CustomCounters(benchmark::State& state) {
    state.counters["Bitmaps"] = num_bitmaps;
    state.counters["TotalValues"] = num_bitmaps * num_outer_slots * num_inner_values;
    state.counters["Overlap%"] = overlap_percent;
}
} // namespace doris::vectorized

static void BM_FastUnionOptimized(benchmark::State& state) {
    for (auto _ : state) {
        auto result = Roaring64Map::fastunion(doris::vectorized::test_map_ptrs.size(),
                                              doris::vectorized::test_map_ptrs.data());
        benchmark::DoNotOptimize(result);
    }
    doris::vectorized::CustomCounters(state);
    state.SetComplexityN(doris::vectorized::num_bitmaps);
}

static void BM_FastUnionLegacy(benchmark::State& state) {
    for (auto _ : state) {
        auto result = doris::vectorized::legacy_fastunion(doris::vectorized::test_map_ptrs.size(),
                                                          doris::vectorized::test_map_ptrs.data());
        benchmark::DoNotOptimize(result);
    }
    doris::vectorized::CustomCounters(state);
    state.SetComplexityN(doris::vectorized::num_bitmaps);
}

BENCHMARK(BM_FastUnionOptimized)
        ->Unit(benchmark::kMillisecond)
        ->Repetitions(5)
        ->DisplayAggregatesOnly()
        ->ComputeStatistics("min",
                            [](const std::vector<double>& v) -> double {
                                return *std::min_element(v.begin(), v.end());
                            })
        ->ComputeStatistics("max", [](const std::vector<double>& v) -> double {
            return *std::max_element(v.begin(), v.end());
        });

BENCHMARK(BM_FastUnionLegacy)
        ->Unit(benchmark::kMillisecond)
        ->Repetitions(5)
        ->DisplayAggregatesOnly()
        ->ComputeStatistics("min",
                            [](const std::vector<double>& v) -> double {
                                return *std::min_element(v.begin(), v.end());
                            })
        ->ComputeStatistics("max", [](const std::vector<double>& v) -> double {
            return *std::max_element(v.begin(), v.end());
        });
